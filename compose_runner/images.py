"""Fetch the statistical maps an image-based meta-analysis needs.

NiMARE hands image paths to nibabel, which cannot read over HTTP, so
Neurostore's remote maps have to be pulled down and the studyset rewritten to
point at local files before a Dataset is built.
"""

import hashlib
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import nibabel as nib
import requests

LGR = logging.getLogger(__name__)

# Neurostore's map-type labels, lowercased, mapped onto NiMARE image types.
# The labels come from MAP_TYPE_CHOICES in neurostore/map_types.py.
MAP_TYPE_TO_IMAGE_TYPE = {
    "t map": "t",
    "t": "t",
    "z map": "z",
    "z": "z",
    "univariate-beta map": "beta",
    "u": "beta",
    "multivariate-beta map": "beta",
    "m": "beta",
    "variance map": "varcope",
    "variance": "varcope",
    "v": "varcope",
}

# Labels NiMARE could technically read but must not be given. A p map carries no
# sign, and NiMARE's only route from one is ``p_to_z``, which is documented to
# return an unsigned z -- so an analysis whose only usable map is a p map joins
# the meta-analysis as an all-positive z. Measured on a real staging studyset:
# every genuine z map was 37-59% negative and the one derived from a p map was
# 0%. Dropping the analysis costs a study; keeping it biases the result.
UNSIGNED_MAP_TYPES = frozenset(
    {"p map (given null hypothesis)", "p map", "p", '1-p map ("inverted" probability)'}
)

# When one analysis carries several maps of the same NiMARE type, which to
# prefer. Univariate beta is a cleaner contrast estimate than multivariate.
IMAGE_TYPE_PREFERENCE = {
    "beta": ["univariate-beta map", "u", "multivariate-beta map", "m"],
}

NIFTI_SUFFIXES = (".nii", ".nii.gz")


def normalize_value_type(value_type):
    """Return the NiMARE image type for a Neurostore map-type label.

    Returns None for labels NiMARE has no use for (ROI masks, parcellations,
    anatomicals and so on) and for the ones it would use wrongly; see
    :data:`UNSIGNED_MAP_TYPES`.
    """
    if not value_type:
        return None
    return MAP_TYPE_TO_IMAGE_TYPE.get(str(value_type).strip().lower())


def unusable_type_reason(value_type):
    """Why a map type is not passed to NiMARE."""
    if str(value_type or "").strip().lower() in UNSIGNED_MAP_TYPES:
        return "carries no sign, so NiMARE would derive an all-positive z"
    return "not a map type NiMARE can use"


def _looks_like_nifti(candidate):
    """Whether a location names a NIfTI rather than a landing page."""
    if not candidate:
        return False
    path = urlparse(str(candidate)).path.lower()
    return path.endswith(NIFTI_SUFFIXES)


def _is_fetchable(candidate):
    """Whether a location can actually be retrieved.

    Neurostore's ``filename`` is sometimes a bare basename
    ("spmT_0001_2.nii.gz"), which looks like a NIfTI but cannot be fetched.
    """
    if not candidate:
        return False
    text = str(candidate)
    if urlparse(text).scheme in ("http", "https", "file"):
        return True
    return Path(text).is_file()


def select_image_url(image):
    """Pick the location to download for an image entry.

    Which of ``url`` and ``filename`` holds the NIfTI depends on how the image
    reached Neurostore -- ingested from NeuroVault it is ``url``, with a bare
    basename in ``filename``; uploaded by compose it is ``filename``, with the
    NeuroVault landing page in ``url``. So choose on content, and return None
    rather than a landing page, which would download happily as HTML.
    """
    candidates = (image.get("filename"), image.get("url"))

    for candidate in candidates:
        if _looks_like_nifti(candidate) and _is_fetchable(candidate):
            return candidate

    return None


def _local_name(image, source_url):
    """Build a stable, collision-free filename for a downloaded image."""
    basename = os.path.basename(urlparse(str(source_url)).path) or "image.nii.gz"
    # Hash the URL so two studies' "z.nii.gz" cannot collide.
    digest = hashlib.md5(str(source_url).encode("utf-8")).hexdigest()[:10]
    image_id = image.get("id") or digest
    return f"{image_id}_{digest}_{basename}"


def _local_source(source_url):
    """Return a local path for a source already on this filesystem, or None."""
    text = str(source_url)
    parsed = urlparse(text)

    if parsed.scheme == "file":
        candidate = Path(parsed.path)
    elif parsed.scheme in ("http", "https"):
        return None
    else:
        candidate = Path(text)

    return candidate if candidate.is_file() else None


_GZIP_MAGIC = b"\x1f\x8b"
_NIFTI_SIZEOF_HDR = (b"\x5c\x01\x00\x00", b"\x00\x00\x01\x5c")  # 348, both endians


def _is_nifti_bytes(payload):
    """Whether downloaded bytes plausibly start a NIfTI (or a gzipped one).

    An error page arrives with HTTP 200, so ``raise_for_status`` does not catch
    it, and caching one under a .nii.gz name would break every later run too.
    """
    if len(payload) < 4:
        return False
    if payload.startswith(_GZIP_MAGIC):
        return True
    return payload[:4] in _NIFTI_SIZEOF_HDR


def _download(source_url, destination, session=None, timeout=60):
    """Fetch one image, skipping the work if it is already cached."""
    if destination.is_file() and destination.stat().st_size > 0:
        return destination

    # Written under a temporary name so an interrupted run cannot leave a
    # truncated file that a later run would treat as cached.
    partial = destination.with_suffix(destination.suffix + ".part")

    local = _local_source(source_url)
    if local is not None:
        # Copied in even though it is on disk: every map has to sit under the
        # directory that becomes the studyset's base path, or NiMARE resolves
        # the relative paths it derives against the wrong root.
        shutil.copyfile(local, partial)
    else:
        getter = session.get if session is not None else requests.get
        response = getter(source_url, timeout=timeout)
        response.raise_for_status()
        content = response.content
        if not _is_nifti_bytes(content):
            raise ValueError(
                f"{source_url} returned {len(content)} bytes that are not a NIfTI; "
                "the location is probably a landing page rather than a map."
            )
        partial.write_bytes(content)

    partial.rename(destination)
    return destination


def download_studyset_images(
    studyset_dict,
    image_dir,
    session=None,
    max_workers=8,
    timeout=60,
):
    """Download every usable image and rewrite the studyset to local paths.

    Images NiMARE cannot use, and images that fail to download, are dropped
    from the studyset rather than aborting the run; ``drop_invalid`` handles the
    resulting gaps.

    Parameters
    ----------
    studyset_dict : :obj:`dict`
        A NIMADS studyset, as returned by the Neurostore API.
    image_dir : :obj:`str` or :obj:`pathlib.Path`
        Directory to download into. Created if needed, and reused across runs
        as a cache.
    session : :obj:`requests.Session`, optional
        Session to issue requests through. Useful for tests and for connection
        reuse.
    max_workers : :obj:`int`, optional
        Maximum concurrent downloads.
    timeout : :obj:`int`, optional
        Per-request timeout in seconds.

    Returns
    -------
    studyset : :obj:`dict`
        The studyset, with each surviving image's ``filename`` and ``url``
        pointing at a local file.
    dropped : :obj:`dict`
        Analysis id to a tuple of descriptions of the maps that analysis lost.
    """
    image_dir = Path(image_dir)
    image_dir.mkdir(parents=True, exist_ok=True)

    # Analysis id -> the maps it lost, and why.
    dropped = {}

    def _record(analysis, image, why):
        label = image.get("value_type") or "untyped"
        dropped.setdefault(analysis.get("id"), []).append(f"{label} ({why})")

    # Collected first so the downloads can run concurrently.
    jobs = []
    for study in studyset_dict.get("studies") or []:
        for analysis in study.get("analyses") or []:
            for image in analysis.get("images") or []:
                image_type = normalize_value_type(image.get("value_type"))
                if image_type is None:
                    _record(
                        analysis, image, unusable_type_reason(image.get("value_type"))
                    )
                    continue
                source_url = select_image_url(image)
                if not source_url:
                    _record(analysis, image, "no fetchable location")
                    continue
                jobs.append((analysis, image, image_type, source_url))

    if not jobs:
        LGR.warning("No usable statistical maps found in the studyset.")
        return studyset_dict, {k: tuple(v) for k, v in dropped.items()}

    resolved = {}

    def _fetch(job):
        _, image, _, source_url = job
        destination = image_dir / _local_name(image, source_url)
        # Absolute: NiMARE resolves a relative image reference against the
        # studyset's base path, which is this same directory, so a relative path
        # would be joined onto itself.
        return _download(
            source_url, destination, session=session, timeout=timeout
        ).resolve()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for job, result in zip(jobs, pool.map(_safe(_fetch), jobs)):
            if result is not None:
                resolved[id(job[1])] = result

    attempted = {id(image) for _, image, _, _ in jobs}
    # A file that does not parse as a NIfTI would otherwise reach nibabel from
    # inside the fit, where nothing attributes it to the analysis it came from.
    unusable = {
        key: reason
        for key, reason in (
            (key, _unusable_reason(path)) for key, path in resolved.items()
        )
        if reason is not None
    }
    for study in studyset_dict.get("studies") or []:
        for analysis in study.get("analyses") or []:
            kept = []
            for image in analysis.get("images") or []:
                local_path = resolved.get(id(image))
                if local_path is None:
                    if id(image) in attempted:
                        _record(analysis, image, "could not be downloaded")
                    continue
                if id(image) in unusable:
                    _record(analysis, image, unusable[id(image)])
                    continue
                image = dict(image)
                image["filename"] = str(local_path)
                image["url"] = str(local_path)
                kept.append(image)
            superseded, analysis["images"] = _resolve_duplicate_types(kept)
            for image, why in superseded:
                _record(analysis, image, why)

    n_dropped = sum(len(v) for v in dropped.values())
    if n_dropped:
        LGR.warning(
            "Dropped %d image(s) across %d analysis/analyses that were unusable, "
            "duplicated, or could not be downloaded.",
            n_dropped,
            len(dropped),
        )

    return studyset_dict, {k: tuple(v) for k, v in dropped.items()}


#: Verdicts from :func:`_unusable_reason`, which reads a whole volume and is
#: asked the same question once per analysis the map is reached from.
_UNUSABLE_CACHE = {}


def _unusable_reason(path):
    """Why a downloaded map cannot be used, or None if it can.

    Only unreadable files. A map that parses but holds no finite non-zero voxel
    is NiMARE's to drop, and it does; a file that is not a NIfTI at all reaches
    nibabel mid-fit instead, which is neither caught nor attributed there.
    """
    key = str(path)
    if key in _UNUSABLE_CACHE:
        return _UNUSABLE_CACHE[key]

    try:
        nib.load(key)
    except Exception as exc:  # noqa: BLE001 - an unreadable map is just dropped
        reason = f"could not be read as a NIfTI ({exc.__class__.__name__})"
    else:
        reason = None

    _UNUSABLE_CACHE[key] = reason
    return reason


def _safe(func):
    """Wrap a fetch so one failure drops an image instead of the whole run."""

    def wrapper(job):
        _, image, _, source_url = job
        try:
            return func(job)
        except Exception as exc:  # noqa: BLE001 - any failure just drops the image
            LGR.warning(
                "Could not download image %s from %s: %s",
                image.get("id"),
                source_url,
                exc,
            )
            return None

    return wrapper


def _resolve_duplicate_types(images):
    """Keep one image per NiMARE type, choosing deterministically.

    The conversion to a Dataset keys images by type, so a duplicate would
    otherwise be resolved by ordering.

    Returns
    -------
    superseded : :obj:`list` of (:obj:`dict`, :obj:`str`)
        The images not kept, each with the reason, so a caller can report them.
    kept : :obj:`list` of :obj:`dict`
        One image per NiMARE image type.
    """
    by_type = {}
    for image in images:
        image_type = normalize_value_type(image.get("value_type"))
        if image_type is None:
            continue
        by_type.setdefault(image_type, []).append(image)

    kept, superseded = [], []
    for image_type, candidates in by_type.items():
        if len(candidates) == 1:
            kept.append(candidates[0])
            continue

        preference = IMAGE_TYPE_PREFERENCE.get(image_type, [])

        def _rank(image, preference=preference):
            label = str(image.get("value_type") or "").strip().lower()
            try:
                return preference.index(label)
            except ValueError:
                return len(preference)

        # Ties break on id so the choice is stable across runs.
        ranked = sorted(candidates, key=lambda i: (_rank(i), str(i.get("id") or "")))
        chosen = ranked[0]
        LGR.info(
            "Analysis has %d '%s' maps; using %s.",
            len(candidates),
            image_type,
            chosen.get("value_type"),
        )
        kept.append(chosen)
        for other in ranked[1:]:
            superseded.append((other, f"superseded by another '{image_type}' map"))

    return superseded, kept
