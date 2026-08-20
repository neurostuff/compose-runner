"""Fetch and normalize the statistical maps an IBMA needs.

NiMARE resolves an image to a path and hands it to nibabel, which cannot read
over HTTP. Neurostore stores remote locations, so the maps have to be pulled
down and the studyset rewritten to point at local files before a Dataset is
built.

Two details make this less mechanical than it sounds:

* Neurostore stores an image's location across ``url`` and ``filename``, and
  which of the two holds the downloadable NIfTI depends on how the image got
  there. Images ingested from NeuroVault put the NIfTI in ``url`` and a bare
  basename in ``filename``; images uploaded by compose put the NeuroVault
  *landing page* in ``url`` and the NIfTI in ``filename``. NiMARE just takes
  ``url or filename``, which fetches HTML for the second shape, so the NIfTI
  has to be picked out explicitly.
* ``value_type`` arrives as a human-readable label. NiMARE only recognizes a
  handful of them, and silently ignores the rest -- including
  "multivariate-beta map", which Neurostore does count as a beta map.
"""

import hashlib
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import requests

LGR = logging.getLogger(__name__)

# Map Neurostore's map-type labels onto the image types NiMARE understands.
# Keys are lowercased labels; see MAP_TYPE_CHOICES in
# store/backend/neurostore/map_types.py for the source of the labels.
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
    "p map (given null hypothesis)": "p",
    "p map": "p",
    "p": "p",
}

# When one analysis carries several maps of the same NiMARE type, prefer the
# more specific one rather than letting dict ordering decide. Univariate beta
# is a cleaner contrast estimate than multivariate beta.
IMAGE_TYPE_PREFERENCE = {
    "beta": ["univariate-beta map", "u", "multivariate-beta map", "m"],
}

NIFTI_SUFFIXES = (".nii", ".nii.gz")


def normalize_value_type(value_type):
    """Return the NiMARE image type for a Neurostore map-type label.

    Returns None for labels NiMARE has no use for (ROI masks, parcellations,
    anatomicals and so on).
    """
    if not value_type:
        return None
    return MAP_TYPE_TO_IMAGE_TYPE.get(str(value_type).strip().lower())


def _looks_like_nifti(candidate):
    """Whether a location names a NIfTI rather than a landing page."""
    if not candidate:
        return False
    path = urlparse(str(candidate)).path.lower()
    return path.endswith(NIFTI_SUFFIXES)


def _is_fetchable(candidate):
    """Whether a location can actually be retrieved.

    Neurostore's ``filename`` is sometimes a bare basename
    ("spmT_0001_2.nii.gz") rather than a location, which looks like a NIfTI but
    cannot be fetched. Treat a candidate as usable only if it is an absolute
    URL or a file that exists on this machine.
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
    reached Neurostore, so neither field can be trusted by position:

    * ingested from NeuroVault -- ``url`` is the NIfTI, ``filename`` is a bare
      basename;
    * uploaded by compose -- ``url`` is the NeuroVault landing page,
      ``filename`` is the NIfTI.

    Both shapes exist in production, so choose on content: the candidate that
    is both fetchable and named like a NIfTI. Returning None is deliberate when
    neither qualifies -- a landing page downloads successfully as HTML, and a
    NIfTI-named file full of HTML is a much worse failure than a missing map.
    """
    candidates = (image.get("filename"), image.get("url"))

    for candidate in candidates:
        if _looks_like_nifti(candidate) and _is_fetchable(candidate):
            return candidate

    return None


def _local_name(image, source_url):
    """Build a stable, collision-free filename for a downloaded image."""
    basename = os.path.basename(urlparse(str(source_url)).path) or "image.nii.gz"
    # Include a hash of the URL so two studies' "z.nii.gz" cannot collide.
    digest = hashlib.md5(str(source_url).encode("utf-8")).hexdigest()[:10]
    image_id = image.get("id") or digest
    return f"{image_id}_{digest}_{basename}"


def _local_source(source_url):
    """Return a local path for a source that is already on this filesystem.

    Studysets normally carry remote URLs, but a file path or a file:// URL is
    valid too, and going through requests for those would fail.
    """
    text = str(source_url)
    parsed = urlparse(text)

    if parsed.scheme == "file":
        candidate = Path(parsed.path)
    elif parsed.scheme in ("http", "https"):
        return None
    else:
        candidate = Path(text)

    return candidate if candidate.is_file() else None


# A gzip member, or an uncompressed NIfTI-1/NIfTI-2 header. Enough to tell a
# real map from an HTML error page, which is the failure that matters here.
_GZIP_MAGIC = b"\x1f\x8b"
_NIFTI_SIZEOF_HDR = (b"\x5c\x01\x00\x00", b"\x00\x00\x01\x5c")  # 348, both endians


def _is_nifti_bytes(payload):
    """Whether downloaded bytes plausibly start a NIfTI (or a gzipped one).

    A landing page or an error page returns HTTP 200 with HTML, so
    ``raise_for_status`` does not catch it. Rejecting it here keeps a file that
    nibabel cannot open from being written into the cache, where it would fail
    every subsequent run too.
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

    # Write to a temporary name first so an interrupted run cannot leave a
    # truncated file behind that a later run would treat as cached.
    partial = destination.with_suffix(destination.suffix + ".part")

    local = _local_source(source_url)
    if local is not None:
        # Already on disk, but still copy it in: every map has to sit under the
        # one directory that becomes the studyset's base path, or NiMARE will
        # resolve the relative paths it derives against the wrong root.
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
    from the returned studyset rather than aborting the run: NiMARE's
    ``drop_invalid`` handles the resulting gaps, and losing one map should not
    cost the whole meta-analysis.

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
        Maximum concurrent downloads. NeuroVault is the bottleneck, so this is
        deliberately modest.
    timeout : :obj:`int`, optional
        Per-request timeout in seconds.

    Returns
    -------
    studyset : :obj:`dict`
        The studyset, with each surviving image's ``filename`` and ``url``
        pointing at a local file.
    dropped : :obj:`dict`
        Analysis id to a tuple of descriptions of the maps that analysis lost,
        so a caller can say which maps are not in the meta-analysis and why
        rather than only how many.
    """
    image_dir = Path(image_dir)
    image_dir.mkdir(parents=True, exist_ok=True)

    # Analysis id -> the maps it lost, and why. Collected as the studyset is
    # walked rather than counted at the end, because "12 images were dropped"
    # is not something a user can act on.
    dropped = {}

    def _record(analysis, image, why):
        label = image.get("value_type") or "untyped"
        dropped.setdefault(analysis.get("id"), []).append(f"{label} ({why})")

    # Collect the work first so it can be done concurrently.
    jobs = []
    for study in studyset_dict.get("studies") or []:
        for analysis in study.get("analyses") or []:
            for image in analysis.get("images") or []:
                image_type = normalize_value_type(image.get("value_type"))
                if image_type is None:
                    _record(analysis, image, "not a map type NiMARE can use")
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
        return _download(source_url, destination, session=session, timeout=timeout)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for job, result in zip(jobs, pool.map(_safe(_fetch), jobs)):
            if result is not None:
                resolved[id(job[1])] = result

    attempted = {id(image) for _, image, _, _ in jobs}
    for study in studyset_dict.get("studies") or []:
        for analysis in study.get("analyses") or []:
            kept = []
            for image in analysis.get("images") or []:
                local_path = resolved.get(id(image))
                if local_path is None:
                    if id(image) in attempted:
                        _record(analysis, image, "could not be downloaded")
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

    ``convert_nimads_to_dataset`` assigns images into a dict keyed by type, so
    a duplicate would silently overwrite its predecessor and leave the result
    dependent on ordering. Pick explicitly instead.

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
