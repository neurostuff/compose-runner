"""Tests for staging a studyset's images locally."""

import gzip
import io

import nibabel as nib
import numpy as np
import pytest

from compose_runner.images import (
    _resolve_duplicate_types,
    download_studyset_images,
    normalize_value_type,
    select_image_url,
    unusable_type_reason,
)


def _gzipped_nifti():
    """A real, tiny NIfTI: staging opens what it downloaded and drops what it cannot.

    A map with no finite non-zero voxel is dropped, so the canned one carries a
    value.
    """
    data = np.zeros((2, 2, 2), dtype=np.float32)
    data[0, 0, 0] = 1.0
    image = nib.Nifti1Image(data, np.eye(4))
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
        handle.write(image.to_bytes())
    return buffer.getvalue()


# Canned responses have to be a NIfTI, or they are rejected.
GZIPPED_NIFTI = _gzipped_nifti()


class FakeResponse:
    """Minimal stand-in for a requests response."""

    def __init__(self, content=GZIPPED_NIFTI, status=200):
        self.content = content
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")


class FakeSession:
    """Records requests and serves canned responses."""

    def __init__(self, failures=()):
        self.failures = set(failures)
        self.requested = []

    def get(self, url, timeout=None):  # noqa: ARG002
        self.requested.append(url)
        if url in self.failures:
            return FakeResponse(status=404)
        return FakeResponse()


def _image(image_id, value_type, filename=None, url=None):
    return {
        "id": image_id,
        "value_type": value_type,
        "filename": filename,
        "url": url,
    }


def _studyset(images):
    return {
        "studies": [
            {
                "id": "study-1",
                "analyses": [{"id": "analysis-1", "images": images}],
            }
        ]
    }


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("Z map", "z"),
        ("T map", "t"),
        ("univariate-beta map", "beta"),
        ("multivariate-beta map", "beta"),
        ("variance map", "varcope"),
        ("  z MAP  ", "z"),
    ],
)
def test_normalize_value_type_known_labels(label, expected):
    """Neurostore's human-readable labels must reach NiMARE's image types."""
    assert normalize_value_type(label) == expected


@pytest.mark.parametrize(
    "label",
    ["ROI/mask", "parcellation", "anatomical", "other", "", None, "nonsense"],
)
def test_normalize_value_type_unusable_labels(label):
    """Maps NiMARE cannot use must not be guessed at."""
    assert normalize_value_type(label) is None


@pytest.mark.parametrize(
    "label",
    ["P map (given null hypothesis)", "P map", '1-P map ("inverted" probability)'],
)
def test_unsigned_maps_are_not_passed_to_nimare(label):
    """A p map has no sign, and NiMARE's only route from one drops it.

    ``p_to_z`` returns an unsigned z, so an analysis whose only usable map is a
    p map would contribute an all-positive map to the meta-analysis. The reason
    has to say that rather than claim NiMARE cannot read the label.
    """
    assert normalize_value_type(label) is None
    assert "no sign" in unusable_type_reason(label)


def test_select_image_url_compose_uploaded_shape():
    """Images uploaded by compose put the landing page in url."""
    image = _image(
        "img-1",
        "Z map",
        filename="http://neurovault.org/media/images/23490/z.nii.gz",
        url="http://neurovault.org/images/1027169/",
    )

    assert (
        select_image_url(image) == "http://neurovault.org/media/images/23490/z.nii.gz"
    )


def test_select_image_url_neurovault_ingested_shape():
    """Images ingested from NeuroVault put the NIfTI in url.

    Their filename is a bare basename, which looks like a NIfTI but cannot be
    fetched.
    """
    image = _image(
        "img-1",
        "T map",
        filename="spmT_0001_2.nii.gz",
        url="https://neurovault.org/media/images/4248/spmT_0001_2.nii.gz",
    )

    assert (
        select_image_url(image)
        == "https://neurovault.org/media/images/4248/spmT_0001_2.nii.gz"
    )


def test_select_image_url_accepts_a_local_file(tmp_path):
    """A studyset may legitimately point at a path on this machine."""
    local = tmp_path / "z.nii.gz"
    local.write_bytes(b"")
    image = _image("img-1", "Z map", filename=str(local))

    assert select_image_url(image) == str(local)


@pytest.mark.parametrize(
    ("filename", "url"),
    [
        ("spmT_0001.nii.gz", None),  # a basename with no location alongside it
        ("some/path", "http://host/page/"),  # a landing page, which serves HTML
        (None, None),
    ],
)
def test_select_image_url_rejects_what_it_cannot_fetch(filename, url):
    assert select_image_url(_image("img-1", "Z map", filename, url)) is None


def test_download_rewrites_images_to_local_paths(tmp_path):
    """The studyset must point at files nibabel can actually open."""
    studyset = _studyset(
        [
            _image(
                "img-1",
                "Z map",
                filename="http://neurovault.org/media/images/1/z.nii.gz",
                url="http://neurovault.org/images/1/",
            )
        ]
    )
    session = FakeSession()

    result, _ = download_studyset_images(studyset, tmp_path, session=session)

    image = result["studies"][0]["analyses"][0]["images"][0]
    assert image["filename"].startswith(str(tmp_path))
    assert image["url"] == image["filename"]
    # The NIfTI was fetched, not the landing page.
    assert session.requested == ["http://neurovault.org/media/images/1/z.nii.gz"]


def test_download_caches_across_calls(tmp_path):
    """A second run should not refetch what is already on disk."""
    studyset = _studyset(
        [_image("img-1", "Z map", filename="http://host/media/z.nii.gz")]
    )
    session = FakeSession()

    download_studyset_images(studyset, tmp_path, session=session)
    first_count = len(session.requested)
    download_studyset_images(
        _studyset([_image("img-1", "Z map", filename="http://host/media/z.nii.gz")]),
        tmp_path,
        session=session,
    )

    assert first_count == 1
    assert len(session.requested) == 1


def test_download_drops_failures_without_raising(tmp_path):
    """One dead link should cost one map, not the whole meta-analysis."""
    studyset = _studyset(
        [
            _image("img-1", "Z map", filename="http://host/media/good.nii.gz"),
            _image("img-2", "T map", filename="http://host/media/missing.nii.gz"),
        ]
    )
    session = FakeSession(failures={"http://host/media/missing.nii.gz"})

    result, _ = download_studyset_images(studyset, tmp_path, session=session)

    images = result["studies"][0]["analyses"][0]["images"]
    assert len(images) == 1
    assert images[0]["id"] == "img-1"


def test_download_gives_distinct_names_to_same_basename(tmp_path):
    """Two studies' "z.nii.gz" must not overwrite each other."""
    studyset = {
        "studies": [
            {
                "id": "study-1",
                "analyses": [
                    {
                        "id": "a1",
                        "images": [
                            _image("i1", "Z map", filename="http://host/a/z.nii.gz")
                        ],
                    }
                ],
            },
            {
                "id": "study-2",
                "analyses": [
                    {
                        "id": "a2",
                        "images": [
                            _image("i2", "Z map", filename="http://host/b/z.nii.gz")
                        ],
                    }
                ],
            },
        ]
    }

    result, _ = download_studyset_images(studyset, tmp_path, session=FakeSession())

    paths = {
        study["analyses"][0]["images"][0]["filename"] for study in result["studies"]
    }
    assert len(paths) == 2


def test_download_handles_studyset_without_images(tmp_path):
    """A coordinate-only studyset should pass through untouched."""
    studyset = _studyset([])

    result, _ = download_studyset_images(studyset, tmp_path, session=FakeSession())

    assert result["studies"][0]["analyses"][0]["images"] == []


@pytest.mark.parametrize("reverse", [False, True], ids=["m-first", "u-first"])
def test_duplicate_beta_types_prefer_univariate(reverse):
    """Both labels map to 'beta', and only one survives the Dataset build.

    Left to ordering the winner would be whichever came last, and the loser has
    to be accounted for rather than vanishing.
    """
    images = [
        _image("img-m", "multivariate-beta map", filename="/tmp/m.nii.gz"),
        _image("img-u", "univariate-beta map", filename="/tmp/u.nii.gz"),
    ]
    if reverse:
        images.reverse()

    superseded, kept = _resolve_duplicate_types(images)

    assert [image["id"] for image in kept] == ["img-u"]
    assert [(i["id"], why) for i, why in superseded] == [
        ("img-m", "superseded by another 'beta' map")
    ]


def test_distinct_types_are_all_kept():
    """Different types do not collide, so nothing should be dropped."""
    images = [
        _image("img-z", "Z map", filename="/tmp/z.nii.gz"),
        _image("img-b", "univariate-beta map", filename="/tmp/b.nii.gz"),
        _image("img-v", "variance map", filename="/tmp/v.nii.gz"),
    ]

    _, kept = _resolve_duplicate_types(images)

    assert {image["id"] for image in kept} == {"img-z", "img-b", "img-v"}


def test_download_rejects_a_payload_that_is_not_a_nifti(tmp_path):
    """An HTML error page arrives with HTTP 200, so status is not enough."""

    class HtmlSession:
        def __init__(self):
            self.requested = []

        def get(self, url, timeout=None):  # noqa: ARG002
            self.requested.append(url)
            return FakeResponse(content=b"<!DOCTYPE html><html>not found</html>")

    studyset = _studyset(
        [_image("img-1", "Z map", filename="http://host/media/z.nii.gz")]
    )
    session = HtmlSession()

    result, _ = download_studyset_images(studyset, tmp_path, session=session)

    assert result["studies"][0]["analyses"][0]["images"] == []
    assert not list(tmp_path.glob("*.nii.gz"))


def test_download_reports_what_it_dropped_and_why(tmp_path):
    """A count is not actionable; the caller needs to know which maps and why."""
    studyset = _studyset(
        [
            _image("img-1", "Z map", filename="http://host/media/z.nii.gz"),
            _image("img-2", "ROI/mask", filename="http://host/media/roi.nii.gz"),
            _image("img-3", "T map", filename="spmT_0001.nii.gz"),
            _image("img-4", "Z map", filename="http://host/media/dead.nii.gz"),
        ]
    )
    session = FakeSession(failures=["http://host/media/dead.nii.gz"])

    result, dropped = download_studyset_images(studyset, tmp_path, session=session)

    reasons = dropped["analysis-1"]
    assert any("ROI/mask" in r and "NiMARE can use" in r for r in reasons)
    assert any("T map" in r and "fetchable location" in r for r in reasons)
    # img-4 lost the coin toss against img-1 or failed outright; either way it
    # is reported rather than vanishing.
    assert any("Z map" in r for r in reasons)
    # One z map survives, and nothing else does -- and nothing unusable or
    # unfetchable was requested.
    kept = result["studies"][0]["analyses"][0]["images"]
    assert [i["id"] for i in kept] == ["img-1"]
    assert set(session.requested) == {
        "http://host/media/z.nii.gz",
        "http://host/media/dead.nii.gz",
    }
