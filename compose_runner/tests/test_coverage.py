"""Tests for reporting what an image-based meta-analysis used.

These run a real workflow over real NIfTIs, because the module under test reads
its answers off a fitted estimator rather than working them out. Nothing here
asserts a transform rule; NiMARE decides those and the report relays them.
"""

import zlib

import nibabel as nib
import numpy as np
import pytest
from nimare.meta.ibma import DerSimonianLaird, FixedEffectsHedges, Stouffers
from nimare.nimads import Studyset
from nimare.workflows import IBMAWorkflow

from compose_runner import coverage

SHAPE = (6, 7, 6)
AFFINE = np.eye(4) * 2
AFFINE[3, 3] = 1


@pytest.fixture
def write_map(tmp_path):
    """Write a small, well-behaved NIfTI and return its path."""

    def _write(name, offset=1.0):
        # crc32, not hash(): str hashing is salted per process.
        rng = np.random.RandomState(zlib.crc32(name.encode()) % (2**31))
        data = rng.normal(offset, 1.0, size=SHAPE).astype(np.float32)
        path = tmp_path / f"{name}.nii.gz"
        nib.save(nib.Nifti1Image(data, AFFINE), str(path))
        return str(path)

    return _write


def _analysis(write_map, analysis_id, value_types, sample_size=None):
    return {
        "id": analysis_id,
        "name": analysis_id,
        "conditions": [],
        "weights": [],
        "points": [],
        "metadata": {"sample_sizes": [sample_size]} if sample_size else {},
        "images": [
            {
                "id": f"{analysis_id}-{i}",
                "value_type": value_type,
                "filename": write_map(f"{analysis_id}-{i}"),
                "url": write_map(f"{analysis_id}-{i}"),
                "space": "MNI",
            }
            for i, value_type in enumerate(value_types)
        ],
    }


def _studyset(write_map, spec):
    """Build a studyset from ``{study_id: (value_types, sample_size)}``."""
    studies = [
        {
            "id": study_id,
            "name": study_id,
            "metadata": {},
            "analyses": [
                _analysis(write_map, f"{study_id}-a", value_types, sample_size)
            ],
        }
        for study_id, (value_types, sample_size) in spec.items()
    ]
    return Studyset({"id": "ss", "name": "ss", "studies": studies}, target="mni152_2mm")


def _fit_and_describe(studyset, estimator, dropped_maps=None):
    """Fit the workflow the way the runner does, then read the outcome off it."""
    workflow = IBMAWorkflow(estimator=estimator, diagnostics="jackknife")
    results = workflow.fit(studyset)
    return results, coverage.describe_result(
        results, studyset, dropped_maps=dropped_maps
    )


def test_requirements_read_off_the_estimator():
    assert coverage.estimator_requirements(Stouffers) == (("z",), ())
    assert coverage.estimator_requirements(FixedEffectsHedges) == (
        ("t",),
        ("sample_sizes",),
    )
    assert coverage.estimator_requirements(DerSimonianLaird) == (
        ("beta", "varcope"),
        (),
    )


def test_a_derived_map_is_reported_as_derived(write_map):
    """A t-map plus a sample size becomes a z-map, and the report says so."""
    studyset = _studyset(write_map, {"s1": (["T map"], 20)})

    _, report = _fit_and_describe(studyset, Stouffers(aggressive_mask=True))

    (analysis,) = report.analyses
    assert analysis.included
    assert analysis.derived == {"z"}
    assert "converted z" in analysis.reason


def test_an_analysis_nimare_cannot_transform_is_excluded_with_a_reason(write_map):
    """A t-map with no sample size cannot become a z-map.

    Paired with a usable study, so the run succeeds and reports the exclusion
    rather than failing.
    """
    studyset = _studyset(write_map, {"ok": (["Z map"], None), "s1": (["T map"], None)})

    _, report = _fit_and_describe(studyset, Stouffers(aggressive_mask=True))

    analysis = {a.study_id: a for a in report.analyses}["s1"]
    assert not analysis.included
    assert analysis.missing_images == {"z"}
    assert "could not produce z" in analysis.reason
    assert "from t" in analysis.reason
    assert len(report.included) == 1


def test_nothing_usable_is_described_from_the_submission(write_map):
    """When nothing survives there is no fitted estimator to read."""
    studyset = _studyset(write_map, {"s1": (["T map"], None)})

    with pytest.raises(ValueError, match="has no data for 'z_maps'"):
        _fit_and_describe(studyset, Stouffers(aggressive_mask=True))

    report = coverage.describe_submission(studyset, Stouffers())

    assert report.included == []
    (analysis,) = report.analyses
    assert analysis.supplied == {"t"}
    assert "could not produce z" in analysis.reason


def test_an_analysis_with_the_map_but_no_metadata_is_explained(write_map):
    """FixedEffectsHedges needs a sample size as well as a t-map.

    The map is there, so the report has to reach for the metadata requirement to
    explain the exclusion.
    """
    studyset = _studyset(write_map, {"ok": (["T map"], 20), "s1": (["T map"], None)})

    _, report = _fit_and_describe(studyset, FixedEffectsHedges(aggressive_mask=True))

    analysis = {a.study_id: a for a in report.analyses}["s1"]
    assert not analysis.included
    assert analysis.missing_images == frozenset()
    assert analysis.missing_metadata == {"sample_sizes"}
    assert "check sample_sizes" in analysis.reason


def test_partial_coverage_still_yields_a_usable_studyset(write_map):
    """The mixed case: some usable, some derivable, some hopeless."""
    studyset = _studyset(
        write_map,
        {
            "supplied": (["Z map"], None),
            "derivable": (["T map"], 20),
            "hopeless": (["T map"], None),
            "unusable": (["univariate-beta map"], None),
        },
    )

    _, report = _fit_and_describe(studyset, Stouffers(aggressive_mask=True))

    assert len(report.included) == 2
    assert len(report.excluded) == 2
    assert report.n_studies_included == 2
    assert {a.study_id for a in report.included} == {"supplied", "derivable"}


def test_summary_names_the_excluded_analyses(write_map):
    studyset = _studyset(
        write_map, {"good": (["Z map"], None), "bad": (["univariate-beta map"], None)}
    )

    _, report = _fit_and_describe(studyset, Stouffers(aggressive_mask=True))
    summary = report.summary()

    assert "Used 1 of 2 analyses" in summary
    assert "Left out 1 analysis/analyses" in summary
    assert "bad" in summary


def test_dropped_maps_are_carried_into_the_report(write_map):
    """What never reached the transform still belongs in the account."""
    studyset = _studyset(write_map, {"s1": (["univariate-beta map"], None)})

    report = coverage.describe_submission(
        studyset,
        Stouffers(),
        dropped_maps={"s1-a": ("ROI/mask (not a map type NiMARE can use)",)},
    )

    (analysis,) = report.analyses
    assert "ROI/mask" in analysis.reason


def test_varcope_is_derived_from_beta_and_t(write_map):
    """DerSimonianLaird needs beta and varcope; t + beta yields the varcope."""
    studyset = _studyset(write_map, {"s1": (["univariate-beta map", "T map"], None)})

    _, report = _fit_and_describe(studyset, DerSimonianLaird(aggressive_mask=True))

    (analysis,) = report.analyses
    assert analysis.included
    assert analysis.derived == {"varcope"}


def test_supplied_maps_are_reported_per_analysis(write_map):
    """An analysis must not be credited with maps a different study uploaded.

    NiMARE's image table has a column per type across the whole studyset, and an
    absent path reads as NaN, which is truthy, so taking truthiness would report
    every analysis as holding every type.
    """
    studyset = _studyset(
        write_map,
        {
            "has_t": (["T map"], None),
            "has_beta": (["univariate-beta map"], None),
            "has_z": (["Z map"], None),
        },
    )

    _, report = _fit_and_describe(studyset, Stouffers(aggressive_mask=True))
    supplied = {a.analysis_id: a.supplied for a in report.analyses}

    assert supplied["has_t-a"] == {"t"}
    assert supplied["has_beta-a"] == {"beta"}
    assert supplied["has_z-a"] == {"z"}

    included = {a.analysis_id: a for a in report.included}
    assert included["has_z-a"].derived == frozenset()
    assert included["has_z-a"].reason == "supplied the required maps"

    excluded = {a.analysis_id: a.reason for a in report.excluded}
    assert "from t" in excluded["has_t-a"]
    assert "z" not in excluded["has_t-a"].split("from ")[1]
