"""Tests for filling in the sample sizes IBMA estimators need."""

import pytest

from compose_runner.metadata import apply_sample_sizes


def _studyset(analyses, study_metadata=None):
    return {
        "studies": [
            {
                "id": "study-1",
                "metadata": study_metadata or {},
                "analyses": analyses,
            }
        ]
    }


def _annotation(notes):
    return {"notes": notes}


def test_sample_size_taken_from_annotation():
    """The annotation is the most deliberately curated source."""
    studyset = _studyset([{"id": "a1", "metadata": {}}])
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": 24}}])

    result = apply_sample_sizes(studyset, annotation)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [24.0]


def test_sample_size_falls_back_to_study_metadata():
    """Older studies record sample size at the study level."""
    studyset = _studyset(
        [{"id": "a1", "metadata": {}}], study_metadata={"sample_size": 30}
    )

    result = apply_sample_sizes(studyset, _annotation([]))

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [30.0]


def test_annotation_wins_over_study_metadata():
    """Per-analysis information beats a study-level average."""
    studyset = _studyset(
        [{"id": "a1", "metadata": {}}], study_metadata={"sample_size": 30}
    )
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": 12}}])

    result = apply_sample_sizes(studyset, annotation)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [12.0]


def test_existing_sample_sizes_are_left_alone():
    """Anything already in NiMARE's expected form should not be rewritten."""
    studyset = _studyset([{"id": "a1", "metadata": {"sample_sizes": [7, 9]}}])
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": 100}}])

    result = apply_sample_sizes(studyset, annotation)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [7, 9]


def test_missing_sample_size_is_left_absent():
    """Better to leave it out than invent one; NiMARE will drop the analysis."""
    studyset = _studyset([{"id": "a1", "metadata": {}}])

    result = apply_sample_sizes(studyset, _annotation([]))

    assert "sample_sizes" not in result["studies"][0]["analyses"][0]["metadata"]


@pytest.mark.parametrize("value", ["", None, "not a number", 0, -5, True, False])
def test_unusable_sample_sizes_are_rejected(value):
    """Annotation columns are free-form, so the values need checking."""
    studyset = _studyset([{"id": "a1", "metadata": {}}])
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": value}}])

    result = apply_sample_sizes(studyset, annotation)

    assert "sample_sizes" not in result["studies"][0]["analyses"][0]["metadata"]


def test_numeric_strings_are_accepted():
    """Annotation values often arrive as strings."""
    studyset = _studyset([{"id": "a1", "metadata": {}}])
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": "18"}}])

    result = apply_sample_sizes(studyset, annotation)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [18.0]


def test_analysis_without_metadata_key_is_handled():
    """Analyses can arrive with no metadata at all."""
    studyset = _studyset([{"id": "a1"}])
    annotation = _annotation([{"analysis": "a1", "note": {"sample_size": 15}}])

    result = apply_sample_sizes(studyset, annotation)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [15.0]


def test_no_annotation_is_tolerated():
    """The annotation is optional."""
    studyset = _studyset([{"id": "a1", "metadata": {"sample_size": 21}}])

    result = apply_sample_sizes(studyset, None)

    assert result["studies"][0]["analyses"][0]["metadata"]["sample_sizes"] == [21.0]
