"""Tests for selecting analyses from the notes a studyset carries.

A studyset owns its annotations, so ``apply_filter`` reads the filter column off
``annotations_df`` rather than out of a separate annotation object. The column is
typed by its values -- a boolean note key arrives as float 1.0/0.0 with NaN where
no note recorded one -- and NaN is truthy, so a plain truth test would put every
un-noted analysis in the selection.
"""

import pytest
from nimare.nimads import Studyset

from compose_runner.run import Runner

_TARGET = "mni152_2mm"


def _analysis(analysis_id, x):
    return {
        "id": analysis_id,
        "name": analysis_id,
        "conditions": [],
        "weights": [],
        "points": [
            {"id": f"p-{analysis_id}", "coordinates": [x, x, x], "space": "MNI", "values": []}
        ],
        "metadata": {},
        "images": [],
    }


# Two analyses in one study and one in another, so a selection has to name the
# study as well: NIMADS does not require analysis ids to be unique across them.
_STUDYSET = {
    "id": "ss",
    "name": "ss",
    "studies": [
        {
            "id": "s1",
            "name": "s1",
            "metadata": {},
            "analyses": [_analysis("a1", 1), _analysis("a2", 2)],
        },
        {"id": "s2", "name": "s2", "metadata": {}, "analyses": [_analysis("a3", 3)]},
    ],
}


def _runner(specification, note_keys, notes):
    """A Runner carrying only what ``apply_filter`` reads, and its studyset."""
    runner = Runner.__new__(Runner)
    runner.cached_specification = specification
    runner.cached_annotation = {
        "id": "ann",
        "name": "ann",
        "studyset": "ss",
        "note_keys": note_keys,
        "notes": notes,
    }
    studyset = Studyset(
        _STUDYSET, target=_TARGET, annotations=[runner.cached_annotation]
    )
    return runner, studyset


def _note(analysis_id, value, key="include"):
    study_id = "s2" if analysis_id == "a3" else "s1"
    return {"study": study_id, "analysis": analysis_id, "note": {key: value}}


def test_boolean_column_selects_the_analyses_marked_true():
    runner, studyset = _runner(
        {"filter": "include", "conditions": [True], "weights": [1]},
        {"include": "boolean"},
        [_note("a1", True), _note("a2", False), _note("a3", True)],
    )

    first, second = runner.apply_filter(studyset, combine=False)

    assert list(first.ids) == ["s1-a1", "s2-a3"]
    assert second is None


def test_an_absent_note_is_not_selected():
    """NaN marks an analysis with no note. It is false, but is itself truthy."""
    runner, studyset = _runner(
        {"filter": "include", "conditions": [True], "weights": [1]},
        {"include": "boolean"},
        [_note("a1", True)],
    )

    first, _ = runner.apply_filter(studyset, combine=False)

    assert list(first.ids) == ["s1-a1"]


def test_a_filter_column_no_note_carries_selects_nothing():
    """An un-noted column must select nothing -- and say so rather than proceed.

    Selecting nothing is the safe half: a null note is truthy, so a bare test
    would take every analysis the column says nothing about. Saying so is the
    other half, since NiMARE reports an empty selection as a missing image type.
    """
    runner, studyset = _runner(
        {"filter": "missing", "conditions": [True], "weights": [1]},
        {"missing": "boolean"},
        [_note("a1", True)],
    )

    with pytest.raises(ValueError, match="No analysis is selected.*'missing'"):
        runner.apply_filter(studyset, combine=False)


def test_boolean_column_splits_into_two_groups():
    runner, studyset = _runner(
        {"filter": "include", "conditions": [True, False], "weights": [1, -1]},
        {"include": "boolean"},
        [_note("a1", True), _note("a2", False), _note("a3", True)],
    )

    first, second = runner.apply_filter(studyset, combine=False)

    assert list(first.ids) == ["s1-a1", "s2-a3"]
    assert list(second.ids) == ["s1-a2"]


def test_string_column_splits_on_the_weighted_conditions():
    runner, studyset = _runner(
        {"filter": "grp", "conditions": ["left", "right"], "weights": [1, -1]},
        {"grp": "string"},
        [
            _note("a1", "left", key="grp"),
            _note("a2", "right", key="grp"),
            _note("a3", "left", key="grp"),
        ],
    )

    first, second = runner.apply_filter(studyset, combine=False)

    assert list(first.ids) == ["s1-a1", "s2-a3"]
    assert list(second.ids) == ["s1-a2"]


def test_note_keys_carrying_an_order_is_read_as_its_type():
    """Annotations gained an "order", so a note key can be a dict."""
    runner, studyset = _runner(
        {"filter": "include", "conditions": [True], "weights": [1]},
        {"include": {"type": "boolean", "order": 0}},
        [_note("a1", True), _note("a2", False)],
    )

    first, _ = runner.apply_filter(studyset, combine=False)

    assert list(first.ids) == ["s1-a1"]


def test_an_unsupported_column_type_is_rejected():
    runner, studyset = _runner(
        {"filter": "include", "conditions": ["x"], "weights": [1]},
        {"include": "number"},
        [_note("a1", 1)],
    )

    with pytest.raises(ValueError, match="not supported"):
        runner.apply_filter(studyset, combine=False)
