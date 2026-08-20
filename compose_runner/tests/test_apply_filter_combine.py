"""Tests for keeping a study's analyses separate on the image-based path.

``combine_analyses`` merges every analysis of a study into one. That is right
for coordinate-based work, where the point is to pool a study's foci, but for
image-based work it does two damaging things:

* it concatenates the study's images into a single analysis, and converting to
  a Dataset keeps only one map per type, so the other contrasts vanish;
* it collapses the study grouping NiMARE relies on to detect, and correct for,
  dependence between a study's contrasts.
"""

import pytest

from compose_runner.run import Runner


class FakeStudyset:
    """Records whether combine_analyses was called."""

    def __init__(self, name="studyset"):
        self.name = name
        self.combined = False
        self.study_ids = ["study-1"]

    def slice(self, analyses=None):  # noqa: ARG002
        sliced = FakeStudyset(self.name)
        return sliced

    def combine_analyses(self):
        self.combined = True
        return self

    def update_path(self, new_path):
        self.base_path = new_path
        return self


class FakeNote:
    def __init__(self, analysis_id, note):
        self.analysis = type("Analysis", (), {"id": analysis_id})()
        self.note = note


class FakeAnnotation:
    def __init__(self, notes):
        self.notes = notes


@pytest.fixture
def runner(tmp_path):
    """A Runner with a boolean single-condition specification."""
    runner = Runner(
        meta_analysis_id="meta-id", environment="production", result_dir=tmp_path
    )
    runner.cached_specification = {
        "filter": "include",
        "conditions": [True],
        "weights": [1],
    }
    runner.cached_annotation = {"note_keys": {"include": "boolean"}}
    return runner


@pytest.fixture
def annotation():
    return FakeAnnotation([FakeNote("a1", {"include": True})])


def test_combine_true_merges_analyses(runner, annotation):
    """The coordinate-based path must keep merging, as it always has."""
    studyset = FakeStudyset()

    first, second = runner.apply_filter(studyset, annotation, combine=True)

    assert first.combined is True
    assert second is None


def test_combine_false_keeps_analyses_separate(runner, annotation):
    """The image-based path must not merge."""
    studyset = FakeStudyset()

    first, second = runner.apply_filter(studyset, annotation, combine=False)

    assert first.combined is False
    assert second is None


def test_combine_defaults_to_true(runner, annotation):
    """Existing callers should be unaffected."""
    studyset = FakeStudyset()

    first, _ = runner.apply_filter(studyset, annotation)

    assert first.combined is True


def test_process_bundle_skips_combining_for_ibma(runner, monkeypatch, tmp_path):
    """An IBMA specification should reach apply_filter with combine=False."""
    captured = {}

    def fake_apply_filter(self, studyset, annotation, combine=True):  # noqa: ARG001
        captured["combine"] = combine
        return FakeStudyset(), None

    monkeypatch.setattr(Runner, "apply_filter", fake_apply_filter)
    monkeypatch.setattr(Runner, "prepare_images", lambda self: None)
    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (None, None)
    )
    monkeypatch.setattr("compose_runner.run.Studyset", lambda *a, **kw: FakeStudyset())
    monkeypatch.setattr("compose_runner.run.Annotation", lambda *a, **kw: None)

    runner.cached_specification = {"type": "IBMA", "filter": "include"}
    runner.cached_studyset = {"studies": []}
    runner.process_bundle()

    assert captured["combine"] is False


def test_process_bundle_combines_for_cbma(runner, monkeypatch):
    """A CBMA specification must keep the existing merging behaviour."""
    captured = {}

    def fake_apply_filter(self, studyset, annotation, combine=True):  # noqa: ARG001
        captured["combine"] = combine
        return FakeStudyset(), None

    monkeypatch.setattr(Runner, "apply_filter", fake_apply_filter)
    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (None, None)
    )
    monkeypatch.setattr("compose_runner.run.Studyset", lambda *a, **kw: FakeStudyset())
    monkeypatch.setattr("compose_runner.run.Annotation", lambda *a, **kw: None)

    runner.cached_specification = {"type": "CBMA", "filter": "include"}
    runner.cached_studyset = {"studies": []}
    runner.process_bundle()

    assert captured["combine"] is True


def test_process_bundle_only_prepares_images_for_ibma(runner, monkeypatch):
    """Coordinate-based runs should not download anything."""
    calls = []

    monkeypatch.setattr(Runner, "prepare_images", lambda self: calls.append("prepared"))
    monkeypatch.setattr(
        Runner,
        "apply_filter",
        lambda self, studyset, annotation, combine=True: (
            FakeStudyset(),
            None,
        ),  # noqa: ARG005
    )
    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (None, None)
    )
    monkeypatch.setattr("compose_runner.run.Studyset", lambda *a, **kw: FakeStudyset())
    monkeypatch.setattr("compose_runner.run.Annotation", lambda *a, **kw: None)

    runner.cached_specification = {"type": "CBMA", "filter": "include"}
    runner.cached_studyset = {"studies": []}
    runner.process_bundle()

    assert calls == []
