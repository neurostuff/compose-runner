"""Tests for routing an image-based specification to the right workflow."""

import pandas as pd
import pytest
from nimare.meta.ibma import Fishers, Stouffers
from nimare.nimads import Studyset

from compose_runner.run import Runner


@pytest.fixture
def runner(tmp_path):
    runner = Runner(
        meta_analysis_id="meta-id", environment="production", result_dir=tmp_path
    )
    runner.cached_specification = {
        "filter": "include",
        "conditions": [True],
        "weights": [1],
    }
    runner.cached_annotation = {"note_keys": {"include": "boolean"}}
    runner.cached_studyset = {"studies": []}
    return runner


class FakeStudyset:
    """Records whether combine_analyses and update_path were called.

    Carries its own notes, as a studyset does: ``annotations_df`` is one row per
    analysis, keyed by the full ``study-analysis`` id, and a boolean note key
    reaches it as a float because annotation values are arithmetic.
    """

    def __init__(self):
        self.combined = False
        self.base_path = None
        self.sliced = None

    @property
    def annotations_df(self):
        return pd.DataFrame({"id": ["s1-a1"], "include": [1.0]})

    def slice(self, analyses=None):
        sliced = FakeStudyset()
        self.sliced = list(analyses or [])
        return sliced

    def combine_analyses(self):
        self.combined = True
        return self

    def update_path(self, new_path):
        self.base_path = new_path
        return self


class FakeResults:
    """Stand-in for a MetaResult, carrying the estimator coverage reads."""

    tables = {}
    description_ = ""

    def __init__(self):
        self.estimator = Fishers()
        self.estimator.inputs_ = {"id": []}
        self.estimator.dataset = None
        self.maps = {}


def _empty_studyset():
    """The smallest Studyset a coverage report can be built from."""
    return Studyset(
        {
            "id": "ss",
            "name": "ss",
            "studies": [
                {
                    "id": "s1",
                    "name": "s1",
                    "metadata": {},
                    "analyses": [
                        {
                            "id": "a1",
                            "name": "a1",
                            "conditions": [],
                            "weights": [],
                            "points": [],
                            "metadata": {},
                            "images": [],
                        }
                    ],
                }
            ],
        }
    )


@pytest.mark.parametrize(
    ("spec_type", "expected"),
    [("IBMA", True), ("ibma", True), ("  Ibma ", True), ("CBMA", False), (None, False)],
)
def test_is_image_based(runner, spec_type, expected):
    """Specifications store the type uppercase; fixtures use lowercase."""
    runner.cached_specification = {"type": spec_type}

    assert runner._is_image_based() is expected


@pytest.mark.parametrize("combine", [True, False])
def test_apply_filter_combines_only_when_asked(runner, combine):
    studyset = FakeStudyset()

    first, second = runner.apply_filter(studyset, combine)

    assert studyset.sliced == ["s1-a1"]
    assert first.combined is combine
    assert second is None


@pytest.mark.parametrize(
    ("spec_type", "combine", "prepared"),
    [("IBMA", False, True), ("CBMA", True, False)],
)
def test_process_bundle_prepares_images_only_for_ibma(
    runner, monkeypatch, spec_type, combine, prepared
):
    """IBMA must not merge a study's analyses, and CBMA must not download."""
    captured = {}

    def fake_apply_filter(self, studyset, combine=True):
        captured["combine"] = combine
        return FakeStudyset(), None

    def fake_prepare_images(self):
        captured["prepared"] = True
        return {"studies": []}

    monkeypatch.setattr(Runner, "apply_filter", fake_apply_filter)
    monkeypatch.setattr(Runner, "prepare_images", fake_prepare_images)
    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (None, None)
    )
    monkeypatch.setattr("compose_runner.run.Studyset", lambda *a, **kw: FakeStudyset())
    runner.cached_specification["type"] = spec_type

    runner.process_bundle()

    assert captured["combine"] is combine
    assert captured.get("prepared", False) is prepared


def test_ibma_estimator_selects_ibma_workflow(runner, monkeypatch):
    captured = {}

    class FakeWorkflow:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, studyset):
            captured["fitted"] = studyset
            return FakeResults()

    monkeypatch.setattr("compose_runner.run.IBMAWorkflow", FakeWorkflow)
    monkeypatch.setattr(Runner, "_persist_meta_results", lambda self: None)

    studyset = _empty_studyset()
    runner.estimator = Fishers()
    runner.first_studyset = studyset
    runner.n_cores = 3

    runner.run_meta_analysis()

    assert captured["fitted"] is studyset
    # Jackknife is the only diagnostic IBMAWorkflow accepts, and naming it
    # rather than constructing it lets the workflow set the cluster thresholds.
    assert captured["diagnostics"] == "jackknife"
    # The workflow, not the estimator, is what parallelizes an IBMA.
    assert captured["n_cores"] == 3


def test_ibma_estimator_rejects_group_comparison(runner):
    """No image-based estimator supports a two-group comparison."""
    runner.estimator = Stouffers()
    runner.first_studyset = "first"
    runner.second_studyset = "second"

    with pytest.raises(ValueError, match="no image-based estimator supports one"):
        runner.run_meta_analysis()


@pytest.mark.parametrize(
    ("key", "value"),
    [("conditions", [True, False]), ("database_studyset", "neurostore")],
    ids=["two-conditions", "database"],
)
def test_an_image_based_comparison_is_refused_before_the_work(
    runner, monkeypatch, key, value
):
    """The specification already says this cannot run, so nothing should be fetched.

    ``run_meta_analysis`` catches it too, but only after the maps are staged
    and, for a database comparison, after the reference studyset has been
    downloaded.
    """
    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (Stouffers(), None)
    )
    monkeypatch.setattr(
        Runner,
        "prepare_images",
        lambda self: pytest.fail("images must not be staged"),
    )
    runner.cached_specification.update(
        {"type": "IBMA", "estimator": {"type": "Stouffers"}, key: value}
    )

    with pytest.raises(ValueError, match="no image-based estimator supports one"):
        runner.process_bundle()


def test_a_coordinate_based_run_gets_its_sample_sizes(runner, monkeypatch):
    """ALE's kernel needs one per experiment, and compose records it on the note.

    One analysis without a sample size fails the whole run, so the copy that
    ``apply_sample_sizes`` makes has to happen on this path too -- and on a
    copy, since ``cached_studyset`` is uploaded as the result's snapshot.
    """
    captured = {}

    monkeypatch.setattr(
        Runner, "load_specification", lambda self, n_cores=None: (None, None)
    )
    monkeypatch.setattr(Runner, "apply_filter", lambda *a, **kw: (FakeStudyset(), None))

    def fake_studyset(payload, **kwargs):
        captured["studyset"] = payload
        return FakeStudyset()

    monkeypatch.setattr("compose_runner.run.Studyset", fake_studyset)
    runner.cached_specification["type"] = "CBMA"
    runner.cached_studyset = {
        "studies": [
            {
                "id": "s1",
                "metadata": {},
                "analyses": [{"id": "a1", "metadata": {}, "points": []}],
            }
        ]
    }
    runner.cached_annotation = {
        "note_keys": {"include": "boolean", "sample_size": "number"},
        "notes": [{"study": "s1", "analysis": "a1", "note": {"sample_size": 25}}],
    }

    runner.process_bundle()

    analysis = captured["studyset"]["studies"][0]["analyses"][0]
    assert analysis["metadata"]["sample_sizes"] == [25.0]
    # The uploaded snapshot keeps what Neurostore served.
    assert runner.cached_studyset["studies"][0]["analyses"][0]["metadata"] == {}


def test_a_relative_result_dir_is_resolved(tmp_path, monkeypatch):
    """Staged maps are recorded as paths, and NiMARE resolves relative ones.

    The base path it resolves against is the image directory itself, so a
    relative result_dir has that directory prepended to a path already holding
    it and every map goes missing.
    """
    monkeypatch.chdir(tmp_path)

    runner = Runner(meta_analysis_id="meta-id", result_dir="results/run1")

    assert runner.result_dir.is_absolute()
    assert runner.image_dir == tmp_path / "results" / "run1" / "images"
