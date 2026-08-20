"""Tests for routing image-based specifications to the right workflow."""

import pytest
from nimare.diagnostics import Jackknife
from nimare.meta.cbma import ALE, ALESubtraction
from nimare.meta.ibma import Fishers, Stouffers
from nimare.nimads import Studyset
from nimare.workflows import IBMAWorkflow

from compose_runner.run import Runner


@pytest.fixture
def runner(tmp_path):
    """A Runner with no network access, for exercising local logic only."""
    return Runner(
        meta_analysis_id="meta-id", environment="production", result_dir=tmp_path
    )


@pytest.mark.parametrize(
    ("spec_type", "expected"),
    [
        ("IBMA", True),
        ("ibma", True),
        ("  Ibma ", True),
        ("CBMA", False),
        ("cbma", False),
    ],
)
def test_is_image_based_is_case_insensitive(runner, spec_type, expected):
    """Specifications store the type uppercase; fixtures use lowercase."""
    runner.cached_specification = {"type": spec_type}

    assert runner._is_image_based() is expected


@pytest.mark.parametrize("spec", [{}, {"type": None}, {"type": ""}])
def test_is_image_based_defaults_to_false(runner, spec):
    """A missing type should not be mistaken for an image-based analysis."""
    runner.cached_specification = spec

    assert runner._is_image_based() is False


def test_ibma_estimator_selects_ibma_workflow(runner, monkeypatch):
    """An image-based estimator used to hit a ValueError."""
    captured = {}

    class FakeWorkflow:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, studyset):
            captured["fitted"] = studyset
            return _FakeResults()

    monkeypatch.setattr("compose_runner.run.IBMAWorkflow", FakeWorkflow)
    monkeypatch.setattr(Runner, "_persist_meta_results", lambda self: None)

    studyset = _empty_studyset()
    runner.estimator = Fishers()
    runner.corrector = None
    runner.first_studyset = studyset
    runner.second_studyset = None

    runner.run_meta_analysis()

    assert captured["fitted"] is studyset
    # FocusCounter counts foci, so IBMAWorkflow only accepts Jackknife. Named
    # rather than constructed, so the workflow applies voxel_thresh and
    # cluster_threshold -- it leaves an already-built diagnostic alone.
    assert captured["diagnostics"] == "jackknife"


def test_ibma_diagnostics_get_the_workflows_thresholds():
    """The workflow's cluster definition has to reach the diagnostic.

    The workflow's ``voxel_thresh`` arrives as the diagnostic's
    ``target_threshold``; ``voxel_thresh`` is the deprecated alias on the
    diagnostic and stays None.
    """
    workflow = IBMAWorkflow(estimator=Fishers(), diagnostics="jackknife")

    diagnostic = workflow.diagnostics[0]
    assert diagnostic.target_threshold == workflow.voxel_thresh == 1.65
    assert diagnostic.cluster_threshold == workflow.cluster_threshold == 10


def test_naming_and_constructing_a_diagnostic_agree():
    """Both spellings must define clusters the same way.

    They did not always: an already-constructed diagnostic used to be returned
    untouched, so ``Jackknife()`` kept None thresholds while ``"jackknife"`` got
    the workflow's. NiMARE now fills in whatever the caller left at its default,
    and this pins that -- it is the reason the runner can name the diagnostic
    without thinking about it.
    """
    named = IBMAWorkflow(estimator=Fishers(), diagnostics="jackknife").diagnostics[0]
    built = IBMAWorkflow(estimator=Fishers(), diagnostics=Jackknife()).diagnostics[0]

    for attribute in ("target_threshold", "cluster_threshold", "n_cores"):
        assert getattr(named, attribute) == getattr(built, attribute)


def test_an_explicitly_configured_diagnostic_is_left_alone():
    """Filling in unset parameters must not overwrite a deliberate choice."""
    workflow = IBMAWorkflow(
        estimator=Fishers(), diagnostics=Jackknife(target_threshold=3.0)
    )

    assert workflow.diagnostics[0].target_threshold == 3.0


def test_n_cores_reaches_the_ibma_workflow(runner, monkeypatch):
    """The workflow is what parallelizes the diagnostics."""
    captured = {}

    class FakeWorkflow:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, studyset):
            return _FakeResults()

    monkeypatch.setattr("compose_runner.run.IBMAWorkflow", FakeWorkflow)
    monkeypatch.setattr(Runner, "_persist_meta_results", lambda self: None)

    runner.estimator = Fishers()
    runner.corrector = None
    runner.first_studyset = _empty_studyset()
    runner.second_studyset = None
    runner.n_cores = 3

    runner.run_meta_analysis()

    assert captured["n_cores"] == 3


def test_ibma_estimator_rejects_group_comparison(runner):
    """No image-based estimator supports a two-group comparison."""
    runner.estimator = Stouffers()
    runner.corrector = None
    runner.first_studyset = "first"
    runner.second_studyset = "second"

    with pytest.raises(ValueError, match="no image-based estimator supports one"):
        runner.run_meta_analysis()


def test_cbma_estimator_still_selects_cbma_workflow(runner, monkeypatch):
    """The coordinate-based path must be untouched."""
    captured = {}

    class FakeWorkflow:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, studyset):
            captured["fitted"] = studyset
            return _FakeResults()

    monkeypatch.setattr("compose_runner.run.CBMAWorkflow", FakeWorkflow)
    monkeypatch.setattr(Runner, "_persist_meta_results", lambda self: None)

    runner.estimator = ALE()
    runner.corrector = None
    runner.first_studyset = "studyset"
    runner.second_studyset = None

    runner.run_meta_analysis()

    assert captured["diagnostics"] == "focuscounter"


def test_pairwise_cbma_still_selects_pairwise_workflow(runner, monkeypatch):
    """The pairwise coordinate-based path must be untouched."""
    captured = {}

    class FakeWorkflow:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, first, second):
            captured["fitted"] = (first, second)
            return _FakeResults()

    monkeypatch.setattr("compose_runner.run.PairwiseCBMAWorkflow", FakeWorkflow)
    monkeypatch.setattr(Runner, "_persist_meta_results", lambda self: None)

    runner.estimator = ALESubtraction()
    runner.corrector = None
    runner.first_studyset = "first"
    runner.second_studyset = "second"

    runner.run_meta_analysis()

    assert captured["fitted"] == ("first", "second")


def test_load_specification_resolves_ibma_estimator(runner):
    """Specification types are uppercase but module names are lowercase."""
    runner.cached_specification = {"type": "IBMA", "estimator": {"type": "Fishers"}}

    estimator, corrector = runner.load_specification()

    assert isinstance(estimator, Fishers)
    assert corrector is None


class _FakeResults:
    """Stand-in for a NiMARE MetaResult.

    Carries an estimator because the runner introspects the fitted result to
    report which analyses were used. A double that lacked one would only pass
    if the runner skipped that reporting when it failed -- which is exactly the
    silence the report exists to remove.
    """

    tables = {}
    description_ = ""

    def __init__(self, estimator=None, fitted_ids=()):
        self.estimator = estimator if estimator is not None else Fishers()
        self.estimator.inputs_ = {"id": list(fitted_ids)}
        self.estimator.dataset = None
        self.maps = {}


def _empty_studyset(analysis_ids=("a1",)):
    """The smallest Studyset the coverage report can be built from.

    No images: these tests never fit anything, they check which workflow gets
    chosen and with what arguments.
    """
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
                            "id": analysis_id,
                            "name": analysis_id,
                            "conditions": [],
                            "weights": [],
                            "points": [],
                            "metadata": {},
                            "images": [],
                        }
                        for analysis_id in analysis_ids
                    ],
                }
            ],
        }
    )
