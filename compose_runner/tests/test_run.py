import pytest
from requests.exceptions import HTTPError

from compose_runner import run as run_module
from compose_runner.run import Runner

@pytest.mark.vcr(record_mode="once")
def test_incorrect_id():
    runner = Runner(
        meta_analysis_id="made_up_id",
        environment="staging",
    )

    with pytest.raises(HTTPError):
        runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_download_bundle():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.download_bundle()
    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None


@pytest.mark.vcr(record_mode="once")
def test_run_workflow():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.run_workflow(n_cores=2)


@pytest.mark.vcr(record_mode="once")
def test_run_database_workflow():
    runner = Runner(
        meta_analysis_id="dRFtnAo9bhp3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_run_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="4CGQSSyaoWN3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="once")
def test_run_string_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="7joU2Siajs5X",
        environment="staging",
    )
    runner.run_workflow()


def test_process_bundle_keeps_studysets(monkeypatch):
    first_studyset = object()
    second_studyset = object()
    estimator = object()
    corrector = object()

    class FakeStudyset:
        def __init__(self, source):
            self.source = source

    class FakeAnnotation:
        def __init__(self, source, studyset):
            self.source = source
            self.studyset = studyset

    def fake_apply_filter(self, studyset, annotation):
        assert isinstance(studyset, FakeStudyset)
        assert isinstance(annotation, FakeAnnotation)
        return first_studyset, second_studyset

    def fake_load_specification(self, n_cores=None):
        assert n_cores == 3
        return estimator, corrector

    monkeypatch.setattr(run_module, "Studyset", FakeStudyset)
    monkeypatch.setattr(run_module, "Annotation", FakeAnnotation)
    monkeypatch.setattr(Runner, "apply_filter", fake_apply_filter)
    monkeypatch.setattr(Runner, "load_specification", fake_load_specification)

    runner = Runner(meta_analysis_id="made_up_id", environment="staging")
    runner.cached_studyset = {"id": "studyset", "studies": []}
    runner.cached_annotation = {"note_keys": {}}

    runner.process_bundle(n_cores=3)

    assert runner.first_studyset is first_studyset
    assert runner.second_studyset is second_studyset
    assert runner.estimator is estimator
    assert runner.corrector is corrector


def test_run_meta_analysis_single_studyset_uses_cbma_workflow(monkeypatch, tmp_path):
    calls = {}

    class FakeCBMAEstimator:
        pass

    class FakeWorkflow:
        def __init__(self, estimator, corrector, diagnostics, output_dir):
            calls["init"] = {
                "estimator": estimator,
                "corrector": corrector,
                "diagnostics": diagnostics,
                "output_dir": output_dir,
            }

        def fit(self, dataset):
            calls["fit"] = {"dataset": dataset}
            return "meta-results"

    monkeypatch.setattr(run_module, "CBMAEstimator", FakeCBMAEstimator)
    monkeypatch.setattr(run_module, "CBMAWorkflow", FakeWorkflow)

    runner = Runner(meta_analysis_id="made_up_id", environment="staging", result_dir=tmp_path)
    runner.first_studyset = object()
    runner.second_studyset = None
    runner.estimator = FakeCBMAEstimator()
    runner.corrector = object()
    runner._persist_meta_results = lambda: None

    runner.run_meta_analysis()

    assert calls["fit"] == {"dataset": runner.first_studyset}
    assert runner.meta_results == "meta-results"


def test_run_meta_analysis_pairwise_uses_pairwise_workflow(monkeypatch, tmp_path):
    calls = {}

    class FakePairwiseEstimator:
        pass

    class FakeWorkflow:
        def __init__(self, estimator, corrector, diagnostics, output_dir):
            calls["init"] = {
                "estimator": estimator,
                "corrector": corrector,
                "diagnostics": diagnostics,
                "output_dir": output_dir,
            }

        def fit(self, dataset1, dataset2):
            calls["fit"] = {"dataset1": dataset1, "dataset2": dataset2}
            return "pairwise-results"

    monkeypatch.setattr(run_module, "PairwiseCBMAEstimator", FakePairwiseEstimator)
    monkeypatch.setattr(run_module, "PairwiseCBMAWorkflow", FakeWorkflow)

    runner = Runner(meta_analysis_id="made_up_id", environment="staging", result_dir=tmp_path)
    runner.first_studyset = object()
    runner.second_studyset = object()
    runner.estimator = FakePairwiseEstimator()
    runner.corrector = object()
    runner._persist_meta_results = lambda: None

    runner.run_meta_analysis()

    assert calls["fit"] == {
        "dataset1": runner.first_studyset,
        "dataset2": runner.second_studyset,
    }
    assert runner.meta_results == "pairwise-results"

# def test_yifan_workflow():
#     runner = Runner(
#         meta_analysis_id="4WELjap2yCJm",
#     )
#     runner.run_workflow()


# @pytest.mark.vcr(record_mode="once")
# def test_mkdachis_comparison_workflow():
#     runner = Runner(
#         meta_analysis_id="6Grzwzs3t7YB",
#     )
#     runner.run_workflow()
