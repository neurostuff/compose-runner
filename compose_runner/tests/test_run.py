import pytest
from requests.exceptions import HTTPError

from compose_runner import run as run_module
from compose_runner.run import Runner


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"status {self.status_code}")

    def json(self):
        return self.payload


@pytest.mark.vcr(record_mode="none")
def test_incorrect_id():
    runner = Runner(
        meta_analysis_id="made_up_id",
        environment="staging",
    )

    with pytest.raises(HTTPError):
        runner.run_workflow()


@pytest.mark.vcr(record_mode="none")
def test_download_bundle():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.download_bundle()
    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None


@pytest.mark.vcr(record_mode="none")
def test_run_workflow():
    runner = Runner(
        meta_analysis_id="3opENJpHxRsH",
        environment="staging",
    )
    runner.run_workflow(n_cores=2)


@pytest.mark.vcr(record_mode="none")
def test_run_database_workflow():
    runner = Runner(
        meta_analysis_id="dRFtnAo9bhp3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="none")
def test_run_group_comparison_workflow():
    runner = Runner(
        meta_analysis_id="4CGQSSyaoWN3",
        environment="staging",
    )
    runner.run_workflow()


@pytest.mark.vcr(record_mode="none")
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


def test_download_bundle_supports_legacy_snapshot_shape(monkeypatch):
    runner = Runner(meta_analysis_id="legacy-meta-analysis", environment="staging")
    meta_analysis = {
        "studyset": {
            "snapshot": {"snapshot": {"id": "studyset", "studies": []}},
            "neurostore_id": "legacy-studyset",
        },
        "annotation": {
            "snapshot": {"snapshot": {"id": "annotation", "notes": [], "note_keys": {}}},
            "neurostore_id": "legacy-annotation",
        },
        "specification": {"filter": "included"},
        "run_key": "legacy-run-key",
    }
    live_studyset = {"id": "legacy-studyset", "studies": [{"id": "live-study"}]}
    live_annotation = {
        "id": "legacy-annotation",
        "notes": [{"id": "live-note"}],
        "note_keys": {},
    }
    requested_urls = []

    def fake_get(url):
        requested_urls.append(url)
        payloads = {
            (
                f"{runner.compose_url}/meta-analyses/"
                f"{runner.meta_analysis_id}?nested=true"
            ): meta_analysis,
            f"{runner.store_url}/studysets/legacy-studyset?nested=true": live_studyset,
            f"{runner.store_url}/annotations/legacy-annotation": live_annotation,
        }
        return FakeResponse(payloads[url])

    monkeypatch.setattr(run_module.requests, "get", fake_get)

    runner.download_bundle()

    assert runner.cached_studyset == live_studyset
    assert runner.cached_annotation == live_annotation
    assert runner.existing_studyset_snapshot == {"id": "studyset", "studies": []}
    assert runner.existing_annotation_snapshot == {
        "id": "annotation",
        "notes": [],
        "note_keys": {},
    }
    assert runner.cached is False
    assert runner.nsc_key == "legacy-run-key"
    assert requested_urls == [
        f"{runner.compose_url}/meta-analyses/{runner.meta_analysis_id}?nested=true",
        f"{runner.store_url}/studysets/legacy-studyset?nested=true",
        f"{runner.store_url}/annotations/legacy-annotation",
    ]


def test_download_bundle_supports_result_snapshot_shape(monkeypatch):
    runner = Runner(meta_analysis_id="new-meta-analysis", environment="development")
    meta_analysis = {
        "project": "project-1",
        "results": [{"id": "result-1"}],
        "snapshots": [],
        "specification": {"filter": "included"},
        "run_key": "new-run-key",
        "neurostore_studyset": None,
        "neurostore_annotation": None,
    }
    project = {
        "id": "project-1",
        "neurostore_studyset": None,
        "neurostore_annotation": None,
    }
    result = {
        "id": "result-1",
        "studyset_snapshot": {"id": "studyset", "studies": []},
        "annotation_snapshot": {"id": "annotation", "notes": [], "note_keys": {}},
    }
    requested_urls = []

    def fake_get(url):
        requested_urls.append(url)
        payloads = {
            (
                f"{runner.compose_url}/meta-analyses/"
                f"{runner.meta_analysis_id}?nested=true"
            ): meta_analysis,
            f"{runner.compose_url}/projects/project-1": project,
            f"{runner.compose_url}/meta-analysis-results/result-1": result,
        }
        return FakeResponse(payloads[url])

    monkeypatch.setattr(run_module.requests, "get", fake_get)

    runner.download_bundle()

    assert runner.cached_studyset == {"id": "studyset", "studies": []}
    assert runner.cached_annotation == {"id": "annotation", "notes": [], "note_keys": {}}
    assert runner.cached is True
    assert requested_urls == [
        f"{runner.compose_url}/meta-analyses/{runner.meta_analysis_id}?nested=true",
        f"{runner.compose_url}/meta-analysis-results/result-1",
        f"{runner.compose_url}/projects/project-1",
    ]


def test_create_result_object_links_matching_existing_snapshots(monkeypatch):
    runner = Runner(meta_analysis_id="meta-analysis-1", environment="staging")
    runner.nsc_key = "run-key"
    runner.cached_studyset = {"id": "studyset-1", "studies": [{"id": "study-1"}]}
    runner.cached_annotation = {
        "id": "annotation-1",
        "notes": [{"id": "note-1"}],
        "note_keys": {"included": {"type": "boolean"}},
    }
    runner.existing_studyset_snapshot = {
        "studies": [{"id": "study-1"}],
        "id": "studyset-1",
    }
    runner.existing_annotation_snapshot = {
        "note_keys": {"included": {"type": "boolean"}},
        "notes": [{"id": "note-1"}],
        "id": "annotation-1",
    }
    runner.existing_studyset_snapshot_id = "cached-studyset-1"
    runner.existing_annotation_snapshot_id = "cached-annotation-1"
    posted = {}

    def fake_post(url, json, headers):
        posted["url"] = url
        posted["json"] = json
        posted["headers"] = headers
        return FakeResponse({"id": "result-1"})

    monkeypatch.setattr(run_module.requests, "post", fake_post)

    runner.create_result_object()

    assert posted == {
        "url": f"{runner.compose_url}/meta-analysis-results",
        "json": {
            "meta_analysis_id": "meta-analysis-1",
            "cached_studyset": "cached-studyset-1",
            "cached_annotation": "cached-annotation-1",
        },
        "headers": {"Compose-Upload-Key": "run-key"},
    }
    assert runner.result_id == "result-1"


def test_create_result_object_uploads_only_changed_live_snapshots(monkeypatch):
    runner = Runner(meta_analysis_id="meta-analysis-1", environment="staging")
    runner.nsc_key = "run-key"
    runner.cached_studyset = {"id": "studyset-1", "studies": [{"id": "study-1"}]}
    runner.cached_annotation = {
        "id": "annotation-1",
        "notes": [{"id": "note-2"}],
        "note_keys": {"included": {"type": "boolean"}},
    }
    runner.existing_studyset_snapshot = {
        "id": "studyset-1",
        "studies": [{"id": "study-1"}],
    }
    runner.existing_annotation_snapshot = {
        "id": "annotation-1",
        "notes": [{"id": "note-1"}],
        "note_keys": {"included": {"type": "boolean"}},
    }
    runner.existing_studyset_snapshot_id = "cached-studyset-1"
    runner.existing_annotation_snapshot_id = "cached-annotation-1"
    posted = {}

    def fake_post(url, json, headers):
        posted["url"] = url
        posted["json"] = json
        posted["headers"] = headers
        return FakeResponse({"id": "result-1"})

    monkeypatch.setattr(run_module.requests, "post", fake_post)

    runner.create_result_object()

    assert posted == {
        "url": f"{runner.compose_url}/meta-analysis-results",
        "json": {
            "meta_analysis_id": "meta-analysis-1",
            "cached_studyset": "cached-studyset-1",
            "annotation_snapshot": runner.cached_annotation,
        },
        "headers": {"Compose-Upload-Key": "run-key"},
    }
    assert runner.result_id == "result-1"


def test_download_bundle_uses_project_neurostore_ids_for_new_shape(monkeypatch):
    runner = Runner(meta_analysis_id="new-meta-analysis", environment="staging")
    meta_analysis = {
        "project": "project-1",
        "results": [],
        "snapshots": [],
        "specification": {"filter": "included"},
        "run_key": "new-run-key",
        "neurostore_studyset": None,
        "neurostore_annotation": None,
    }
    project = {
        "id": "project-1",
        "neurostore_studyset_id": "studyset-1",
        "neurostore_annotation_id": "annotation-1",
    }
    studyset = {"id": "studyset-1", "studies": []}
    annotation = {"id": "annotation-1", "notes": [], "note_keys": {}}
    requested_urls = []

    def fake_get(url):
        requested_urls.append(url)
        payloads = {
            (
                f"{runner.compose_url}/meta-analyses/"
                f"{runner.meta_analysis_id}?nested=true"
            ): meta_analysis,
            f"{runner.compose_url}/projects/project-1": project,
            f"{runner.store_url}/studysets/studyset-1?nested=true": studyset,
            f"{runner.store_url}/annotations/annotation-1": annotation,
        }
        return FakeResponse(payloads[url])

    monkeypatch.setattr(run_module.requests, "get", fake_get)

    runner.download_bundle()

    assert runner.cached_studyset == studyset
    assert runner.cached_annotation == annotation
    assert runner.cached is False
    assert requested_urls == [
        f"{runner.compose_url}/meta-analyses/{runner.meta_analysis_id}?nested=true",
        f"{runner.compose_url}/projects/project-1",
        f"{runner.store_url}/studysets/studyset-1?nested=true",
        f"{runner.store_url}/annotations/annotation-1",
    ]


def test_download_bundle_uses_meta_analysis_neurostore_object_ids(monkeypatch):
    runner = Runner(meta_analysis_id="new-meta-analysis", environment="development")
    meta_analysis = {
        "project": "project-1",
        "results": [],
        "snapshots": [],
        "specification": {"filter": "included"},
        "run_key": "new-run-key",
        "neurostore_studyset": {"id": "studyset-1", "studysets": ["source-studyset"]},
        "neurostore_annotation": {"id": "annotation-1"},
    }
    studyset = {"id": "studyset-1", "studies": []}
    annotation = {"id": "annotation-1", "notes": [], "note_keys": {}}
    requested_urls = []

    def fake_get(url):
        requested_urls.append(url)
        payloads = {
            (
                f"{runner.compose_url}/meta-analyses/"
                f"{runner.meta_analysis_id}?nested=true"
            ): meta_analysis,
            f"{runner.store_url}/studysets/studyset-1?nested=true": studyset,
            f"{runner.store_url}/annotations/annotation-1": annotation,
        }
        return FakeResponse(payloads[url])

    monkeypatch.setattr(run_module.requests, "get", fake_get)

    runner.download_bundle()

    assert runner.cached_studyset == studyset
    assert runner.cached_annotation == annotation
    assert runner.cached is False
    assert requested_urls == [
        f"{runner.compose_url}/meta-analyses/{runner.meta_analysis_id}?nested=true",
        f"{runner.store_url}/studysets/studyset-1?nested=true",
        f"{runner.store_url}/annotations/annotation-1",
    ]


def test_download_bundle_falls_back_to_compose_linked_store_ids(monkeypatch):
    runner = Runner(meta_analysis_id="linked-meta-analysis", environment="development")
    meta_analysis = {
        "project": "project-1",
        "results": [],
        "snapshots": [],
        "specification": {"filter": "included"},
        "run_key": "new-run-key",
        "neurostore_studyset": "compose-studyset-1",
        "neurostore_annotation": "compose-annotation-1",
    }
    compose_studyset = {"id": "compose-studyset-1", "studysets": [{"id": "store-studyset-1"}]}
    compose_annotation = {
        "id": "compose-annotation-1",
        "annotations": [{"id": "store-annotation-1"}],
    }
    studyset = {"id": "store-studyset-1", "studies": []}
    annotation = {"id": "store-annotation-1", "notes": [], "note_keys": {}}
    requested_urls = []

    def fake_get(url):
        requested_urls.append(url)
        payloads = {
            (
                f"{runner.compose_url}/meta-analyses/"
                f"{runner.meta_analysis_id}?nested=true"
            ): meta_analysis,
            f"{runner.compose_url}/neurostore-studysets/compose-studyset-1": compose_studyset,
            f"{runner.compose_url}/neurostore-annotations/compose-annotation-1": compose_annotation,
            f"{runner.store_url}/studysets/store-studyset-1?nested=true": studyset,
            f"{runner.store_url}/annotations/store-annotation-1": annotation,
        }
        if url == f"{runner.store_url}/studysets/compose-studyset-1?nested=true":
            return FakeResponse({}, status_code=404)
        if url == f"{runner.store_url}/annotations/compose-annotation-1":
            return FakeResponse({}, status_code=404)
        return FakeResponse(payloads[url])

    monkeypatch.setattr(run_module.requests, "get", fake_get)

    runner.download_bundle()

    assert runner.cached_studyset == studyset
    assert runner.cached_annotation == annotation
    assert runner.cached is False
    assert requested_urls == [
        f"{runner.compose_url}/meta-analyses/{runner.meta_analysis_id}?nested=true",
        f"{runner.store_url}/studysets/compose-studyset-1?nested=true",
        f"{runner.compose_url}/neurostore-studysets/compose-studyset-1",
        f"{runner.store_url}/studysets/store-studyset-1?nested=true",
        f"{runner.store_url}/annotations/compose-annotation-1",
        f"{runner.compose_url}/neurostore-annotations/compose-annotation-1",
        f"{runner.store_url}/annotations/store-annotation-1",
    ]


@pytest.mark.vcr(record_mode="once")
def test_download_bundle_dev_meta_analysis_shape():
    runner = Runner(
        meta_analysis_id="VR2eJbv3BJCi",
        environment="development",
    )

    runner.download_bundle()

    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None
    assert runner.cached is False
    assert runner.cached_studyset["id"] == "8Tc9iMdR4uwR"
    assert runner.cached_annotation["id"] == "vvigrL8Wv75H"
    assert runner.cached_annotation["note_keys"]["included"]["type"] == "boolean"


@pytest.mark.vcr(record_mode="once")
def test_download_bundle_dev_string_id_shape():
    runner = Runner(
        meta_analysis_id="jeqZ65Bsnniw",
        environment="development",
    )

    runner.download_bundle()

    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None
    assert runner.cached is False
    assert runner.cached_studyset["id"] == "MYFAqDMNBryo"
    assert runner.cached_annotation["id"] == "ExiEeALEEvUb"
    assert runner.cached_annotation["note_keys"]["included"]["type"] == "boolean"


@pytest.mark.vcr(record_mode="once")
def test_download_bundle_production_cached_snapshot_shape():
    runner = Runner(
        meta_analysis_id="mHtoV82Dmnm9",
        environment="production",
    )

    runner.download_bundle()

    assert runner.cached_studyset is not None
    assert runner.cached_annotation is not None
    assert runner.cached_specification is not None
    assert runner.cached is False
    assert runner.existing_studyset_snapshot_id == "iTjiYLR4aWcs"
    assert runner.existing_annotation_snapshot_id == "XijxA4i3hb8S"
    assert runner.existing_studyset_snapshot is not None
    assert runner.existing_annotation_snapshot is not None
    assert runner.cached_studyset["id"] == "n45qe4g5nrFw"
    assert runner.cached_annotation["id"] == "mmaYdBWkKPoQ"
    assert isinstance(runner.cached_studyset["studies"], list)
    assert runner.cached_annotation["note_keys"]["included"]["type"] == "boolean"


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
