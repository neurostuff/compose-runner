from importlib import import_module
from pathlib import Path

import requests

import neurosynth_compose_sdk
from neurosynth_compose_sdk.api.compose_api import ComposeApi
import neurostore_sdk
from neurostore_sdk.api.store_api import StoreApi
from nimare.workflows import cbma_workflow
from nimare.nimads import Studyset, Annotation


class Runner:
    """Runner for executing and uploading a meta-analysis workflow."""

    def __init__(
        self,
        meta_analysis_id,
        staging=False,
        result_dir=None,
        nsc_key=None,
        nv_key=None,
    ):
        # the meta-analysis id associated with this run
        self.meta_analysis_id = meta_analysis_id

        if staging:
            # staging
            self.compose_url = "https://synth.neurostore.xyz/api"
            self.store_url = "https://neurostore.xyz/api"
        else:
            # production
            self.compose_url = "https://compose.neurosynth.org/api"
            self.store_url = "https://neurostore.org/api"

        # Enter a context with an instance of the API client
        compose_configuration = neurosynth_compose_sdk.Configuration(
            host=self.compose_url
        )
        store_configuration = neurostore_sdk.Configuration(host=self.store_url)
        compose_client = neurosynth_compose_sdk.ApiClient(compose_configuration)
        store_client = neurostore_sdk.ApiClient(store_configuration)
        self.compose_api = ComposeApi(compose_client)
        self.store_api = StoreApi(store_client)

        # initialize inputs
        self.cached_studyset = None
        self.cached_annotation = None
        self.cached_specification = None
        self.dataset = None
        self.estimator = None
        self.corrector = None

        # initialize api-keys
        self.nsc_key = nsc_key  # neurosynth compose key to upload to neurosynth compose
        self.nv_key = nv_key  # neurovault key to upload to neurovault

        # result directory
        if result_dir is None:
            self.result_dir = Path.cwd() / "results"
        else:
            self.result_dir = Path(result_dir)

        # whether the inputs were cached from neurostore
        self.cached = True

        # initialize outputs
        self.result_id = None
        self.meta_results = None  # the meta-analysis result output from nimare
        self.results_object = (
            None  # the result object represented on neurosynth compose
        )

    def run_workflow(self):
        self.download_bundle()
        self.process_bundle()
        self.create_result_object()
        self.run_meta_analysis()
        self.upload_results()

    def download_bundle(self):
        meta_analysis = requests.get(
            f"{self.compose_url}/meta-analyses/{self.meta_analysis_id}?nested=true"
        ).json()
        # meta_analysis = self.compose_api.meta_analyses_id_get(
        #     id=self.meta_analysis_id, nested=True
        # ).to_dict()  # does not currently return run_key

        # check to see if studyset and annotation are cached
        studyset_dict = annotation_dict = None
        if meta_analysis["studyset"]:
            studyset_dict = meta_analysis["studyset"]["snapshot"]
            self.cached_studyset = (
                None if studyset_dict is None else studyset_dict.get("snapshot", None)
            )
        if meta_analysis["annotation"]:
            annotation_dict = meta_analysis["annotation"]["snapshot"]
            self.cached_annotation = (
                None
                if annotation_dict is None
                else annotation_dict.get("snapshot", None)
            )
        # if either are not cached, download them from neurostore
        if self.cached_studyset is None or self.cached_annotation is None:
            self.cached_studyset = requests.get(
                f"{self.store_url}/studysets/{meta_analysis['studyset']['neurostore_id']}?nested=true"
            ).json()
            self.cached_annotation = requests.get(
                f"{self.store_url}/annotations/{meta_analysis['annotation']['neurostore_id']}"
            ).json()
            # set cached to false
            self.cached = False
        # retrieve specification
        self.cached_specification = meta_analysis["specification"]

        # run key for running this particular meta-analysis
        self.nsc_key = meta_analysis["run_key"]

    def process_bundle(self):
        studyset = Studyset(self.cached_studyset)
        annotation = Annotation(self.cached_annotation, studyset)
        include = self.cached_specification["filter"]
        analysis_ids = [n.analysis.id for n in annotation.notes if n.note[f"{include}"]]
        filtered_studyset = studyset.slice(analyses=analysis_ids)
        dataset = filtered_studyset.to_dataset()
        estimator, corrector = self.load_specification()
        self.dataset = dataset
        self.estimator = estimator
        self.corrector = corrector

    def create_result_object(self):
        # take a snapshot of the studyset and annotation (before running the workflow)
        headers = {"Compose-Upload-Key": self.nsc_key}
        data = {"meta_analysis_id": self.meta_analysis_id}
        if not self.cached:
            data.update(
                {
                    "studyset_snapshot": self.cached_studyset,
                    "annotation_snapshot": self.cached_annotation,
                }
            )
        resp = requests.post(
            f"{self.compose_url}/meta-analysis-results",
            json=data,
            headers=headers,
        )
        self.result_id = resp.json().get("id", None)
        if self.result_id is None:
            raise ValueError(f"Could not create result for {self.meta_analysis_id}")

    def run_meta_analysis(self):
        self.meta_results = cbma_workflow(
            self.dataset, self.estimator, self.corrector, output_dir=self.result_dir
        )

    def upload_results(self):
        statistical_maps = [
            (
                "statistical_maps",
                open(self.result_dir / (m + ".nii.gz"), "rb"),
            )
            for m in self.meta_results.maps.keys()
            if not m.startswith("label_")
        ]
        cluster_tables = [
            (
                "cluster_tables",
                open(self.result_dir / (f + ".tsv"), "rb"),
            )
            for f, df in self.meta_results.tables.items()
            if "clust" in f and not df.empty
        ]

        diagnostic_tables = [
            (
                "diagnostic_tables",
                open(self.result_dir / (f + ".tsv"), "rb"),
            )
            for f, df in self.meta_results.tables.items()
            if "clust" not in f and df is not None
        ]
        files = statistical_maps + cluster_tables + diagnostic_tables

        headers = {"Compose-Upload-Key": self.nsc_key}
        self.results_object = requests.put(
            f"{self.compose_url}/meta-analysis-results/{self.result_id}",
            files=files,
            json={"method_description": self.meta_results.description_},
            headers=headers,
        )

    def load_specification(self):
        """Returns function to run analysis on dataset."""
        spec = self.cached_specification
        est_mod = import_module(".".join(["nimare", "meta", spec["type"].lower()]))
        estimator = getattr(est_mod, spec["estimator"]["type"])
        if spec["estimator"].get("args"):
            est_args = {**spec["estimator"]["args"]}
            if est_args.get("**kwargs") is not None:
                for k, v in est_args["**kwargs"].items():
                    est_args[k] = v
                del est_args["**kwargs"]
            estimator_init = estimator(**est_args)
        else:
            estimator_init = estimator()

        if spec.get("corrector"):
            cor_mod = import_module(".".join(["nimare", "correct"]))
            corrector = getattr(cor_mod, spec["corrector"]["type"])
            if spec["corrector"].get("args"):
                cor_args = {**spec["corrector"]["args"]}
                if cor_args.get("**kwargs") is not None:
                    for k, v in cor_args["**kwargs"].items():
                        cor_args[k] = v
                    del cor_args["**kwargs"]
                corrector_init = corrector(**cor_args)
            else:
                corrector_init = corrector()
        else:
            corrector_init = None

        return estimator_init, corrector_init


def run(meta_analysis_id, staging=False, result_dir=None, nsc_key=None, nv_key=None):
    runner = Runner(
        meta_analysis_id=meta_analysis_id,
        staging=staging,
        result_dir=result_dir,
        nsc_key=nsc_key,
        nv_key=nv_key,
    )

    runner.run_workflow()
    url = "/".join(
        [runner.compose_url.rstrip("/api"), "meta-analyses", meta_analysis_id]
    )

    return url, runner.meta_results
