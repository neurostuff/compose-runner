from importlib import import_module
from pathlib import Path
import gzip
import json
import io

import requests

# import neurosynth_compose_sdk
# from neurosynth_compose_sdk.api.compose_api import ComposeApi
# import neurostore_sdk
# from neurostore_sdk.api.store_api import StoreApi
from nimare.workflows import CBMAWorkflow, PairwiseCBMAWorkflow
from nimare.meta.cbma.base import CBMAEstimator, PairwiseCBMAEstimator
from nimare.nimads import Studyset, Annotation
from nimare.meta.cbma import ALE, ALESubtraction, SCALE


def gen_database_url(branch, database):
    return f"https://github.com/neurostuff/neurostore_database/raw/{branch}/{database}.json.gz"


class Runner:
    """Runner for executing and uploading a meta-analysis workflow."""

    def __init__(
        self,
        meta_analysis_id,
        environment="production",
        result_dir=None,
        nsc_key=None,
        nv_key=None,
    ):
        # the meta-analysis id associated with this run
        self.meta_analysis_id = meta_analysis_id

        if environment == "staging":
            # staging
            self.compose_url = "https://synth.neurostore.xyz/api"
            self.store_url = "https://neurostore.xyz/api"
            self.reference_studysets = {
                "neurosynth": gen_database_url("staging", "neurosynth"),
                "neuroquery": gen_database_url("staging", "neuroquery"),
                "neurostore": gen_database_url("staging", "neurostore"),
                "neurostore_small": gen_database_url("staging", "neurostore_small"),
            }
        elif environment == "local":
            self.compose_url = "http://localhost:81/api"
            self.store_url = "http://localhost:80/api"
            self.reference_studysets = {
                "neurosynth": gen_database_url("staging", "neurosynth"),
                "neuroquery": gen_database_url("staging", "neuroquery"),
                "neurostore": gen_database_url("staging", "neurostore"),
                "neurostore_small": gen_database_url("staging", "neurostore_small"),
            }
        else:
            # production
            self.compose_url = "https://compose.neurosynth.org/api"
            self.store_url = "https://neurostore.org/api"
            self.reference_studysets = {
                "neurosynth": gen_database_url("main", "neurosynth"),
                "neuroquery": gen_database_url("main", "neuroquery"),
                "neurostore": gen_database_url("main", "neurostore"),
            }

        # Enter a context with an instance of the API client
        # compose_configuration = neurosynth_compose_sdk.Configuration(
        #     host=self.compose_url
        # )
        # store_configuration = neurostore_sdk.Configuration(host=self.store_url)
        # compose_client = neurosynth_compose_sdk.ApiClient(compose_configuration)
        # store_client = neurostore_sdk.ApiClient(store_configuration)
        # self.compose_api = ComposeApi(compose_client)
        # self.store_api = StoreApi(store_client)

        # initialize inputs
        self.cached_studyset = None
        self.cached_annotation = None
        self.cached_specification = None
        self.first_dataset = None
        self.second_dataset = None
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
                (
                    f"{self.store_url}/studysets/"
                    f"{meta_analysis['studyset']['neurostore_id']}?nested=true"
                )
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

    def apply_filter(self, studyset, annotation):
        """
        Apply filter to studyset.
            Options:
                - bool: filter by boolean column
                  can be single or multiple conditions
                - string: filter by string column
                  can be single or multiple conditions
                - database_studyset: use a reference studyset
                  only useful for multiple conditions
        """
        column = self.cached_specification["filter"]
        column_type = self.cached_annotation["note_keys"][f"{column}"]
        conditions = self.cached_specification.get("conditions", [])
        database_studyset = self.cached_specification.get("database_studyset")
        weights = self.cached_specification.get("weights", [])
        weight_conditions = {w: c for c, w in zip(conditions, weights)}

        if not (conditions or weights) and column_type != "boolean":
            raise ValueError(
                f"Column type {column_type} requires a conditions and weights."
            )

        # get analysis ids for the first studyset
        if column_type == "boolean":
            analysis_ids = [
                n.analysis.id for n in annotation.notes if n.note[f"{column}"]
            ]

        elif column_type == "string":
            analysis_ids = [
                n.analysis.id
                for n in annotation.notes
                if n.note[f"{column}"] == weight_conditions[1]
            ]
        else:
            raise ValueError(f"Column type {column_type} not supported.")

        first_studyset = studyset.slice(analyses=analysis_ids)
        first_studyset = first_studyset.combine_analyses()

        # if there is only one condition, return the first studyset
        if len(conditions) <= 1 and not database_studyset:
            return first_studyset, None

        elif len(conditions) == 2 and database_studyset:
            raise ValueError("Cannot have multiple conditions and a database studyset.")

        elif len(conditions) == 2 and not database_studyset:
            second_analysis_ids = [
                n.analysis.id
                for n in annotation.notes
                if n.note[f"{column}"] == weight_conditions[-1]
            ]
            second_studyset = studyset.slice(analyses=second_analysis_ids)
            second_studyset = second_studyset.combine_analyses()

            return first_studyset, second_studyset

        elif len(conditions) <= 1 and database_studyset:
            # Download the gzip file
            response = requests.get(self.reference_studysets[database_studyset])

            # Check if the request was successful
            if response.status_code != 200:
                raise ValueError(
                    f"Could not download reference studyset {database_studyset}."
                )
            # Wrap the content of the response in a BytesIO object
            gzip_content = io.BytesIO(response.content)

            # Decompress the gzip content
            with gzip.GzipFile(fileobj=gzip_content, mode="rb") as gz_file:
                # Read and decode the JSON data
                json_data = gz_file.read().decode("utf-8")

                # Load the JSON data into a dictionary
                reference_studyset_dict = json.loads(json_data)

            reference_studyset = Studyset(reference_studyset_dict)

            # get study ids from studyset
            study_ids = set([s.id for s in studyset.studies])

            # reference study ids
            reference_study_ids = set([s.id for s in reference_studyset.studies])

            keep_study_ids = reference_study_ids - study_ids

            # get analysis ids from reference studyset
            analysis_ids = [
                a.id
                for s in reference_studyset.studies
                for a in s.analyses
                if s.id in keep_study_ids
            ]
            second_studyset = reference_studyset.slice(analyses=analysis_ids)
            second_studyset = second_studyset.combine_analyses()

            return first_studyset, second_studyset

    def process_bundle(self):
        studyset = Studyset(self.cached_studyset)
        annotation = Annotation(self.cached_annotation, studyset)
        first_studyset, second_studyset = self.apply_filter(studyset, annotation)
        first_dataset = first_studyset.to_dataset()
        second_dataset = (
            second_studyset.to_dataset() if second_studyset is not None else None
        )
        estimator, corrector = self.load_specification()
        estimator, corrector = self.validate_specification(
            estimator, corrector, first_dataset, second_dataset
        )
        self.first_dataset = first_dataset
        self.second_dataset = second_dataset
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
        if self.second_dataset and isinstance(self.estimator, PairwiseCBMAEstimator):
            workflow = PairwiseCBMAWorkflow(
                estimator=self.estimator,
                corrector=self.corrector,
                diagnostics="focuscounter",
                output_dir=self.result_dir,
            )
            self.meta_results = workflow.fit(self.first_dataset, self.second_dataset)
        elif self.second_dataset is None and isinstance(self.estimator, CBMAEstimator):
            workflow = CBMAWorkflow(
                estimator=self.estimator,
                corrector=self.corrector,
                diagnostics="focuscounter",
                output_dir=self.result_dir,
            )
            self.meta_results = workflow.fit(self.first_dataset, self.second_dataset)
        else:
            raise ValueError(
                f"Estimator {self.estimator} and datasets {self.first_dataset} and {self.second_dataset} are not compatible."
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

    def validate_specification(
        self, estimator, corrector, dataset, second_dataset=None
    ):
        if (
            isinstance(estimator, (ALE, ALESubtraction, SCALE))
            and estimator.kernel_transformer.sample_size is not None
        ):
            if any(dataset.metadata["sample_sizes"].isnull()):
                raise ValueError(
                    "Sample size is required for ALE with sample size weighting."
                )
        return estimator, corrector


def run(
    meta_analysis_id,
    environment="production",
    result_dir=None,
    nsc_key=None,
    nv_key=None,
):
    runner = Runner(
        meta_analysis_id=meta_analysis_id,
        environment=environment,
        result_dir=result_dir,
        nsc_key=nsc_key,
        nv_key=nv_key,
    )

    runner.run_workflow()
    url = "/".join(
        [runner.compose_url.rstrip("/api"), "meta-analyses", meta_analysis_id]
    )

    return url, runner.meta_results
