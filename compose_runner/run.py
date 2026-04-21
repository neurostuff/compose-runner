import compose_runner.sentry
import gzip
import hashlib
import json
import io
import pickle
from importlib import import_module
from pathlib import Path

import requests

# import neurosynth_compose_sdk
# from neurosynth_compose_sdk.api.compose_api import ComposeApi
# import neurostore_sdk
# from neurostore_sdk.api.store_api import StoreApi
from nimare.correct import FDRCorrector
from nimare.workflows import CBMAWorkflow, PairwiseCBMAWorkflow
from nimare.meta.cbma.base import CBMAEstimator, PairwiseCBMAEstimator
from nimare.nimads import Studyset, Annotation
from nimare.meta.cbma import ALE, ALESubtraction, SCALE


def gen_database_url(branch, database):
    return f"https://github.com/neurostuff/neurostore_database/raw/{branch}/{database}.json.gz"


class Runner:
    """Runner for executing and uploading a meta-analysis workflow."""

    _TARGET_SPACE = "mni152_2mm"

    _ENTITY_SNAPSHOT_ID_KEYS = {
        "studyset": ("snapshot_studyset_id",),
        "annotation": ("snapshot_annotation_id",),
    }
    _ENTITY_STORE_PATHS = {
        "studyset": "studysets",
        "annotation": "annotations",
    }
    _ENTITY_SNAPSHOT_PATHS = {
        "studyset": "snapshot-studysets",
        "annotation": "snapshot-annotations",
    }
    _ENTITY_NEUROSTORE_KEYS = {
        "studyset": ("neurostore_studyset", "neurostore_studyset_id"),
        "annotation": (
            "neurostore_annotation",
            "neurostore_annotation_id",
        ),
    }
    _ENTITY_SNAPSHOT_SUMMARY_KEYS = {
        "studyset": ("neurostore_studyset", "studysets"),
        "annotation": ("neurostore_annotation", "annotations"),
    }
    _ENTITY_COMPOSE_PATHS = {
        "studyset": "neurostore-studysets",
        "annotation": "neurostore-annotations",
    }
    _ENTITY_COMPOSE_CHILD_KEYS = {
        "studyset": "studysets",
        "annotation": "annotations",
    }

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
        if environment == "development":
            self.compose_url = "https://dev.synth.neurostore.xyz/api"
            self.store_url = "https://dev.neurostore.xyz/api"
            self.reference_studysets = {
                "neurosynth": gen_database_url("staging", "neurosynth"),
                "neuroquery": gen_database_url("staging", "neuroquery"),
                "neurostore": gen_database_url("staging", "neurostore"),
                "neurostore_small": gen_database_url("staging", "neurostore_small"),
            }
        elif environment == "staging":
            # staging
            self.compose_url = "https://staging.synth.neurostore.xyz/api"
            self.store_url = "https://staging.neurostore.xyz/api"
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
        self.existing_studyset_snapshot = None
        self.existing_annotation_snapshot = None
        self.existing_studyset_snapshot_id = None
        self.existing_annotation_snapshot_id = None
        self.first_studyset = None
        self.second_studyset = None
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

    def run_workflow(self, no_upload=False, n_cores=None):
        self.download_bundle()
        self.process_bundle(n_cores=n_cores)
        self.run_meta_analysis()
        if not no_upload:
            self.create_result_object()
            self.upload_results()

    def _get_json(self, url, error_message):
        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(error_message) from e
        return response.json()

    @staticmethod
    def _unwrap_snapshot(payload):
        current = payload
        while isinstance(current, dict):
            snapshot = current.get("snapshot")
            if not isinstance(snapshot, dict):
                snapshot = current.get("cached")
            if not isinstance(snapshot, dict) or snapshot is current:
                break
            current = snapshot
        return current if isinstance(current, dict) else None

    @staticmethod
    def _extract_document_id(payload):
        if isinstance(payload, str):
            return payload
        if isinstance(payload, dict):
            payload_id = payload.get("id")
            if isinstance(payload_id, str):
                return payload_id
        return None

    @staticmethod
    def _is_studyset_snapshot(payload):
        return isinstance(payload, dict) and isinstance(payload.get("studies"), list)

    @staticmethod
    def _is_annotation_snapshot(payload):
        return isinstance(payload, dict) and isinstance(payload.get("notes"), list)

    def _get_result_documents(self, meta_analysis):
        result_documents = []
        seen_ids = set()
        result_refs = list(meta_analysis.get("snapshots") or [])
        result_refs.extend(meta_analysis.get("results") or [])

        for result_ref in reversed(result_refs):
            if isinstance(result_ref, str):
                result_id = result_ref
                result_doc = None
            elif isinstance(result_ref, dict):
                result_id = result_ref.get("id") or result_ref.get("result_id")
                result_doc = result_ref
            else:
                continue

            if result_id in seen_ids:
                continue
            if result_id is not None:
                seen_ids.add(result_id)
            if result_doc is None:
                if result_id is None:
                    continue
                result_doc = self._get_json(
                    f"{self.compose_url}/meta-analysis-results/{result_id}",
                    f"Could not download meta-analysis result {result_id}",
                )

            result_documents.append(result_doc)

        return result_documents

    def _get_project_document(self, meta_analysis):
        project = meta_analysis.get("project")
        if isinstance(project, dict):
            return project
        if isinstance(project, str):
            return self._get_json(
                f"{self.compose_url}/projects/{project}",
                f"Could not download project {project}",
            )
        return None

    def _get_entity_snapshot_record(self, entity_name, documents):
        is_expected_snapshot = (
            self._is_studyset_snapshot
            if entity_name == "studyset"
            else self._is_annotation_snapshot
        )
        for document in documents:
            if not isinstance(document, dict):
                continue
            snapshot_id = None
            for key in self._ENTITY_SNAPSHOT_ID_KEYS[entity_name]:
                snapshot_id = self._extract_document_id(document.get(key))
                if snapshot_id is None:
                    continue
                break
            if snapshot_id is None:
                ref_key, summary_key = self._ENTITY_SNAPSHOT_SUMMARY_KEYS[entity_name]
                ref_document = document.get(ref_key)
                if isinstance(ref_document, dict):
                    summary_documents = ref_document.get(summary_key) or []
                    for summary_document in summary_documents:
                        snapshot_id = self._extract_document_id(summary_document)
                        if snapshot_id is not None:
                            break
            if snapshot_id is None:
                continue
            try:
                snapshot_document = self._get_json(
                    f"{self.compose_url}/{self._ENTITY_SNAPSHOT_PATHS[entity_name]}/{snapshot_id}",
                    f"Could not download {entity_name} snapshot {snapshot_id}",
                )
            except requests.exceptions.HTTPError:
                continue
            payload = self._unwrap_snapshot(snapshot_document)
            if is_expected_snapshot(payload):
                return payload, snapshot_id
        return None, None

    @staticmethod
    def _extract_neurostore_id(payload):
        if isinstance(payload, str):
            return payload
        if isinstance(payload, dict):
            neurostore_id = payload.get("neurostore_id")
            if isinstance(neurostore_id, str):
                return neurostore_id
            payload_id = payload.get("id")
            if isinstance(payload_id, str):
                return payload_id
        return None

    def _get_neurostore_id(self, entity_name, documents):
        for document in documents:
            if not isinstance(document, dict):
                continue
            for key in self._ENTITY_NEUROSTORE_KEYS[entity_name]:
                neurostore_id = self._extract_neurostore_id(document.get(key))
                if neurostore_id is not None:
                    return neurostore_id
        return None

    def _get_compose_neurostore_document(self, entity_name, documents):
        compose_document = None
        for document in documents:
            if not isinstance(document, dict):
                continue
            for key in self._ENTITY_NEUROSTORE_KEYS[entity_name]:
                payload = document.get(key)
                if isinstance(payload, dict):
                    compose_document = payload
                    break
                compose_id = self._extract_neurostore_id(payload)
                if compose_id is not None:
                    compose_document = self._get_json(
                        f"{self.compose_url}/{self._ENTITY_COMPOSE_PATHS[entity_name]}/{compose_id}",
                        f"Could not download {entity_name} compose link {compose_id}",
                    )
                    break
            if compose_document is not None:
                break
        return compose_document

    def _get_compose_child_neurostore_id(self, entity_name, documents):
        compose_document = self._get_compose_neurostore_document(entity_name, documents)
        if not isinstance(compose_document, dict):
            return None
        child_key = self._ENTITY_COMPOSE_CHILD_KEYS[entity_name]
        child_documents = compose_document.get(child_key) or []
        for child_document in child_documents:
            child_id = self._extract_neurostore_id(child_document)
            if child_id is not None:
                return child_id
        return None

    def _download_entity_from_store(self, entity_name, entity_id, documents):
        try:
            return self._get_json(
                f"{self.store_url}/{self._ENTITY_STORE_PATHS[entity_name]}/{entity_id}"
                f"{'?nested=true' if entity_name == 'studyset' else ''}",
                f"Could not download {entity_name} {entity_id}",
            )
        except requests.exceptions.HTTPError as direct_error:
            linked_entity_id = self._get_compose_child_neurostore_id(entity_name, documents)
            if linked_entity_id is None or linked_entity_id == entity_id:
                raise
            try:
                return self._get_json(
                    f"{self.store_url}/{self._ENTITY_STORE_PATHS[entity_name]}/{linked_entity_id}"
                    f"{'?nested=true' if entity_name == 'studyset' else ''}",
                    f"Could not download {entity_name} {linked_entity_id}",
                )
            except requests.exceptions.HTTPError:
                raise direct_error

    def _collect_entity_records(self, documents):
        records = {}
        for entity_name in self._ENTITY_STORE_PATHS:
            snapshot, snapshot_id = self._get_entity_snapshot_record(entity_name, documents)
            records[entity_name] = {
                "snapshot": snapshot,
                "snapshot_id": snapshot_id,
                "neurostore_id": self._get_neurostore_id(entity_name, documents),
            }
        return records

    def _apply_entity_records(self, records):
        self.existing_studyset_snapshot = records["studyset"]["snapshot"]
        self.existing_studyset_snapshot_id = records["studyset"]["snapshot_id"]
        self.existing_annotation_snapshot = records["annotation"]["snapshot"]
        self.existing_annotation_snapshot_id = records["annotation"]["snapshot_id"]

    @staticmethod
    def _snapshot_md5(payload):
        serialized_payload = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.md5(serialized_payload.encode("utf-8")).hexdigest()

    def _should_link_existing_snapshot(self, live_payload, existing_payload, existing_id):
        if existing_id is None or existing_payload is None:
            return False
        return self._snapshot_md5(live_payload) == self._snapshot_md5(existing_payload)

    def download_bundle(self):
        meta_analysis = self._get_json(
            f"{self.compose_url}/meta-analyses/{self.meta_analysis_id}?nested=true",
            f"Could not download meta-analysis {self.meta_analysis_id}",
        )
        # meta_analysis = self.compose_api.meta_analyses_id_get(
        #     id=self.meta_analysis_id, nested=True
        # ).to_dict()  # does not currently return run_key

        documents = [meta_analysis]
        entity_records = self._collect_entity_records(documents)
        self._apply_entity_records(entity_records)
        neurostore_documents = list(documents)
        should_fetch_result_documents = any(
            record["snapshot"] is None or record["neurostore_id"] is None
            for record in entity_records.values()
        )
        if should_fetch_result_documents:
            result_documents = self._get_result_documents(meta_analysis)
            if result_documents:
                documents.extend(result_documents)
                neurostore_documents = list(documents)
                entity_records = self._collect_entity_records(documents)
                self._apply_entity_records(entity_records)

        if any(record["neurostore_id"] is None for record in entity_records.values()):
            project_document = self._get_project_document(meta_analysis)
            neurostore_documents.append(project_document)
            entity_records = self._collect_entity_records(neurostore_documents)
            self._apply_entity_records(entity_records)

        if all(record["neurostore_id"] is not None for record in entity_records.values()):
            try:
                self.cached_studyset = self._download_entity_from_store(
                    "studyset",
                    entity_records["studyset"]["neurostore_id"],
                    neurostore_documents,
                )
                self.cached_annotation = self._download_entity_from_store(
                    "annotation",
                    entity_records["annotation"]["neurostore_id"],
                    neurostore_documents,
                )
                self.cached = False
            except requests.exceptions.RequestException:
                if (
                    self.existing_studyset_snapshot is None
                    or self.existing_annotation_snapshot is None
                ):
                    raise
                self.cached_studyset = self.existing_studyset_snapshot
                self.cached_annotation = self.existing_annotation_snapshot
                self.cached = True
        elif (
            self.existing_studyset_snapshot is not None
            and self.existing_annotation_snapshot is not None
        ):
            self.cached_studyset = self.existing_studyset_snapshot
            self.cached_annotation = self.existing_annotation_snapshot
            self.cached = True
        else:
            raise ValueError(
                "Could not resolve studyset and annotation sources for "
                f"{self.meta_analysis_id}"
            )
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

        # since we added "order" to annotations
        if isinstance(column_type, dict):
            column_type = column_type.get("type")

        if not (conditions or weights) and column_type != "boolean":
            raise ValueError(
                f"Column type {column_type} requires a conditions and weights."
            )

        # get analysis ids for the first studyset
        if column_type == "boolean":
            analysis_ids = [
                n.analysis.id for n in annotation.notes if n.note.get(f"{column}")
            ]

        elif column_type == "string":
            analysis_ids = [
                n.analysis.id
                for n in annotation.notes
                if n.note.get(f"{column}", "") == weight_conditions[1]
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
            if column_type == "boolean":
                second_analysis_ids = [
                    n.analysis.id
                    for n in annotation.notes
                    if not n.note.get(f"{column}")
                ]
            else:
                second_analysis_ids = [
                    n.analysis.id
                    for n in annotation.notes
                    if n.note.get(f"{column}") == weight_conditions[-1]
                ]
            second_studyset = studyset.slice(analyses=second_analysis_ids)
            second_studyset = second_studyset.combine_analyses()

            return first_studyset, second_studyset

        elif len(conditions) <= 1 and database_studyset:
            # collect user study IDs cheaply before loading the large reference database
            study_ids = set(studyset.study_ids)

            # Download the gzip file
            response = requests.get(self.reference_studysets[database_studyset])

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise requests.exceptions.HTTPError(
                    f"Could not download reference studyset {database_studyset}."
                ) from e

            # Wrap the content of the response in a BytesIO object
            gzip_content = io.BytesIO(response.content)

            # Decompress the gzip content
            with gzip.GzipFile(fileobj=gzip_content, mode="rb") as gz_file:
                # Read and decode the JSON data
                json_data = gz_file.read().decode("utf-8")

                # Load the JSON data into a dictionary
                reference_studyset_dict = json.loads(json_data)

            # pre-filter at the dict level to exclude user studies before constructing
            # Studyset, keeping the object small and avoiding expensive materialize calls
            reference_studyset_dict["studies"] = [
                s for s in reference_studyset_dict.get("studies", [])
                if s["id"] not in study_ids
            ]

            reference_studyset = Studyset(reference_studyset_dict, target=self._TARGET_SPACE)
            del reference_studyset_dict

            second_studyset = reference_studyset.combine_analyses()

            return first_studyset, second_studyset

    def process_bundle(self, n_cores=None):
        studyset = Studyset(self.cached_studyset, target=self._TARGET_SPACE)
        annotation = Annotation(self.cached_annotation, studyset)
        first_studyset, second_studyset = self.apply_filter(studyset, annotation)
        estimator, corrector = self.load_specification(n_cores=n_cores)
        self.first_studyset = first_studyset
        self.second_studyset = second_studyset
        self.estimator = estimator
        self.corrector = corrector

    def create_result_object(self):
        headers = {"Compose-Upload-Key": self.nsc_key}
        data = {"meta_analysis_id": self.meta_analysis_id}
        entity_payloads = {
            "studyset": (
                self.cached_studyset,
                self.existing_studyset_snapshot,
                self.existing_studyset_snapshot_id,
            ),
            "annotation": (
                self.cached_annotation,
                self.existing_annotation_snapshot,
                self.existing_annotation_snapshot_id,
            ),
        }
        for entity_name, (live_payload, existing_payload, existing_id) in entity_payloads.items():
            if self._should_link_existing_snapshot(
                live_payload,
                existing_payload,
                existing_id,
            ):
                data[f"snapshot_{entity_name}_id"] = existing_id
            else:
                data[f"snapshot_{entity_name}"] = live_payload

        resp = requests.post(
            f"{self.compose_url}/meta-analysis-results",
            json=data,
            headers=headers,
        )
        resp.raise_for_status()
        self.result_id = resp.json().get("id", None)
        if self.result_id is None:
            raise ValueError(f"Could not create result for {self.meta_analysis_id}")

    def run_meta_analysis(self):
        if self.second_studyset and isinstance(self.estimator, PairwiseCBMAEstimator):
            workflow = PairwiseCBMAWorkflow(
                estimator=self.estimator,
                corrector=self.corrector,
                diagnostics="focuscounter",
                output_dir=self.result_dir,
            )
            self.meta_results = workflow.fit(
                self.first_studyset,
                self.second_studyset,
            )
        elif self.second_studyset is None and isinstance(self.estimator, CBMAEstimator):
            workflow = CBMAWorkflow(
                estimator=self.estimator,
                corrector=self.corrector,
                diagnostics="focuscounter",
                output_dir=self.result_dir,
            )
            self.meta_results = workflow.fit(self.first_studyset)
        else:
            raise ValueError(
                "Estimator "
                f"{self.estimator} and studysets {self.first_studyset} and "
                f"{self.second_studyset} are not compatible."
            )
        self._persist_meta_results()

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
            if f.endswith("clust") and not df.empty
        ]

        diagnostic_tables = [
            (
                "diagnostic_tables",
                open(self.result_dir / (f + ".tsv"), "rb"),
            )
            for f, df in self.meta_results.tables.items()
            if not f.endswith("clust") and df is not None
        ]
        files = statistical_maps + cluster_tables + diagnostic_tables

        headers = {"Compose-Upload-Key": self.nsc_key}
        self.results_object = requests.put(
            f"{self.compose_url}/meta-analysis-results/{self.result_id}",
            files=files,
            json={"method_description": self.meta_results.description_},
            headers=headers,
        )

    def load_specification(self, n_cores=None):
        """Returns function to run analysis on dataset."""
        spec = self.cached_specification
        est_mod = import_module(".".join(["nimare", "meta", spec["type"].lower()]))
        estimator = getattr(est_mod, spec["estimator"]["type"])
        est_args = {**spec["estimator"]["args"]} if spec["estimator"].get("args") else {}
        if n_cores is not None:
            est_args["n_cores"] = n_cores
        if est_args.get("n_iters") is not None:
            est_args["n_iters"] = int(est_args["n_iters"])
        if est_args.get("**kwargs") is not None:
            for k, v in est_args["**kwargs"].items():
                est_args[k] = v
            del est_args["**kwargs"]
        estimator_init = estimator(**est_args)

        if spec.get("corrector"):
            cor_mod = import_module(".".join(["nimare", "correct"]))
            corrector = getattr(cor_mod, spec["corrector"]["type"])
            cor_args = {**spec["corrector"]["args"]} if spec["corrector"].get("args") else {}
            if n_cores is not None and corrector is not FDRCorrector:
                cor_args["n_cores"] = n_cores
            if cor_args.get("n_iters") is not None and corrector is not FDRCorrector:
                cor_args["n_iters"] = int(cor_args["n_iters"])
            if cor_args.get("**kwargs") is not None:
                for k, v in cor_args["**kwargs"].items():
                    cor_args[k] = v
                del cor_args["**kwargs"]
            corrector_init = corrector(**cor_args)
        else:
            corrector_init = None

        return estimator_init, corrector_init


    def _persist_meta_results(self):
        """Persist meta-analysis results locally for downstream access."""
        if self.meta_results is None:
            return
        self.result_dir.mkdir(parents=True, exist_ok=True)
        meta_results_path = self.result_dir / "meta_results.pkl"
        with meta_results_path.open("wb") as meta_file:
            pickle.dump(self.meta_results, meta_file, protocol=pickle.HIGHEST_PROTOCOL)


def run(
    meta_analysis_id,
    environment="production",
    result_dir=None,
    nsc_key=None,
    nv_key=None,
    no_upload=False,
    n_cores=None,
):
    runner = Runner(
        meta_analysis_id=meta_analysis_id,
        environment=environment,
        result_dir=result_dir,
        nsc_key=nsc_key,
        nv_key=nv_key,
    )

    runner.run_workflow(no_upload=no_upload, n_cores=n_cores)

    if no_upload:
        return None, runner.meta_results

    url = "/".join(
        [runner.compose_url.rstrip("/api"), "meta-analyses", meta_analysis_id]
    )

    return url, runner.meta_results
