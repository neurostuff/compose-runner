import compose_runner.sentry
import gzip
import logging
import hashlib
import json
import io
import pickle
import re
import tarfile
import tempfile
from copy import deepcopy
from datetime import date, datetime
from importlib import import_module
from inspect import signature
from pathlib import Path
from uuid import UUID

import numpy as np
import pandas as pd
import requests
import neurosynth_compose_sdk
import neurostore_sdk
from neurosynth_compose_sdk.api.compose_api import ComposeApi
from neurostore_sdk.api.store_api import StoreApi
from neurosynth_compose_sdk.exceptions import ApiException as ComposeApiException
from neurostore_sdk.exceptions import ApiException as StoreApiException
from neurosynth_compose_sdk.models import ResultInit

from nimare.correct import FDRCorrector
from nimare.workflows import CBMAWorkflow, IBMAWorkflow, PairwiseCBMAWorkflow
from nimare.meta.cbma.base import CBMAEstimator, PairwiseCBMAEstimator
from nimare.meta.ibma import IBMAEstimator
from nimare.nimads import Studyset, from_parquet
from nimare.meta.cbma import ALE, ALESubtraction, SCALE

from compose_runner import coverage
from compose_runner.images import download_studyset_images
from compose_runner.metadata import apply_sample_sizes

LGR = logging.getLogger(__name__)


def gen_database_url(branch, database):
    return f"https://github.com/neurostuff/neurostore_database/raw/{branch}/{database}.json.gz"


def gen_release_url(store_host, version):
    """URL for one of neurostore's parquet studyset releases."""
    host = store_host.rstrip("/")
    return f"{host}/neurostore-studyset-releases/{version}/download"


def _extract_studyset_release(archive, destination):
    """Unpack a studyset release and return the directory holding its tables.

    The archive carries a single directory named for the release, so the
    ``studyset.json`` manifest sits one level below the root rather than at it.
    """
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(destination, filter="data")

    manifests = sorted(destination.glob("**/studyset.json"))
    if not manifests:
        raise ValueError(
            f"{archive.name} is not a studyset parquet release: it has no "
            "studyset.json manifest."
        )
    return manifests[0].parent


def _annotation_column(studyset, column):
    """Read one annotation column off a studyset.

    A studyset carries its annotations itself, as ``annotations_df``: one row per
    analysis, keyed by the full ``study-analysis`` id, with a column per note
    key. That id is what ``slice`` wants, and it is unique, which a bare analysis
    id is not -- NIMADS does not require analysis ids to be distinct across
    studies.

    Returns
    -------
    ids : :obj:`pandas.Series`
        Full analysis ids, one per analysis in the studyset.
    values : :obj:`pandas.Series`
        The column's values, aligned to ``ids``. Null wherever no note recorded
        one, including when the column is absent altogether. The frame has a row
        per analysis, so it cannot distinguish an analysis with no note from one
        whose note is null -- a distinction compose's annotations do not draw,
        since they carry a note for every analysis in their studyset.
    """
    frame = studyset.annotations_df
    ids = frame["id"].astype(str)
    if column not in frame.columns:
        return ids, pd.Series(None, index=frame.index, dtype=object)
    return ids, frame[column]


def _truthy(values):
    """Which of an annotation column's values Python would call true.

    A note column arrives as float 1.0/0.0 where its values are numbers --
    booleans included, since consumers do arithmetic on annotation weights --
    and as objects otherwise, so the test has to survive both. Nulls are filled
    first: a missing note is false, but ``NaN`` on its own is true.
    """
    return values.fillna(False).astype(bool)


_ENVIRONMENT_URLS = {
    "development": (
        "https://dev.synth.neurostore.xyz/api",
        "https://dev.neurostore.xyz/api",
    ),
    "staging": (
        "https://staging.synth.neurostore.xyz/api",
        "https://staging.neurostore.xyz/api",
    ),
    "local": ("http://localhost:81/api", "http://localhost:80/api"),
    "production": ("https://compose.neurosynth.org/api", "https://neurostore.org/api"),
}


# PyMARE names the dependence group whose members cancel, but by the integer
# code NiMARE assigned it, which means nothing to whoever chose the studies.
_CANCELLED_GROUP = re.compile(r"Group (\S+) pools \d+ estimates")


class Runner:
    """Runner for executing and uploading a meta-analysis workflow."""

    _TARGET_SPACE = "mni152_2mm"

    # Which reference studysets come from neurostore's studyset release API, as
    # a tarball of parquet tables, rather than from a static NIMADS dump. Only
    # neurostore's own: a release carries no record of which database a study
    # came from, so a neurosynth or neuroquery subset cannot be cut out of one.
    _RELEASE_REFERENCES = frozenset({"neurostore"})
    _REFERENCE_RELEASE_VERSION = "nightly"

    _ENTITY_SNAPSHOT_ID_KEYS = {
        "studyset": ("snapshot_studyset_id",),
        "annotation": ("snapshot_annotation_id",),
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
        self.meta_analysis_id = meta_analysis_id

        env = environment if environment in _ENVIRONMENT_URLS else "production"
        compose_host, store_host = _ENVIRONMENT_URLS[env]
        self.compose_url = compose_host

        ref_branch = "main" if environment == "production" else "staging"
        ref_dbs = ["neurosynth", "neuroquery"]
        if environment != "production":
            ref_dbs.append("neurostore_small")
        self.reference_studysets = {
            db: gen_database_url(ref_branch, db) for db in ref_dbs
        }
        # Served by this environment's own API, so the comparison group is the
        # database the studyset was selected from as it stands tonight.
        self.reference_studysets["neurostore"] = gen_release_url(
            store_host, self._REFERENCE_RELEASE_VERSION
        )

        self._compose_config = neurosynth_compose_sdk.Configuration(host=compose_host)
        self.compose_api = ComposeApi(
            neurosynth_compose_sdk.ApiClient(self._compose_config)
        )
        self.store_api = StoreApi(
            neurostore_sdk.ApiClient(neurostore_sdk.Configuration(host=store_host))
        )

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
        self.n_cores = None
        self.staged_studyset = None
        # Which maps never made it into an image-based meta-analysis, and why.
        self.dropped_maps = {}
        self.coverage_report = None

        # initialize api-keys
        self.nsc_key = nsc_key  # neurosynth compose key to upload to neurosynth compose
        self.nv_key = nv_key  # neurovault key to upload to neurovault

        # result directory
        # Resolved: the studyset records where its downloaded maps are, and
        # NiMARE treats a relative reference as relative to the studyset's base
        # path -- so a relative result_dir has the base path prepended to a path
        # that already contains it, and every map goes missing.
        if result_dir is None:
            self.result_dir = Path.cwd() / "results"
        else:
            self.result_dir = Path(result_dir).expanduser().resolve()

        # Where image-based meta-analyses stage their maps. Kept alongside the
        # results so it doubles as a cache across reruns, and so NiMARE has
        # somewhere writable for any maps it derives.
        self.image_dir = self.result_dir / "images"

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
                result_doc = self.compose_api.meta_analysis_results_id_get(
                    id=result_id
                ).to_dict()

            result_documents.append(result_doc)

        return result_documents

    def _get_project_document(self, meta_analysis):
        project = meta_analysis.get("project")
        if isinstance(project, dict):
            return project
        if isinstance(project, str):
            return self.compose_api.projects_id_get(id=project).to_dict()
        return None

    def _get_entity_snapshot_record(self, entity_name, documents):
        is_expected_snapshot = (
            self._is_studyset_snapshot
            if entity_name == "studyset"
            else self._is_annotation_snapshot
        )
        ref_key = self._ENTITY_SNAPSHOT_SUMMARY_KEYS[entity_name][0]
        summary_key = self._ENTITY_SNAPSHOT_SUMMARY_KEYS[entity_name][1]
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
                # Old API format: list of {id, md5} snapshot summaries
                ref_document = document.get(ref_key)
                if isinstance(ref_document, dict):
                    for summary_document in ref_document.get(summary_key) or []:
                        snapshot_id = self._extract_document_id(summary_document)
                        if snapshot_id is not None:
                            break

            if snapshot_id is not None:
                try:
                    if entity_name == "studyset":
                        snapshot_document = self.compose_api.snapshot_studysets_id_get(
                            id=snapshot_id
                        ).to_dict()
                    else:
                        snapshot_document = (
                            self.compose_api.snapshot_annotations_id_get(
                                id=snapshot_id
                            ).to_dict()
                        )
                except ComposeApiException:
                    continue
                payload = self._unwrap_snapshot(snapshot_document)
                if is_expected_snapshot(payload):
                    return payload, snapshot_id
            else:
                # New API format (SDK 1.1+): snapshot embedded directly in the reference doc
                ref_document = document.get(ref_key)
                if isinstance(ref_document, dict):
                    payload = ref_document.get("snapshot")
                    if isinstance(payload, dict) and is_expected_snapshot(payload):
                        return payload, None
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
        for document in documents:
            if not isinstance(document, dict):
                continue
            for key in self._ENTITY_NEUROSTORE_KEYS[entity_name]:
                payload = document.get(key)
                if isinstance(payload, dict):
                    return payload
                compose_id = self._extract_neurostore_id(payload)
                if compose_id is not None:
                    if entity_name == "studyset":
                        return self.compose_api.neurostore_studysets_id_get(
                            id=compose_id
                        ).to_dict()
                    else:
                        return self.compose_api.neurostore_annotations_id_get(
                            id=compose_id
                        ).to_dict()
        return None

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
            if entity_name == "studyset":
                return self.store_api.studysets_id_get(
                    id=entity_id, nested=True
                ).to_dict()
            else:
                return self.store_api.annotations_id_get(id=entity_id).to_dict()
        except StoreApiException as direct_error:
            linked_entity_id = self._get_compose_child_neurostore_id(
                entity_name, documents
            )
            if linked_entity_id is None or linked_entity_id == entity_id:
                raise
            try:
                if entity_name == "studyset":
                    return self.store_api.studysets_id_get(
                        id=linked_entity_id, nested=True
                    ).to_dict()
                else:
                    return self.store_api.annotations_id_get(
                        id=linked_entity_id
                    ).to_dict()
            except StoreApiException:
                raise direct_error

    def _collect_entity_records(self, documents):
        records = {}
        for entity_name in self._ENTITY_NEUROSTORE_KEYS:
            snapshot, snapshot_id = self._get_entity_snapshot_record(
                entity_name, documents
            )
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
    def _json_payload_default(value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, set):
            return sorted(value, key=str)
        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict):
            return to_dict()
        raise TypeError(
            f"Object of type {value.__class__.__name__} is not JSON serializable"
        )

    @classmethod
    def _snapshot_json(cls, payload):
        return json.dumps(
            payload,
            default=cls._json_payload_default,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _json_safe_payload(cls, payload):
        return json.loads(cls._snapshot_json(payload))

    @classmethod
    def _snapshot_md5(cls, payload):
        serialized_payload = cls._snapshot_json(payload)
        return hashlib.md5(serialized_payload.encode("utf-8")).hexdigest()

    def _should_link_existing_snapshot(
        self, live_payload, existing_payload, existing_id
    ):
        if existing_id is None or existing_payload is None:
            return False
        return self._snapshot_md5(live_payload) == self._snapshot_md5(existing_payload)

    def download_bundle(self):
        meta_analysis = self.compose_api.meta_analyses_id_get(
            id=self.meta_analysis_id, nested=True
        ).to_dict()

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

        if all(
            record["neurostore_id"] is not None for record in entity_records.values()
        ):
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
            except (ComposeApiException, StoreApiException):
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
        self.nsc_key = meta_analysis.get("run_key")

    def apply_filter(self, studyset, combine=True):
        """
        Apply filter to studyset.
            Options:
                - bool: filter by boolean column
                  can be single or multiple conditions
                - string: filter by string column
                  can be single or multiple conditions
                - database_studyset: use a reference studyset
                  only useful for multiple conditions

        Set ``combine=False`` to keep each analysis separate, as IBMA needs:
        merging concatenates a study's images into one analysis, and converting
        to a Dataset keeps only one map per type, so the extra contrasts are
        lost along with the study grouping the dependence correction needs.
        """
        column = self.cached_specification["filter"]
        note_keys = (self.cached_annotation or {}).get("note_keys") or {}
        if column not in note_keys:
            # Otherwise a bare KeyError, which names the column but nothing else.
            raise ValueError(
                f"The specification selects analyses by {column!r}, which this "
                f"annotation does not record. It has: {sorted(note_keys)}."
            )
        column_type = note_keys[column]
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
        analysis_id, note_value = _annotation_column(studyset, column)
        if column_type == "boolean":
            included = _truthy(note_value)
            analysis_ids = list(analysis_id[included])

        elif column_type == "string":
            analysis_ids = list(analysis_id[note_value == weight_conditions[1]])
        else:
            raise ValueError(f"Column type {column_type} not supported.")

        if not analysis_ids:
            # NiMARE would report this as a missing image type, which sends
            # whoever reads it looking at the maps rather than at the selection.
            raise ValueError(
                f"No analysis is selected: none of the {len(analysis_id)} "
                f"analyses in the studyset has a {column!r} note that "
                + (
                    "is true."
                    if column_type == "boolean"
                    else f"equals {weight_conditions[1]!r}."
                )
            )

        first_studyset = studyset.slice(analyses=analysis_ids)
        if combine:
            first_studyset = first_studyset.combine_analyses()

        # if there is only one condition, return the first studyset
        if len(conditions) <= 1 and not database_studyset:
            return first_studyset, None

        elif len(conditions) == 2 and database_studyset:
            raise ValueError("Cannot have multiple conditions and a database studyset.")

        elif len(conditions) == 2 and not database_studyset:
            if column_type == "boolean":
                second_analysis_ids = list(analysis_id[~included])
            else:
                second_analysis_ids = list(
                    analysis_id[note_value == weight_conditions[-1]]
                )
            second_studyset = studyset.slice(analyses=second_analysis_ids)
            if combine:
                second_studyset = second_studyset.combine_analyses()

            return first_studyset, second_studyset

        elif len(conditions) <= 1 and database_studyset:
            # collect user study IDs cheaply before loading the large reference database
            study_ids = set(studyset.study_ids)
            reference_studyset = self._load_reference_studyset(
                database_studyset, study_ids
            )

            second_studyset = (
                reference_studyset.combine_analyses() if combine else reference_studyset
            )

            return first_studyset, second_studyset

    def _load_reference_studyset(self, database_studyset, exclude_study_ids):
        """The comparison group for a database meta-analysis.

        The user's own studies are excluded from it: they are already the other
        group, and an analysis cannot be in both.
        """
        if database_studyset in self._RELEASE_REFERENCES:
            return self._load_release_reference(database_studyset, exclude_study_ids)
        return self._load_dump_reference(database_studyset, exclude_study_ids)

    def _get_reference_studyset(self, database_studyset, **kwargs):
        """Request a reference studyset, naming it in the error if it is not served."""
        response = requests.get(self.reference_studysets[database_studyset], **kwargs)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            response.close()
            raise requests.exceptions.HTTPError(
                f"Could not download reference studyset {database_studyset}."
            ) from e

        return response

    def _load_release_reference(self, database_studyset, exclude_study_ids):
        """Read a reference studyset from a parquet studyset release.

        Streamed to disk rather than held in memory: the archive is tens of
        megabytes and has to be unpacked to a directory for NiMARE to read it.
        """
        with tempfile.TemporaryDirectory() as workdir:
            archive = Path(workdir) / "release.tar.gz"
            with self._get_reference_studyset(
                database_studyset, stream=True
            ) as response, archive.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    handle.write(chunk)

            release_dir = _extract_studyset_release(archive, Path(workdir) / "release")
            # The reference is only ever the comparison group, so nothing reads
            # its notes: its annotation columns are pure cost here.
            store = from_parquet(str(release_dir), load_annotations=False)

        # The tables are read eagerly, so the studyset outlives the directory.
        reference_studyset = Studyset(store, target=self._TARGET_SPACE)
        # As a list: the ids are matched with ``numpy.isin``, which reads a set
        # as one opaque object and so quietly excludes nothing.
        return reference_studyset.exclude_study_ids(list(exclude_study_ids))

    def _load_dump_reference(self, database_studyset, exclude_study_ids):
        """Read a reference studyset from a gzipped NIMADS dump."""
        response = self._get_reference_studyset(database_studyset)

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
            s
            for s in reference_studyset_dict.get("studies", [])
            if s["id"] not in exclude_study_ids
        ]

        reference_studyset = Studyset(
            reference_studyset_dict, target=self._TARGET_SPACE
        )
        del reference_studyset_dict

        return reference_studyset

    def _is_image_based(self):
        """Whether the specification asks for an image-based meta-analysis."""
        spec_type = str((self.cached_specification or {}).get("type") or "").strip()
        return spec_type.lower() == "ibma"

    def prepare_images(self):
        """Return a studyset whose maps are on disk, with sample sizes filled in.

        NiMARE hands image paths to nibabel, which cannot read over HTTP, so the
        maps have to be local before the studyset becomes a Dataset. Staged on a
        copy, because ``cached_studyset`` is uploaded as the result's studyset
        snapshot and must keep the locations and maps Neurostore served.
        """
        self.image_dir.mkdir(parents=True, exist_ok=True)
        staged, self.dropped_maps = download_studyset_images(
            deepcopy(self.cached_studyset), self.image_dir
        )
        self.staged_studyset = apply_sample_sizes(staged, self.cached_annotation)
        return self.staged_studyset

    def _reject_image_based_comparison(self):
        """Refuse an image-based group comparison while it is still cheap.

        ``run_meta_analysis`` catches this too, but only after the maps are
        staged and, for a database comparison, after the whole reference
        studyset has been downloaded -- tens of megabytes to reach a conclusion
        the specification already carried.
        """
        spec = self.cached_specification or {}
        if len(spec.get("conditions") or []) <= 1 and not spec.get("database_studyset"):
            return
        raise ValueError(
            "A group comparison was requested, but no image-based estimator "
            f"supports one. {spec['estimator']['type']} takes a single studyset. "
            "Choose a single-group selection, or a pairwise coordinate-based "
            "estimator."
        )

    def process_bundle(self, n_cores=None):
        self.n_cores = n_cores
        image_based = self._is_image_based()

        # Before the images are fetched, so a specification NiMARE rejects costs
        # nothing rather than a studyset's worth of downloads.
        estimator, corrector = self.load_specification(n_cores=n_cores)

        if image_based:
            self._reject_image_based_comparison()
            studyset_dict = self.prepare_images()
        else:
            # A coordinate-based kernel needs a sample size per experiment as
            # much as an image-based estimator does -- ALE sizes its Gaussian
            # from it, and one experiment without one fails the whole run --
            # and compose records it on the annotation note rather than on the
            # analysis. Copied, since cached_studyset is uploaded as the
            # result's snapshot and must keep what Neurostore served.
            studyset_dict = apply_sample_sizes(
                deepcopy(self.cached_studyset), self.cached_annotation
            )
        # The annotation is attached to the studyset rather than wrapped in an
        # object of its own: it is the studyset that owns the notes, and reading
        # them back gives the full analysis ids that ``slice`` selects on.
        studyset = Studyset(
            studyset_dict,
            target=self._TARGET_SPACE,
            annotations=[self.cached_annotation] if self.cached_annotation else None,
        )
        first_studyset, second_studyset = self.apply_filter(
            studyset, combine=not image_based
        )

        if image_based:
            # ImageTransformer writes maps it derives into the base path.
            first_studyset = first_studyset.update_path(str(self.image_dir))

        self.first_studyset = first_studyset
        self.second_studyset = second_studyset
        self.estimator = estimator
        self.corrector = corrector

    def _empty_input_maps(self):
        """Which input maps carry no usable value at all.

        NiMARE counts a voxel as valid only where it is finite *and* non-zero, so
        an empty upload has no valid voxel anywhere. Under ``aggressive_mask``
        the mask is the intersection across inputs, so one such map empties it.
        """
        inputs = getattr(self.estimator, "inputs_", None) or {}
        ids = [str(image_id) for image_id in inputs.get("id", [])]

        empty = set()
        for name, (kind, _) in (self.estimator._required_inputs or {}).items():
            if kind != "image" or name not in inputs:
                continue
            values = np.asarray(inputs[name], dtype=float)
            valid = np.isfinite(values) & (values != 0)
            empty.update(i for i in range(valid.shape[0]) if not valid[i].any())

        return [ids[i] if i < len(ids) else f"image {i}" for i in sorted(empty)]

    def _check_result_is_not_empty(self):
        """Refuse a result that has no value anywhere.

        An image-based meta-analysis can finish and write every map having
        computed nothing: the run looks successful and the maps that would be
        uploaded are entirely NaN.
        """
        maps = (self.meta_results.maps or {}) if self.meta_results else {}
        finite = {
            name: int(np.isfinite(np.asarray(values, dtype=float)).sum())
            for name, values in maps.items()
            if values is not None and not name.startswith("label_")
        }
        if not finite or any(finite.values()):
            return

        fitted = list((getattr(self.estimator, "inputs_", None) or {}).get("id", []))
        message = [
            "The meta-analysis produced no value at any voxel: every map is "
            f"entirely NaN. {len(fitted)} analysis/analyses reached the "
            f"estimator, of {len(self.first_studyset.analyses)} submitted."
        ]
        if len(fitted) < 2:
            message.append(
                "A meta-analysis needs at least two analyses to pool; see the "
                "coverage report for why the rest were left out."
            )
        empty = self._empty_input_maps()
        aggressive = getattr(self.estimator, "aggressive_mask", False)
        if empty:
            listed = ", ".join(empty[:5]) + (
                f", and {len(empty) - 5} more" if len(empty) > 5 else ""
            )
            message.append(
                f"{len(empty)} input map(s) have no finite non-zero voxel at all "
                f"and so contribute nothing; exclude them: {listed}."
            )
            if aggressive:
                message.append(
                    "Under aggressive_mask=True one such map empties the mask on "
                    "its own, however well the rest overlap."
                )
        elif aggressive:
            message.append(
                "With aggressive_mask=True a voxel must be valid in all "
                f"{len(self.estimator.inputs_.get('id', []))} input maps, and "
                "these do not overlap that completely. Set aggressive_mask=False, "
                "which analyses each group of voxels sharing a validity pattern "
                "and is NiMARE's default."
            )
        raise ValueError(" ".join(message))

    def _describe_coverage(self, report):
        """Log and persist the account of what the meta-analysis used."""
        self.coverage_report = report
        LGR.info(
            "Image coverage for %s:\n%s",
            type(self.estimator).__name__,
            report.summary(),
        )
        self.result_dir.mkdir(parents=True, exist_ok=True)
        (self.result_dir / "ibma_coverage.tsv").write_text(report.to_tsv())

        if report.excluded:
            LGR.warning(
                "%d of %d analyses are not in this meta-analysis. See %s.",
                len(report.excluded),
                len(report.analyses),
                self.result_dir / "ibma_coverage.tsv",
            )

    def _analysis_names(self):
        """Full analysis id to (study name, analysis name), for naming failures."""
        names = {}
        for study in self.first_studyset.studies:
            for analysis in study.analyses:
                names[f"{study.id}-{analysis.id}"] = (
                    getattr(study, "name", None) or study.id,
                    getattr(analysis, "name", None) or analysis.id,
                )
        return names

    def _describe_dependence_group(self, label):
        """Say which analyses a dependence group holds, and why they cancelled.

        NiMARE groups an estimator's images by the study that contributed them,
        so a group that cancels is one paper whose maps carry no joint signal --
        most often a pair of mirrored contrasts (A>B and B>A), which are exact
        negatives and average to zero.
        """
        inputs = getattr(self.estimator, "inputs_", None) or {}
        codes = np.asarray(inputs.get("contrast_names", []))
        ids = [str(image_id) for image_id in inputs.get("id", [])]
        if codes.size == 0 or codes.size != len(ids):
            return None
        try:
            code = int(label)
        except (TypeError, ValueError):
            return None

        members = [index for index in range(codes.size) if codes[index] == code]
        if not members:
            return None

        names = self._analysis_names()
        studies = {names.get(ids[i], (ids[i], None))[0] for i in members}
        labelled = [f"{names.get(ids[i], ('', ids[i]))[1]}" for i in members]
        lines = [
            f"That group is {' / '.join(sorted(studies))}, which contributed "
            f"{len(members)} analyses: {', '.join(labelled)}."
        ]

        image_name = next(
            (
                name
                for name, (kind, _) in (self.estimator._required_inputs or {}).items()
                if kind == "image" and name in inputs
            ),
            None,
        )
        if image_name is not None:
            maps = np.asarray(inputs[image_name], dtype=float)[members]
            valid = np.isfinite(maps) & (maps != 0)
            empty = [labelled[i] for i in range(len(members)) if not valid[i].any()]
            if empty:
                lines.append(
                    f"{', '.join(empty)} has no finite non-zero voxel, so it "
                    "carries no signal to pool."
                )
            else:
                shared = valid.all(axis=0)
                if shared.sum() > 1:
                    correlations = np.corrcoef(maps[:, shared])
                    off = correlations[~np.eye(len(members), dtype=bool)]
                    lowest = float(np.nanmin(off)) if off.size else None
                    if lowest is not None and lowest < -0.99:
                        first, second = np.unravel_index(
                            np.nanargmin(
                                np.where(
                                    np.eye(len(members), dtype=bool),
                                    np.nan,
                                    correlations,
                                )
                            ),
                            correlations.shape,
                        )
                        lines.append(
                            f"{labelled[first]} and {labelled[second]} correlate "
                            f"{lowest:+.3f}: they are the same contrast in "
                            "opposite directions, and averaging them cancels."
                        )
                    elif lowest is not None:
                        lines.append(
                            "Over the voxels they share, those maps correlate "
                            f"between {lowest:+.3f} and {float(np.nanmax(off)):+.3f}, "
                            "which sums to no joint variance -- what happens when a "
                            "paper contributes maps of complementary networks."
                        )

        lines.append(
            "Keep one analysis per group, or set the estimator's groupby to "
            "false, which treats every map as independent and inflates "
            "significance."
        )
        return " ".join(lines)

    def _explain_failure(self, message):
        """Add what compose knows to a NiMARE failure, when it can."""
        match = _CANCELLED_GROUP.search(message)
        if match is None:
            return message
        described = self._describe_dependence_group(match.group(1))
        return message if described is None else f"{message}\n\n{described}"

    def _fit_image_based(self, workflow):
        """Fit the workflow, then report what it used and what it discarded.

        When nothing survives there is no fitted estimator to read, so the
        submitted studyset is described instead and NiMARE's message is kept.
        """
        try:
            self.meta_results = workflow.fit(self.first_studyset)
        except ValueError as exc:
            # Which report is right depends on how far the fit got. Once the
            # estimator has inputs, the transform ran and describing the
            # submission instead would report every converted analysis as one
            # NiMARE could not convert.
            if (getattr(self.estimator, "inputs_", None) or {}).get("id") is not None:
                report = coverage.describe_estimator(
                    self.first_studyset, self.estimator, dropped_maps=self.dropped_maps
                )
            else:
                report = coverage.describe_submission(
                    self.first_studyset, self.estimator, dropped_maps=self.dropped_maps
                )
            self._describe_coverage(report)
            raise ValueError(
                f"{self._explain_failure(str(exc))}\n\n{report.summary()}"
            ) from exc

        self._describe_coverage(
            coverage.describe_result(
                self.meta_results, self.first_studyset, dropped_maps=self.dropped_maps
            )
        )
        self._check_result_is_not_empty()

    def create_result_object(self):
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
        kwargs = {"meta_analysis_id": self.meta_analysis_id}
        for entity_name, (
            live_payload,
            existing_payload,
            existing_id,
        ) in entity_payloads.items():
            if self._should_link_existing_snapshot(
                live_payload, existing_payload, existing_id
            ):
                kwargs[f"snapshot_{entity_name}_id"] = existing_id
            else:
                kwargs[f"snapshot_{entity_name}"] = self._json_safe_payload(
                    live_payload
                )

        self._compose_config.api_key["upload_key"] = self.nsc_key
        result = self.compose_api.meta_analysis_results_post(
            result_init=ResultInit(**kwargs)
        )
        self.result_id = result.id
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
        elif isinstance(self.estimator, IBMAEstimator):
            if self.second_studyset is not None:
                raise ValueError(
                    "A group comparison was requested, but no image-based estimator "
                    f"supports one. {type(self.estimator).__name__} takes a single "
                    "studyset. Choose a single-group selection, or a pairwise "
                    "coordinate-based estimator."
                )
            # Omitted when unset: the workflow passes n_cores straight to
            # _check_ncores, which cannot handle None.
            workflow_kwargs = {} if self.n_cores is None else {"n_cores": self.n_cores}
            workflow = IBMAWorkflow(
                estimator=self.estimator,
                corrector=self.corrector,
                diagnostics="jackknife",
                output_dir=self.result_dir,
                **workflow_kwargs,
            )
            self._fit_image_based(workflow)
        else:
            raise ValueError(
                "Estimator "
                f"{self.estimator} and studysets {self.first_studyset} and "
                f"{self.second_studyset} are not compatible."
            )
        self._persist_meta_results()

    def upload_results(self):
        # Mirror save_maps, which writes no file for a map whose value is None.
        stat_maps = [
            (m + ".nii.gz", (self.result_dir / (m + ".nii.gz")).read_bytes())
            for m, values in self.meta_results.maps.items()
            if not m.startswith("label_") and values is not None
        ]
        cluster_tables = [
            (f + ".tsv", (self.result_dir / (f + ".tsv")).read_bytes())
            for f, df in self.meta_results.tables.items()
            if f.endswith("clust") and not df.empty
        ]
        diagnostic_tables = [
            (f + ".tsv", (self.result_dir / (f + ".tsv")).read_bytes())
            for f, df in self.meta_results.tables.items()
            if not f.endswith("clust") and df is not None
        ]

        files = {}
        if stat_maps:
            files["statistical_maps"] = stat_maps
        if cluster_tables:
            files["cluster_tables"] = cluster_tables
        if diagnostic_tables:
            files["diagnostic_tables"] = diagnostic_tables

        # Use api_client.param_serialize so files go through files_parameters,
        # which correctly expands List[Tuple[name, bytes]] into separate multipart
        # parts with the same field name — the path the SDK's ResultUploadStatisticalMaps
        # form-params route doesn't support for multiple files.
        _param = self.compose_api.api_client.param_serialize(
            method="PUT",
            resource_path="/meta-analysis-results/{id}",
            path_params={"id": self.result_id},
            header_params={"Content-Type": "multipart/form-data"},
            post_params=[("method_description", self.meta_results.description_)],
            files=files,
            auth_settings=["upload_key"],
            collection_formats={},
        )
        response_data = self.compose_api.api_client.call_api(*_param)
        response_data.read()
        self.results_object = self.compose_api.api_client.response_deserialize(
            response_data=response_data,
            response_types_map={"200": "ResultReturn"},
        ).data

    def load_specification(self, n_cores=None):
        """Returns function to run analysis on dataset."""
        spec = self.cached_specification
        est_mod = import_module(".".join(["nimare", "meta", spec["type"].lower()]))
        estimator = getattr(est_mod, spec["estimator"]["type"])
        est_args = (
            {**spec["estimator"]["args"]} if spec["estimator"].get("args") else {}
        )
        if self._is_image_based():
            if est_args.get("**kwargs") is not None:
                est_args.update(est_args.pop("**kwargs"))
            # No image-based estimator takes n_cores; PermutedOLS parallelizes
            # its permutations under the name n_jobs.
            if n_cores is not None and "n_jobs" in signature(estimator).parameters:
                est_args.setdefault("n_jobs", n_cores)
        else:
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
            cor_args = (
                {**spec["corrector"]["args"]} if spec["corrector"].get("args") else {}
            )
            # A null is the config's way of saying "not set", but FWECorrector
            # keeps every extra argument and hands it to the estimator's
            # correction method, where None is a value: ALE thresholds against
            # it and PermutedOLS, which has no voxel_thresh at all, rejects it
            # outright. Dropping them defers to each estimator's own default,
            # which is what an unset parameter asks for.
            cor_args = {
                name: value for name, value in cor_args.items() if value is not None
            }
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
