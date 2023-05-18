from importlib import import_module
from pathlib import Path

import requests

import neurosynth_compose_sdk
from neurosynth_compose_sdk.apis.tags.compose_api import ComposeApi
import neurostore_sdk
from neurostore_sdk.apis.tags.store_api import StoreApi
from nimare.workflows import cbma_workflow
from nimare.nimads import Studyset, Annotation

COMPOSE_URL = "https://compose.neurosynth.org/api"
STORE_URL = "https://neurostore.org/api"

compose_configuration = neurosynth_compose_sdk.Configuration(
    host=COMPOSE_URL
)
store_configuration = neurostore_sdk.Configuration(
    host=STORE_URL
)

# Enter a context with an instance of the API client
compose_client = neurosynth_compose_sdk.ApiClient(compose_configuration)
store_client = neurostore_sdk.ApiClient(store_configuration)

# Create an instance of the API class
compose_api = ComposeApi(compose_client)
store_api = StoreApi(store_client)


def load_specification(spec):
    """Returns function to run analysis on dataset."""
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


def download_bundle(meta_analysis_id):
    meta_analysis = requests.get(f"{COMPOSE_URL}/meta-analyses/{meta_analysis_id}").json()
    run_key = meta_analysis["run_key"]
    studyset_dict = requests.get(f"{STORE_URL}/studysets/{meta_analysis['studyset']}?nested=true").json()
    annotation_dict = requests.get(f"{STORE_URL}/annotations/{meta_analysis['annotation']}").json()
    specification_dict = requests.get(f"{COMPOSE_URL}/specifications/{meta_analysis['specification']}").json()
    return studyset_dict, annotation_dict, specification_dict, run_key


def process_bundle(studyset_dict, annotation_dict, specification_dict):
    studyset = Studyset(studyset_dict)
    annotation = Annotation(annotation_dict, studyset)
    include = specification_dict["filter"]
    analysis_ids = [n.analysis.id for n in annotation.notes if n.note[f"{include}"]]
    filtered_studyset = studyset.slice(analyses=analysis_ids)
    dataset = filtered_studyset.to_dataset()
    estimator, corrector = load_specification(specification_dict)
    return dataset, estimator, corrector


def upload_results(results, result_dir, result_id, nsc_key=None, nv_key=None):
    statistical_maps = [
        (
            "statistical_maps",
            open(result_dir / (m + ".nii.gz"), "rb"),
        ) for m in results.maps.keys()
    ]
    cluster_tables = [
        (
            "cluster_tables",
            open(result_dir / (f + ".tsv"), "rb"),
        ) for f in results.tables.keys()
        if "clust" in f
    ]

    diagnostic_tables = [
        (
            "diagnostic_tables",
            open(result_dir / (f + ".tsv"), "rb"),
        ) for f in results.tables.keys()
        if "clust" not in f
    ]
    files = statistical_maps + cluster_tables + diagnostic_tables
    headers = {"Compose-Upload-Key": nsc_key}
    upload_resp = requests.put(
        f"{COMPOSE_URL}/meta-analysis-results/{result_id}",
        files=files,
        json={"method_description": results.description_},
        headers=headers,
    )

    return upload_resp.json()


def run(meta_analysis_id, nsc_key=None, nv_key=None):
    studyset, annotation, specification, run_key = download_bundle(meta_analysis_id)
    if nsc_key is None:
        nsc_key = run_key

    # take a snapshot of the studyset and annotation (before running the workflow)
    headers = {"Compose-Upload-Key": nsc_key}
    resp = requests.post(
        f"{COMPOSE_URL}/meta-analysis-results",
        json={
            "meta_analysis_id": meta_analysis_id,
            # "studyset_snapshot": studyset, ## fix this
            # "annotation_snapshot": annotation, ## fix this
        },
        headers=headers,
    )
    result_id = resp.json()["id"]
    dataset, estimator, corrector = process_bundle(studyset, annotation, specification)

    output_dir = Path("results")
    output_dir.mkdir(exist_ok=True)
    results = cbma_workflow(dataset, estimator, corrector, output_dir=output_dir)
    upload_results(results, output_dir, result_id, nsc_key, nv_key)


run("5kpBKDqxNVsU")
