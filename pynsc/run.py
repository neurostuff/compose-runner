from importlib import import_module

import neurosynth_compose_sdk
from neurosynth_compose_sdk.apis.tags.compose_api import ComposeApi
import neurostore_sdk
from neurostore_sdk.apis.tags.store_api import StoreApi
from nimare.workflows import cbma_workflow
from nimare.nimads import Studyset, Annotation

compose_configuration = neurosynth_compose_sdk.Configuration(
    host = "https://compose.neurosynth.org/api"
)
store_configuration = neurostore_sdk.Configuration(
    host = "https://neurostore.org/api"
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
        args = {**spec["estimator"]["args"]}
        if spec["estimator"]["args"].get("**kwargs") is not None:
            for k, v in spec["estimator"]["args"]["**kwargs"].items():
                args[k] = v
        estimator_init = estimator(**args)
    else:
        estimator_init = estimator()

    if spec.get("corrector"):
        cor_mod = import_module(".".join(["nimare", "correct"]))
        corrector = getattr(cor_mod, spec["corrector"]["type"])
        corrector_args = spec["corrector"].get("args")
        if corrector_args:
            if corrector_args.get("**kwargs") is not None:
                for k, v in corrector_args["**kwargs"].items():
                    corrector_args[k] = v
                corrector_args.pop("**kwargs")
            corrector_init = corrector(**corrector_args)
        else:
            corrector_init = corrector()
    else:
        corrector_init = None

    return estimator_init, corrector_init


def download_bundle(meta_analysis_id):
    meta_analysis = compose_api.meta_analyses_id_get(path_params={"id": meta_analysis_id}).body
    studyset_dict = dict(store_api.studysets_id_get(
        path_params={"id": meta_analysis["studyset"]}, query_params={"nested": True}
    ).body)
    annotation_dict = dict(store_api.annotations_id_get(
        path_params={"id": meta_analysis["annotation"]}
    ).body)
    specification_dict = dict(compose_api.specifications_id_get(
        path_params={"id": meta_analysis["specification"]}
    ).body)
    return studyset_dict, annotation_dict, specification_dict


def process_bundle(studyset_dict, annotation_dict, specification_dict):
    studyset = Studyset(studyset_dict)
    annotation = Annotation(annotation_dict, studyset)
    include = specification_dict["filter"]
    analysis_ids = [n.analysis.id for n in annotation.notes if n.note[f"{include}"]]
    filtered_studyset = studyset.slice(analyses=analysis_ids)
    dataset = filtered_studyset.to_dataset()
    estimator, corrector = load_specification(specification_dict)
    return dataset, estimator, corrector


def upload_results(results, nsc_key=None, nv_key=None):
    pass


def run(meta_analysis_id, nsc_key=None, nv_key=None):
    studyset, annotation, specification = download_bundle(meta_analysis_id)
    dataset, estimator, corrector = process_bundle(studyset, annotation, specification)
    results = cbma_workflow(dataset, estimator, corrector)
    upload_results(results, nsc_key, nv_key)


run("3d5yYTAupehV")
