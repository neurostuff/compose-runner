from importlib import import_module
import requests

from nimare.workflows import cbma_workflow
from nimare.nimads import Studyset, Annotation

COMPOSE_URL = "https://compose.neurosynth.org/api"
STORE_URL = "https://neurostore.org/api"


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
    studyset_dict = requests.get(f"{STORE_URL}/studysets/{meta_analysis['studyset']}?nested=true").json()
    annotation_dict = requests.get(f"{STORE_URL}/annotations/{meta_analysis['annotation']}").json()
    specification_dict = requests.get(f"{COMPOSE_URL}/specifications/{meta_analysis['specification']}").json()
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
