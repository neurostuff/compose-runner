"""Assemble and run an image-based meta-analysis bundle end to end.

Compose cannot create an IBMA yet, so there is no bundle to download and no way
to find out what breaks by running one. This builds the bundle the frontend
*would* produce -- real Neurostore studies, real NeuroVault maps, and an
estimator specification whose arguments and defaults are read out of the
frontend's own ``meta_analysis_params.json`` -- and pushes it through
:class:`~compose_runner.run.Runner`.

The point is that the specification is not hand-written. Taking the argument
names and defaults from the committed config is what surfaces the places where
the config, NiMARE and this runner disagree; a hand-written specification would
just encode what already works.

Usage::

    python scripts/simulate_ibma_bundle.py --estimator Stouffers
    python scripts/simulate_ibma_bundle.py --estimator FixedEffectsHedges --n-studies 8
    python scripts/simulate_ibma_bundle.py --list

    # Write the bundle out without running it, to see its shape:
    python scripts/simulate_ibma_bundle.py --estimator Fishers --dry-run
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from compose_runner.images import normalize_value_type  # noqa: E402
from compose_runner.run import Runner  # noqa: E402

LGR = logging.getLogger("simulate_ibma_bundle")

NEUROSTORE_API = "https://neurostore.org/api"

# The frontend config that decides which arguments a specification carries.
# Overridable because it only exists on the branch that adds IBMA support.
DEFAULT_CONFIG_PATH = Path(
    os.environ.get(
        "COMPOSE_PARAMS_CONFIG",
        "/home/jdkent/projects/neurostore/compose/neurosynth-frontend/src/assets"
        "/config/meta_analysis_params.json",
    )
)

# Which Neurostore study flag says a study carries the maps an estimator needs.
# Neurostore precomputes these, so candidate studies can be found without
# reading every image.
_IMAGE_TYPE_TO_STUDY_FLAG = {
    "z": "has_z_maps",
    "t": "has_t_maps",
    "beta": "has_beta_and_variance_maps",
    "varcope": "has_beta_and_variance_maps",
}


def load_config(config_path):
    """Read the frontend's algorithm config."""
    if not config_path.is_file():
        raise SystemExit(
            f"No config at {config_path}. Point --config at a checkout of the "
            "neurostore branch that adds IBMA support, or set "
            "COMPOSE_PARAMS_CONFIG."
        )
    with open(config_path) as handle:
        return json.load(handle)


def required_image_types(estimator_entry, estimator_name):
    """Which NiMARE image types the estimator needs, per the config.

    Falls back to asking NiMARE directly, since ``requirements`` is new and an
    older config will not carry it.
    """
    requirements = estimator_entry.get("requirements")
    if requirements:
        return list(requirements.get("images") or [])

    from nimare.meta import ibma

    cls = getattr(ibma, estimator_name)
    return [
        target
        for _, (kind, target) in getattr(cls, "_required_inputs", {}).items()
        if kind == "image"
    ]


def build_specification(config, estimator_name, corrector_name=None):
    """Build the specification the frontend would POST for this estimator.

    Every argument the config lists is sent with its configured default, which
    is what the frontend does. That is deliberate: an argument the config still
    advertises but NiMARE has dropped should show up here as a failure, not be
    quietly omitted.
    """
    ibma_config = config.get("IBMA") or {}
    if estimator_name not in ibma_config:
        raise SystemExit(
            f"{estimator_name} is not in the config's IBMA section. Available: "
            f"{sorted(ibma_config)}"
        )

    entry = ibma_config[estimator_name]
    estimator_args = {
        name: parameter.get("default")
        for name, parameter in (entry.get("parameters") or {}).items()
    }
    # The frontend always sends this key, empty or not.
    estimator_args["**kwargs"] = {}

    if corrector_name is None:
        # PermutedOLS is the only image-based estimator with its own FWE
        # implementation. Everything else has to use FDR, because NiMARE's
        # montecarlo FWE for IBMA lives on the estimator.
        corrector_name = "FWECorrector" if entry.get("FWE_enabled") else "FDRCorrector"

    corrector_config = (config.get("CORRECTOR") or {}).get(corrector_name) or {}
    corrector_args = {
        name: parameter.get("default")
        for name, parameter in (corrector_config.get("parameters") or {}).items()
    }
    if corrector_name == "FWECorrector":
        corrector_args["method"] = "montecarlo"
        # Small, so the simulation finishes; a real run uses 5000.
        corrector_args["n_iters"] = 50

    return {
        "id": "simulated-specification",
        "type": "IBMA",
        "estimator": {"type": estimator_name, "args": estimator_args},
        "corrector": {"type": corrector_name, "args": corrector_args},
        "filter": "included",
        "conditions": [True],
        "weights": [1.0],
        "database_studyset": None,
    }


def fetch_candidate_studies(image_types, n_studies, session, page_size=50):
    """Pull real Neurostore studies that carry the maps this estimator needs.

    Filters on Neurostore's precomputed per-study flags, then verifies against
    the study's actual images, because the flags describe the study while the
    estimator consumes analyses.
    """
    flags = {
        _IMAGE_TYPE_TO_STUDY_FLAG[image_type]
        for image_type in image_types
        if image_type in _IMAGE_TYPE_TO_STUDY_FLAG
    }
    wanted = set(image_types)

    selected = []
    page = 1
    while len(selected) < n_studies and page <= 20:
        response = session.get(
            f"{NEUROSTORE_API}/studies/",
            params={
                "source": "neurovault",
                "page_size": page_size,
                "page": page,
                "nested": "true",
            },
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") or []
        if not results:
            break

        for study in results:
            if flags and not all(study.get(flag) for flag in flags):
                continue
            usable = _usable_analyses(study, wanted)
            if not usable:
                continue
            selected.append({**study, "analyses": usable})
            if len(selected) >= n_studies:
                break
        page += 1

    if not selected:
        raise SystemExit(
            f"No Neurostore study was found carrying {sorted(wanted)} maps."
        )
    return selected


def fetch_studies_from_search(search, data_type, n_studies, session, max_versions=3):
    """Assemble studies from a Neurostore base-study search.

    This is the shape a compose studyset actually has: the user searches, picks
    base studies, and one version of each becomes a study. Nothing is filtered
    on whether the estimator can use it -- that is the point. Whatever comes
    back comes back, and the coverage report says what happened to it.

    Parameters
    ----------
    search : :obj:`str`
        Free-text query, passed through to ``/base-studies/``.
    data_type : :obj:`str`
        ``"image"`` to ask for base studies that carry maps at all.
    n_studies : :obj:`int`
        Cap on how many base studies to take.
    max_versions : :obj:`int`
        Versions to try per base study before giving up on it. One base study
        in this corpus has 171, so trying them all is not an option.
    """
    response = session.get(
        f"{NEUROSTORE_API}/base-studies/",
        params={"search": search, "data_type": data_type, "page_size": 100},
        timeout=120,
    )
    response.raise_for_status()
    base_studies = (response.json().get("results") or [])[:n_studies]
    LGR.info("Search %r returned %d base study/studies.", search, len(base_studies))

    studies = []
    for base_study in base_studies:
        study = _first_version_with_images(base_study, session, max_versions)
        if study is None:
            LGR.info(
                "No version of %s carries analyses with images; skipping.",
                base_study.get("id"),
            )
            continue
        studies.append(study)

    if not studies:
        raise SystemExit(f"No study with images was found for search {search!r}.")
    return studies


def _first_version_with_images(base_study, session, max_versions):
    """Fetch versions of a base study until one has analyses carrying images."""
    for version_id in (base_study.get("versions") or [])[:max_versions]:
        version_id = version_id if isinstance(version_id, str) else version_id.get("id")
        if not version_id:
            continue
        response = session.get(
            f"{NEUROSTORE_API}/studies/{version_id}",
            params={"nested": "true"},
            timeout=120,
        )
        if response.status_code != 200:
            continue
        study = response.json()
        if any(analysis.get("images") for analysis in study.get("analyses") or []):
            return study
    return None


def _usable_analyses(study, wanted):
    """Keep the analyses that carry every map type the estimator needs."""
    usable = []
    for analysis in study.get("analyses") or []:
        present = {
            normalize_value_type(image.get("value_type"))
            for image in (analysis.get("images") or [])
        }
        if wanted <= present:
            usable.append(analysis)
    return usable


def build_studyset(studies, max_analyses_per_study=2):
    """Assemble the studies into the NIMADS studyset a snapshot would hold.

    Several analyses per study are kept on purpose: a study contributing more
    than one contrast is what the dependence handling exists for, and it is the
    normal case for NeuroVault collections.
    """
    payload_studies = []
    for study in studies:
        analyses = study["analyses"][:max_analyses_per_study]
        payload_studies.append(
            {
                "id": study["id"],
                "name": study.get("name"),
                "authors": study.get("authors"),
                "publication": study.get("publication"),
                "metadata": study.get("metadata") or {},
                "analyses": [
                    {
                        "id": analysis["id"],
                        "name": analysis.get("name"),
                        # Left empty because this builds from /studies/, whose
                        # nested response gives condition *ids* as bare strings.
                        # The /studysets/ endpoint compose actually reads returns
                        # them as objects with a name, which nimads.Analysis
                        # handles; ids would not survive it. Real snapshots
                        # frequently carry no conditions anyway.
                        "conditions": [],
                        "weights": [],
                        "images": analysis.get("images") or [],
                        "points": analysis.get("points") or [],
                        "metadata": analysis.get("metadata") or {},
                    }
                    for analysis in analyses
                ],
            }
        )

    return {
        "id": "simulated-studyset",
        "name": "Simulated IBMA studyset",
        "studies": payload_studies,
    }


def build_annotation(studyset, sample_size=25):
    """Build the inclusion annotation, with a sample size on every note.

    Compose has nowhere else to record sample size, and two estimators require
    it, so the note is where it has to come from.

    Pass ``sample_size=None`` to leave it out. That is worth doing: without a
    sample size NiMARE cannot turn a t-map into a z-map, so it is the difference
    between including and excluding every t-map-only study in a corpus.
    """
    note_keys = {"included": "boolean"}
    if sample_size is not None:
        note_keys["sample_size"] = "number"

    notes = []
    for study in studyset["studies"]:
        for analysis in study["analyses"]:
            note = {"included": True}
            if sample_size is not None:
                note["sample_size"] = sample_size
            notes.append(
                {"study": study["id"], "analysis": analysis["id"], "note": note}
            )

    return {
        "id": "simulated-annotation",
        "name": "Simulated inclusion annotation",
        "note_keys": note_keys,
        "notes": notes,
    }


def summarize_bundle(bundle):
    """Describe the bundle the way a reviewer would want to see it."""
    studyset = bundle["studyset"]
    n_analyses = sum(len(study["analyses"]) for study in studyset["studies"])
    n_images = sum(
        len(analysis["images"])
        for study in studyset["studies"]
        for analysis in study["analyses"]
    )
    value_types = sorted(
        {
            str(image.get("value_type"))
            for study in studyset["studies"]
            for analysis in study["analyses"]
            for image in analysis["images"]
        }
    )
    multi = [study["id"] for study in studyset["studies"] if len(study["analyses"]) > 1]

    lines = [
        "Bundle",
        f"  studies              : {len(studyset['studies'])}",
        f"  analyses             : {n_analyses}",
        f"  images               : {n_images}",
        f"  value_types present  : {value_types}",
        f"  studies w/ >1 analysis: {len(multi)} (dependence applies to these)",
        "",
        "Specification",
        json.dumps(bundle["specification"], indent=2),
    ]
    return "\n".join(lines)


def simulate(
    estimator_name,
    config_path=DEFAULT_CONFIG_PATH,
    n_studies=6,
    result_dir=None,
    corrector_name=None,
    dry_run=False,
    n_cores=None,
    search=None,
    data_type="image",
    sample_size=25,
):
    """Build a bundle for one estimator and, unless dry, run it.

    With ``search``, the studyset comes from a Neurostore base-study query and
    is taken as it is -- uneven, partly unusable, exactly what compose will
    hand over. Without it, studies are picked for already carrying the maps the
    estimator wants, which is the easier case.
    """
    config = load_config(Path(config_path))
    specification = build_specification(config, estimator_name, corrector_name)
    image_types = required_image_types(config["IBMA"][estimator_name], estimator_name)
    LGR.info("%s needs %s maps.", estimator_name, image_types)

    session = requests.Session()
    if search:
        studies = fetch_studies_from_search(search, data_type, n_studies, session)
    else:
        studies = fetch_candidate_studies(image_types, n_studies, session)
    studyset = build_studyset(studies)
    annotation = build_annotation(studyset, sample_size=sample_size)

    bundle = {
        "studyset": studyset,
        "annotation": annotation,
        "specification": specification,
    }

    result_dir = Path(result_dir or REPO_ROOT / "simulated_ibma" / estimator_name)
    result_dir.mkdir(parents=True, exist_ok=True)
    with open(result_dir / "bundle.json", "w") as handle:
        json.dump(bundle, handle, indent=2)

    print(summarize_bundle(bundle))
    print(f"\nBundle written to {result_dir / 'bundle.json'}")

    if dry_run:
        return None

    runner = Runner(
        meta_analysis_id="simulated-meta-analysis",
        environment="production",
        result_dir=result_dir,
    )
    runner.cached_studyset = studyset
    runner.cached_annotation = annotation
    runner.cached_specification = specification

    print("\nRunning...")
    runner.process_bundle(n_cores=n_cores)
    runner.run_meta_analysis()

    _report_result(runner)
    return runner


def _report_result(runner):
    """Print what the run actually produced, and what it inferred."""
    import numpy as np

    estimator = runner.estimator
    dependence = estimator._dependence()
    maps = runner.meta_results.maps

    print("\nResult")
    print(f"  estimator            : {type(estimator).__name__}")
    print(f"  corrector            : {type(runner.corrector).__name__}")
    print(f"  images fitted        : {len(estimator.inputs_['id'])}")
    print(f"  independent groups   : {dependence.n_groups}")
    print(f"  dependence corrected : {dependence.has_dependence}")
    print(f"  maps                 : {sorted(maps)}")
    for name in sorted(maps):
        values = np.asarray(maps[name], dtype=float)
        finite = np.isfinite(values)
        if not finite.any():
            print(f"    {name:<38} all non-finite")
            continue
        print(
            f"    {name:<38} finite {finite.sum():>7}/{finite.size:<7} "
            f"range [{values[finite].min():+.3f}, {values[finite].max():+.3f}]"
        )
    print(f"  output               : {runner.result_dir}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimator", default="Stouffers")
    parser.add_argument("--corrector", default=None)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--n-studies", type=int, default=6)
    parser.add_argument("--result-dir", default=None)
    parser.add_argument("--n-cores", type=int, default=None)
    parser.add_argument(
        "--search",
        default=None,
        help=(
            "Build the studyset from a Neurostore base-study search instead of "
            "picking studies that already carry the estimator's maps. This is "
            "the messy case: whatever the search returns is used as-is."
        ),
    )
    parser.add_argument(
        "--data-type",
        default="image",
        help="data_type filter for --search. Default is 'image'.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=25,
        help=(
            "Sample size to record on every annotation note. Pass 0 to leave it "
            "out, which is what stops NiMARE deriving a z-map from a t-map."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print the bundle without running the meta-analysis.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the image-based estimators the config exposes.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    config = load_config(Path(args.config))
    if args.list:
        for name, entry in sorted((config.get("IBMA") or {}).items()):
            print(
                f"{name:<28} images={required_image_types(entry, name)} "
                f"FWE={entry.get('FWE_enabled')}"
            )
        return 0

    simulate(
        args.estimator,
        config_path=args.config,
        n_studies=args.n_studies,
        result_dir=args.result_dir,
        corrector_name=args.corrector,
        dry_run=args.dry_run,
        n_cores=args.n_cores,
        search=args.search,
        data_type=args.data_type,
        sample_size=args.sample_size or None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
