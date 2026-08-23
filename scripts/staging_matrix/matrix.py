"""Run the (studyset x specification) matrix, one subprocess per cell."""

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = os.environ.get("COMPOSE_RUNNER_PYTHON", sys.executable)
BUNDLES = HERE / "bundles"
SPECS = HERE / "specs"
RUNS = HERE / "runs"
IMAGES = HERE / "images"

TERMS = [
    "hand",
    "language",
    "pain",
    "memory",
    "face",
    "motor",
    "reward",
    "emotion",
    "attention",
    "auditory",
    "working_memory",
    "semantic",
]
CORPORA = ["corpus_z", "corpus_t", "corpus_beta", "tiny"]

# (specification, annotation file, n_cores) per studyset group.
CORE = [
    ("stouffers_fdr", "boolean_noted", None),
    ("fishers_fdr", "boolean_noted", None),
]

TARGETED = {
    "corpus_z": [
        ("stouffers_fdr_negcorr", "boolean_noted", None),
        ("stouffers_fwe_bonferroni", "boolean_noted", None),
        ("stouffers_samplesize", "boolean_noted", None),
        ("stouffers_samplesize", "boolean_none", None),
        ("stouffers_onesided", "boolean_noted", None),
        ("stouffers_aggressive_mask", "boolean_noted", None),
        ("stouffers_groupby_study", "boolean_noted", None),
        ("stouffers_string_filter", "string_noted", None),
        ("stouffers_two_group", "boolean_split_noted", None),
        ("fishers_samplesize", "boolean_noted", None),
        ("stouffers_fdr", "boolean_noted", 4),
        ("stouffers_fdr", "string_noted", None),
        ("stouffers_fdr", "boolean_empty", None),
        ("stouffers_groupby_false", "boolean_noted", None),
    ],
    "corpus_z_edge": [],
    "corpus_t": [
        ("fixedeffectshedges_fdr", "boolean_noted", None),
        ("fixedeffectshedges_fdr", "boolean_real", None),
        ("fixedeffectshedges_config_as_written", "boolean_noted", None),
        ("fixedeffectshedges_fdr", "boolean_none", None),
        ("stouffers_fdr", "boolean_none", None),
        ("stouffers_samplesize", "boolean_real", None),
    ],
    "corpus_beta": [
        ("permutedols_fdr", "boolean_noted", None),
        ("permutedols_fdr", "boolean_noted", 4),
        ("permutedols_fwe_montecarlo", "boolean_noted", 4),
        ("samplesizebasedlikelihood_ml", "boolean_noted", None),
        ("samplesizebasedlikelihood_ml", "boolean_real", None),
        ("samplesizebasedlikelihood_config_as_written", "boolean_noted", None),
        ("dersimonianlaird_fdr", "boolean_noted", None),
        ("hedges_fdr", "boolean_noted", None),
        ("weightedleastsquares_fdr", "boolean_noted", None),
        ("variancebasedlikelihood_fdr", "boolean_noted", None),
    ],
    "tiny": [
        ("stouffers_aggressive_mask", "boolean_noted", None),
        ("permutedols_fdr", "boolean_noted", None),
    ],
    # These carry coordinates, so the coordinate-based path is reachable.
    "face": [
        ("stouffers_groupby_false", "boolean_noted", None),
        ("fishers_groupby_false", "boolean_noted", None),
        ("stouffers_aggressive_mask", "boolean_noted", None),
    ],
    "motor": [
        ("stouffers_groupby_false", "boolean_noted", None),
        ("ale_fdr", "boolean_noted", None),
        ("ale_fwe_montecarlo", "boolean_noted", 4),
        ("alesubtraction_two_group", "boolean_split_noted", None),
        ("mkdachi2_two_group", "boolean_split_noted", None),
    ],
    "language": [
        ("ale_fdr", "boolean_noted", None),
    ],
}

# Run alone: each loads the whole neurostore nightly release (~1.5 GB).
SERIAL = {
    "motor": [
        ("alesubtraction_database_neurostore", "boolean_noted", None),
        ("mkdachi2_database_neurostore", "boolean_noted", None),
    ],
    "corpus_z": [("stouffers_database_studyset", "boolean_noted", None)],
}


def cases(only=None):
    out = []
    for slug in TERMS + CORPORA:
        for spec, annotation, n_cores in CORE:
            out.append((slug, spec, annotation, n_cores))
    for slug, entries in TARGETED.items():
        for spec, annotation, n_cores in entries:
            out.append((slug, spec, annotation, n_cores))
    seen, unique = set(), []
    for slug, spec, annotation, n_cores in out:
        key = case_id(slug, spec, annotation, n_cores)
        if key in seen:
            continue
        seen.add(key)
        if only and only not in key:
            continue
        unique.append((slug, spec, annotation, n_cores))
    return unique


def serial_cases(only=None):
    out = []
    for slug, entries in SERIAL.items():
        for spec, annotation, n_cores in entries:
            if only and only not in case_id(slug, spec, annotation, n_cores):
                continue
            out.append((slug, spec, annotation, n_cores))
    return out


def case_id(slug, spec, annotation, n_cores):
    suffix = "" if n_cores is None else f"__cores{n_cores}"
    return f"{slug}__{spec}__{annotation}{suffix}"


def run_case(case, timeout=3600, force=False):
    slug, spec, annotation, n_cores = case
    name = case_id(*case)
    out = RUNS / name
    result = out / "result.json"
    if result.is_file() and not force:
        return name, "cached", 0.0

    command = [
        PY,
        str(HERE / "run_case.py"),
        "--studyset",
        str(BUNDLES / slug / "studyset.json"),
        "--annotation",
        str(BUNDLES / slug / f"annotation_{annotation}.json"),
        "--specification",
        str(SPECS / f"{spec}.json"),
        "--out",
        str(out),
        "--image-cache",
        str(IMAGES),
    ]
    if n_cores is not None:
        command += ["--n-cores", str(n_cores)]

    started = time.time()
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        status = "ran"
        if proc.returncode < 0:
            status = f"signal {-proc.returncode}"
    except subprocess.TimeoutExpired:
        status = "timeout"
        out.mkdir(parents=True, exist_ok=True)
        result.write_text(
            json.dumps(
                {
                    "status": "error",
                    "error_type": "Timeout",
                    "error": f"case exceeded {timeout}s",
                    "studyset": slug,
                    "specification": spec,
                    "annotation": annotation,
                },
                indent=1,
            )
        )
        return name, status, time.time() - started
    else:
        if not result.is_file():
            out.mkdir(parents=True, exist_ok=True)
            result.write_text(
                json.dumps(
                    {
                        "status": "error",
                        "error_type": "HarnessFailure",
                        "error": (proc.stderr or proc.stdout)[-4000:],
                        "studyset": slug,
                        "specification": spec,
                        "annotation": annotation,
                    },
                    indent=1,
                )
            )
    return name, status, time.time() - started


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--only", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--serial-only", action="store_true")
    args = parser.parse_args()

    todo = [] if args.serial_only else cases(args.only)
    print(
        f"{len(todo)} parallel cases, {len(serial_cases(args.only))} serial", flush=True
    )

    def _report(outcome):
        name, status, seconds = outcome
        record = RUNS / name / "result.json"
        summary = {}
        if record.is_file():
            data = json.loads(record.read_text())
            summary = {
                "status": data.get("status"),
                "error_type": data.get("error_type"),
                "error": (data.get("error") or "")[:160],
            }
        print(
            f"[{status:8s} {seconds:6.1f}s] {name} -> {json.dumps(summary)}", flush=True
        )

    if todo:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for outcome in pool.map(
                lambda c: run_case(c, args.timeout, args.force), todo
            ):
                _report(outcome)

    for case in serial_cases(args.only):
        _report(run_case(case, args.timeout, args.force))


if __name__ == "__main__":
    sys.exit(main())
