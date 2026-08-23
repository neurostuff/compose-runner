"""Drive compose_runner.Runner over one hand-rolled bundle, in its own process.

Isolated per case so a segfault, a hang or an unpickleable failure costs one
cell of the matrix rather than the run.
"""

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import numpy as np  # noqa: E402

from compose_runner.run import Runner  # noqa: E402


class _Collector(logging.Handler):
    """Keep the warnings and errors a run emits, for the report."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.records = []

    def emit(self, record):
        try:
            self.records.append(
                {
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage()[:2000],
                }
            )
        except Exception:
            pass


def _map_stats(meta_results):
    stats = {}
    for name, values in (meta_results.maps or {}).items():
        if values is None or name.startswith("label_"):
            continue
        array = np.asarray(values, dtype=float)
        finite = np.isfinite(array)
        stats[name] = {
            "finite": int(finite.sum()),
            "size": int(array.size),
            "min": float(array[finite].min()) if finite.any() else None,
            "max": float(array[finite].max()) if finite.any() else None,
        }
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--studyset", required=True)
    parser.add_argument("--annotation", required=True)
    parser.add_argument("--specification", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--image-cache", required=True)
    parser.add_argument("--n-cores", type=int, default=None)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # One shared image cache for the whole matrix: the maps are large and the
    # same studies recur across cases.
    images = out / "images"
    if not images.exists():
        images.symlink_to(Path(args.image_cache).resolve())

    collector = _Collector()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        filename=str(out / "run.log"),
        filemode="w",
    )
    logging.getLogger().addHandler(collector)

    record = {
        "studyset": args.studyset,
        "annotation": args.annotation,
        "specification": args.specification,
        "status": "error",
    }

    runner = Runner(
        meta_analysis_id="local-" + out.name,
        environment="staging",
        result_dir=out,
    )
    runner.cached_studyset = json.loads(Path(args.studyset).read_text())
    runner.cached_annotation = json.loads(Path(args.annotation).read_text())
    runner.cached_specification = json.loads(Path(args.specification).read_text())

    try:
        runner.process_bundle(n_cores=args.n_cores)
        record["stage"] = "process_bundle"
        record["n_analyses_selected"] = len(
            runner.first_studyset.analyses
            if hasattr(runner.first_studyset, "analyses")
            else []
        )
        runner.run_meta_analysis()
        record["stage"] = "run_meta_analysis"
        record["status"] = "ok"
        record["maps"] = _map_stats(runner.meta_results)
        record["tables"] = sorted(runner.meta_results.tables or {})
        estimator = runner.estimator
        record["n_images_fitted"] = len(
            (getattr(estimator, "inputs_", {}) or {}).get("id", [])
        )
        try:
            dependence = estimator._dependence()
            record["dependence"] = {
                "n_groups": int(dependence.n_groups),
                "has_dependence": bool(dependence.has_dependence),
            }
        except Exception as exc:
            record["dependence_error"] = repr(exc)
    except BaseException as exc:  # noqa: BLE001 - the point is to catalogue these
        record["error_type"] = type(exc).__name__
        record["error"] = str(exc)[:4000]
        record["traceback"] = traceback.format_exc()[-8000:]
        frames = traceback.extract_tb(exc.__traceback__)
        record["raised_in"] = (
            f"{frames[-1].filename}:{frames[-1].lineno} in {frames[-1].name}"
            if frames
            else None
        )
        record["frames"] = [
            f"{Path(f.filename).name}:{f.lineno} {f.name}" for f in frames
        ]

    if runner.coverage_report is not None:
        report = runner.coverage_report
        record["coverage"] = {
            "analyses": len(report.analyses),
            "included": len(report.analyses) - len(report.excluded),
            "excluded": len(report.excluded),
        }
    record["dropped_maps"] = sum(len(v) for v in (runner.dropped_maps or {}).values())
    record["warnings"] = collector.records[-40:]

    (out / "result.json").write_text(json.dumps(record, indent=1, default=str))
    print(json.dumps({k: record.get(k) for k in ("status", "error_type", "error")}))
    return 0 if record["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
