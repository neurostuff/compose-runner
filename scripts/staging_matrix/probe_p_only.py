"""What happens to an analysis whose only usable map is a p map.

NiMARE's only route from a p map is ``p_to_z``, which is documented to return an
*unsigned* z, so such an analysis used to join a Stouffers meta-analysis as an
all-positive map -- reported as a successful conversion. Measured here on a real
staging p map, before ``compose_runner.images`` stopped mapping the label:

    3GSBdy6kxwWe-UcibZsQV2tSg   min -7.081  max +5.152  negative 57.2%
    3uj2L2hXRs9G-794UmU2Hx3Pk   min -3.225  max +3.296  negative 59.0%
    5D2wQxcdwZt2-6kXDsMhjpYiK   min -6.434  max +9.629  negative 40.0%
    8DzjYSWuiXNn-55bN7jfniJMt   min +0.302  max +6.490  negative  0.0%  <- p-only
    qJk7k8rMaU26-k8hTTy9oiJn4   min -6.857  max +7.963  negative 36.8%

No analysis in the staging studysets is p-only -- the one carrying a p map
carries a z map too, which takes precedence -- so this builds the case out of
that same real map by keeping only its p map. Run it to see the analysis dropped
with a reason instead.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np  # noqa: E402

from compose_runner.run import Runner  # noqa: E402

HERE = Path(__file__).resolve().parent
P_LABELS = ("p map", "p map (given null hypothesis)")


def _is_p_map(image):
    return str(image.get("value_type") or "").strip().lower() in P_LABELS


def build_studyset():
    """Four z-map studies and one cut down to its p map."""
    studyset = json.loads((HERE / "bundles" / "corpus_z" / "studyset.json").read_text())
    kept, p_only_id = [], None

    for study in studyset["studies"]:
        p_analysis = next(
            (a for a in study["analyses"] if any(map(_is_p_map, a["images"]))), None
        )
        if p_only_id is None and p_analysis is not None:
            p_analysis["images"] = [i for i in p_analysis["images"] if _is_p_map(i)][:1]
            study["analyses"] = [p_analysis]
            p_only_id = f"{study['id']}-{p_analysis['id']}"
            kept.append(study)
        elif len(kept) < 5:
            study["analyses"] = study["analyses"][:1]
            kept.append(study)
        if len(kept) >= 5 and p_only_id:
            break

    if p_only_id is None:
        raise SystemExit("No analysis in corpus_z carries a p map.")
    return {"id": "p-only-probe", "name": "p-only probe", "studies": kept}, p_only_id


def main():
    studyset, p_only_id = build_studyset()
    annotation = {
        "id": "p-only-annotation",
        "name": "p-only probe",
        "note_keys": {"included": "boolean", "sample_size": "number"},
        "notes": [
            {
                "study": s["id"],
                "analysis": a["id"],
                "note": {"included": True, "sample_size": 25},
            }
            for s in studyset["studies"]
            for a in s["analyses"]
        ],
    }

    out = HERE / "probes" / "p_only"
    out.mkdir(parents=True, exist_ok=True)
    images = out / "images"
    if not images.exists():
        images.symlink_to((HERE / "images").resolve())

    runner = Runner("p-only-probe", environment="staging", result_dir=out)
    runner.cached_studyset = studyset
    runner.cached_annotation = annotation
    runner.cached_specification = json.loads(
        (HERE / "specs" / "stouffers_fdr.json").read_text()
    )
    runner.process_bundle()
    runner.run_meta_analysis()

    ids = [str(i) for i in runner.estimator.inputs_["id"]]
    print(f"\np-only analysis : {p_only_id}")
    print(f"  in the fit    : {p_only_id in ids}")
    for analysis_id, reasons in (runner.dropped_maps or {}).items():
        print(f"  dropped {analysis_id}: {'; '.join(reasons)}")

    z = np.asarray(runner.estimator.inputs_["z_maps"], dtype=float)
    print("\ninput maps")
    for row, image_id in enumerate(ids):
        values = z[row][np.isfinite(z[row]) & (z[row] != 0)]
        marker = "   <-- p-only" if image_id == p_only_id else ""
        print(
            f"  {image_id}: min {values.min():+.3f} max {values.max():+.3f} "
            f"negative {100 * (values < 0).mean():.1f}%{marker}"
        )
    print("\ncoverage:\n" + runner.coverage_report.summary())


if __name__ == "__main__":
    main()
