"""Assemble type-targeted studysets out of the cached staging studies.

The per-term studysets are whatever the search returned. These are cut by which
NiMARE image type an analysis actually carries, so an estimator's required input
is present rather than derived -- the case a compose user gets when they curate.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from compose_runner.images import normalize_value_type  # noqa: E402
from build_bundles import BUNDLES, RAW, build_annotation, describe  # noqa: E402


def _all_studies():
    """Every cached study, deduplicated by id."""
    studies = {}
    for path in sorted(RAW.glob("*.json")):
        if path.name == "flagged_ids.json":
            continue
        for study in json.loads(path.read_text()):
            studies.setdefault(study["id"], study)
    return list(studies.values())


def corpus_studyset(name, wanted_type, max_studies=8, max_analyses=3):
    """Keep the analyses carrying ``wanted_type`` directly."""
    payload_studies = []
    for study in _all_studies():
        analyses = []
        for analysis in study.get("analyses") or []:
            types = {
                normalize_value_type(image.get("value_type"))
                for image in analysis.get("images") or []
            }
            if wanted_type in types:
                analyses.append(analysis)
        if not analyses:
            continue
        payload_studies.append(
            {
                "id": study["id"],
                "name": study.get("name"),
                "authors": study.get("authors"),
                "publication": study.get("publication"),
                "metadata": study.get("metadata") or {},
                "analyses": [
                    {
                        "id": a["id"],
                        "name": a.get("name"),
                        "conditions": a.get("conditions") or [],
                        "weights": a.get("weights") or [],
                        "images": a.get("images") or [],
                        "points": a.get("points") or [],
                        "metadata": a.get("metadata") or {},
                    }
                    for a in analyses[:max_analyses]
                ],
            }
        )
        if len(payload_studies) >= max_studies:
            break
    return {
        "id": f"studyset-{name}",
        "name": f"staging {wanted_type}-map corpus",
        "studies": payload_studies,
    }


def write(slug, studyset):
    out = BUNDLES / slug
    out.mkdir(parents=True, exist_ok=True)
    (out / "studyset.json").write_text(json.dumps(studyset, indent=1))
    for kind in ("boolean", "boolean_split", "string"):
        for tag, sample_size in (("noted", 25), ("real", "real"), ("none", None)):
            (out / f"annotation_{kind}_{tag}.json").write_text(
                json.dumps(
                    build_annotation(studyset, kind=kind, sample_size=sample_size),
                    indent=1,
                )
            )
    print(slug, json.dumps(describe(studyset)))


def main():
    summary = json.loads((BUNDLES / "summary.json").read_text())
    for slug, image_type in (
        ("corpus_z", "z"),
        ("corpus_t", "t"),
        ("corpus_beta", "beta"),
    ):
        studyset = corpus_studyset(slug, image_type)
        write(slug, studyset)
        summary[slug] = describe(studyset)

    # Two studies only: the smallest studyset a user can submit, and the one a
    # leave-one-out diagnostic has least to work with.
    tiny = corpus_studyset("tiny", "z", max_studies=2, max_analyses=1)
    tiny["id"] = "studyset-tiny"
    tiny["name"] = "staging two-study z corpus"
    write("tiny", tiny)
    summary["tiny"] = describe(tiny)

    (BUNDLES / "summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
