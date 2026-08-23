"""Hand-roll NIMADS studysets + annotations out of staging Neurostore searches.

Nothing here uses compose: the point is to assemble the bundle a frontend would
POST, from real staging studies carrying real NeuroVault maps, so the runner can
be driven without a meta-analysis existing.
"""

import json
import sys
from pathlib import Path

import requests

STAGING = "https://staging.neurostore.xyz/api"
HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
BUNDLES = HERE / "bundles"

# One studyset per term. Chosen for a range of corpus sizes and provenances.
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
    "working memory",
    "semantic",
]

N_STUDIES = 6
MAX_ANALYSES = 3
MAX_VERSIONS = 4


def _get(session, path, **params):
    response = session.get(f"{STAGING}{path}", params=params, timeout=180)
    response.raise_for_status()
    return response.json()


def fetch_term(session, term):
    """Cache the studies behind one search, one version per base study."""
    cache = RAW / f"{term.replace(' ', '_')}.json"
    if cache.is_file():
        return json.loads(cache.read_text())

    payload = _get(
        session, "/base-studies/", search=term, data_type="image", page_size=100
    )
    studies = []
    for base_study in payload.get("results") or []:
        study = _first_version_with_images(session, base_study)
        if study is not None:
            studies.append(study)
        if len(studies) >= N_STUDIES:
            break

    cache.write_text(json.dumps(studies))
    return studies


def _first_version_with_images(session, base_study):
    for version in (base_study.get("versions") or [])[:MAX_VERSIONS]:
        version_id = version if isinstance(version, str) else version.get("id")
        if not version_id:
            continue
        try:
            study = _get(session, f"/studies/{version_id}", nested="true")
        except requests.HTTPError:
            continue
        if any(a.get("images") for a in study.get("analyses") or []):
            return study
    return None


def build_studyset(term, studies):
    """The NIMADS studyset a compose snapshot would hold.

    Analyses with no image at all are left out -- a compose user selecting for
    image data would not have them -- but nothing is filtered on *which* maps
    an analysis carries. That unevenness is the thing being tested.
    """
    payload_studies = []
    for study in studies:
        analyses = [a for a in study.get("analyses") or [] if a.get("images")]
        analyses = analyses[:MAX_ANALYSES]
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
                        "id": analysis["id"],
                        "name": analysis.get("name"),
                        "conditions": analysis.get("conditions") or [],
                        "weights": analysis.get("weights") or [],
                        "images": analysis.get("images") or [],
                        "points": analysis.get("points") or [],
                        "metadata": analysis.get("metadata") or {},
                    }
                    for analysis in analyses
                ],
            }
        )
    slug = term.replace(" ", "_")
    return {
        "id": f"studyset-{slug}",
        "name": f"staging {term} (data_type=image)",
        "studies": payload_studies,
    }


def build_annotation(studyset, kind="boolean", sample_size=None):
    """The annotation compose would attach.

    kind:
      ``boolean``       -- one ``included`` column, every analysis true.
      ``boolean_split`` -- ``included`` alternating true/false, for a two-group
                           selection.
      ``string``        -- an ``analysis_group`` column with two values, which
                           is the other shape ``apply_filter`` supports.
    sample_size:
      ``None``  -- no sample-size note; the runner falls back to metadata.
      a number  -- noted on every analysis.
      ``"real"``-- taken from the analysis's own metadata where it has one.
    """
    note_keys = {}
    if kind == "string":
        note_keys["analysis_group"] = "string"
    else:
        note_keys["included"] = "boolean"
    if sample_size is not None:
        note_keys["sample_size"] = "number"

    notes = []
    index = 0
    for study in studyset["studies"]:
        for analysis in study["analyses"]:
            note = {}
            if kind == "string":
                note["analysis_group"] = "left" if index % 2 == 0 else "right"
            elif kind == "boolean_split":
                note["included"] = index % 2 == 0
            else:
                note["included"] = True

            if sample_size == "real":
                found = _analysis_sample_size(study, analysis)
                if found is not None:
                    note["sample_size"] = found
            elif sample_size is not None:
                note["sample_size"] = sample_size

            notes.append(
                {"study": study["id"], "analysis": analysis["id"], "note": note}
            )
            index += 1

    return {
        "id": f"annotation-{studyset['id']}-{kind}",
        "name": f"{kind} annotation for {studyset['id']}",
        "note_keys": note_keys,
        "notes": notes,
    }


def _analysis_sample_size(study, analysis):
    for source in (analysis.get("metadata") or {}, study.get("metadata") or {}):
        for key in ("sample_sizes", "sample_size", "n"):
            value = source.get(key)
            if isinstance(value, (list, tuple)) and value:
                value = value[0]
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                return number
    return None


def describe(studyset):
    n_analyses = sum(len(s["analyses"]) for s in studyset["studies"])
    images = [
        image
        for s in studyset["studies"]
        for a in s["analyses"]
        for image in a["images"]
    ]
    value_types = {}
    for image in images:
        value_types[str(image.get("value_type"))] = (
            value_types.get(str(image.get("value_type")), 0) + 1
        )
    n_points = sum(len(a["points"]) for s in studyset["studies"] for a in s["analyses"])
    return {
        "studies": len(studyset["studies"]),
        "analyses": n_analyses,
        "images": len(images),
        "points": n_points,
        "value_types": dict(sorted(value_types.items(), key=lambda kv: -kv[1])),
        "multi_analysis_studies": sum(
            1 for s in studyset["studies"] if len(s["analyses"]) > 1
        ),
    }


def main():
    session = requests.Session()
    summary = {}
    for term in TERMS:
        studies = fetch_term(session, term)
        studyset = build_studyset(term, studies)
        if not studyset["studies"]:
            print(f"!! {term}: no study with images", file=sys.stderr)
            continue
        slug = term.replace(" ", "_")
        out = BUNDLES / slug
        out.mkdir(parents=True, exist_ok=True)
        (out / "studyset.json").write_text(json.dumps(studyset, indent=1))
        for kind in ("boolean", "boolean_split", "string"):
            for tag, sample_size in (
                ("noted", 25),
                ("real", "real"),
                ("none", None),
            ):
                annotation = build_annotation(
                    studyset, kind=kind, sample_size=sample_size
                )
                (out / f"annotation_{kind}_{tag}.json").write_text(
                    json.dumps(annotation, indent=1)
                )
        summary[slug] = describe(studyset)
        print(slug, json.dumps(summary[slug]))
    (BUNDLES / "summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
