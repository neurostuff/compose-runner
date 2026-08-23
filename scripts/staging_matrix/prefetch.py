"""Warm the shared image cache before the matrix fans out.

Also the cleanest place to see what the corpus loses on the way in: two
processes downloading the same map would otherwise race on the same partial
file, and the per-case reports would mix that up with real drops.
"""

import json
import logging
import sys
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from compose_runner.images import download_studyset_images  # noqa: E402

logging.basicConfig(level=logging.WARNING)
HERE = Path(__file__).resolve().parent
report = {}
for studyset_path in sorted((HERE / "bundles").glob("*/studyset.json")):
    studyset = json.loads(studyset_path.read_text())
    _, dropped = download_studyset_images(deepcopy(studyset), HERE / "images")
    slug = studyset_path.parent.name
    report[slug] = {
        "analyses_with_drops": len(dropped),
        "dropped": sorted({why for reasons in dropped.values() for why in reasons}),
        "n_dropped": sum(len(v) for v in dropped.values()),
    }
    print(slug, json.dumps(report[slug]), flush=True)
(HERE / "prefetch_report.json").write_text(json.dumps(report, indent=1))
