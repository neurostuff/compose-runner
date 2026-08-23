"""Roll the matrix's per-case records up into one table, grouped by failure."""

import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
rows = []
for record_path in sorted((HERE / "runs").glob("*/result.json")):
    data = json.loads(record_path.read_text())
    rows.append((record_path.parent.name, data))

ok = [r for r in rows if r[1].get("status") == "ok"]
bad = [r for r in rows if r[1].get("status") != "ok"]
print(f"{len(rows)} cases: {len(ok)} ok, {len(bad)} failed\n")

print("== succeeded ==")
for name, data in ok:
    coverage = data.get("coverage") or {}
    dependence = data.get("dependence") or {}
    print(
        f"{name:<62} fitted={data.get('n_images_fitted'):>3} "
        f"cov={coverage.get('included')}/{coverage.get('analyses')} "
        f"groups={dependence.get('n_groups')} dep={dependence.get('has_dependence')}"
    )

print("\n== failed, grouped ==")
grouped = defaultdict(list)
for name, data in bad:
    key = (data.get("error_type"), (data.get("error") or "").split("\n")[0][:110])
    grouped[key].append((name, data))
for (error_type, message), group in sorted(grouped.items(), key=lambda kv: -len(kv[1])):
    print(f"\n[{len(group)}x] {error_type}: {message}")
    for name, data in group:
        frames = data.get("frames") or []
        where = next(
            (f for f in reversed(frames) if "compose_runner" in f or "run.py" in f),
            frames[-1] if frames else "",
        )
        print(f"    {name}   <- {where}")
