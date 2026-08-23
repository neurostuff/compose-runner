"""Minimal NiMARE-only reproductions for the asks in docs/nimare-asks.md.

No compose-runner anywhere: synthetic NIfTIs, a NIMADS studyset dict and the
public API, so each one can be pasted into a NiMARE issue as it stands.

Usage::

    python scripts/nimare_asks_repro.py
"""

import logging
import sys
import tempfile
import warnings
from pathlib import Path

import nibabel as nib
import numpy as np

from nimare.correct import FWECorrector
from nimare.meta.ibma import PermutedOLS, Stouffers
from nimare.nimads import Studyset
from nimare.transforms import ImageTransformer

SHAPE = (6, 7, 6)
AFFINE = np.eye(4) * 2
AFFINE[3, 3] = 1
TMP = Path(tempfile.mkdtemp(prefix="nimare-asks-"))


class Collect(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages = []

    def emit(self, record):
        self.messages.append(f"{record.name}: {record.getMessage()}")


def _write(name, data):
    path = TMP / f"{name}.nii.gz"
    nib.save(nib.Nifti1Image(np.asarray(data, dtype=np.float32), AFFINE), str(path))
    return str(path)


def _map(rng, offset=0.0):
    data = rng.normal(offset, 1.0, size=SHAPE)
    data[2:4, 3:5, 2:4] += 3.0
    return data


def studyset(analyses):
    """analyses: list of (study_id, analysis_id, value_type, path)."""
    studies = {}
    for study_id, analysis_id, value_type, path in analyses:
        studies.setdefault(study_id, []).append((analysis_id, value_type, path))
    payload = {
        "id": "ss",
        "name": "ss",
        "studies": [
            {
                "id": study_id,
                "name": study_id,
                "metadata": {},
                "analyses": [
                    {
                        "id": analysis_id,
                        "name": analysis_id,
                        "metadata": {"sample_sizes": [20]},
                        "images": [
                            {
                                "id": f"{analysis_id}-img",
                                "value_type": value_type,
                                "filename": path,
                                "url": path,
                                "space": "MNI",
                            }
                        ],
                        "points": [],
                        "conditions": [],
                        "weights": [],
                    }
                    for analysis_id, value_type, path in entries
                ],
            }
            for study_id, entries in studies.items()
        ],
    }
    return Studyset(payload, target="mni152_2mm")


def section(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def repro_unsigned_p():
    section("A. z derived from a p map is unsigned, and nothing says so")
    rng = np.random.RandomState(0)
    entries = []
    for i in range(4):
        entries.append((f"s{i}", f"a{i}", "Z map", _write(f"z{i}", _map(rng))))
    # One study contributes only a p map.
    p_values = np.clip(np.abs(rng.normal(0, 0.3, size=SHAPE)), 1e-6, 1.0)
    entries.append(
        ("s4", "a4", "P map (given null hypothesis)", _write("p4", p_values))
    )

    collector = Collect()
    logging.getLogger("nimare").addHandler(collector)
    estimator = Stouffers()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # What IBMAWorkflow.fit does before handing the studyset to the estimator.
        transformed = ImageTransformer(target="z").transform(studyset(entries))
        estimator.fit(transformed)
    logging.getLogger("nimare").removeHandler(collector)

    ids = [str(i) for i in estimator.inputs_["id"]]
    z = np.asarray(estimator.inputs_["z_maps"], dtype=float)
    for row, image_id in enumerate(ids):
        values = z[row][np.isfinite(z[row]) & (z[row] != 0)]
        marker = "  <-- derived from p" if image_id == "s4-a4" else ""
        print(
            f"  {image_id}: min {values.min():+.3f} max {values.max():+.3f} "
            f"negative {100 * (values < 0).mean():5.1f}%{marker}"
        )
    signed = [m for m in collector.messages if "sign" in m.lower()]
    print(f"  warnings mentioning sign: {signed or 'none'}")
    print(f"  python warnings          : {[str(w.message) for w in caught] or 'none'}")


def repro_empty_image():
    section("B. An image with no valid voxel is kept, and never named")

    def build(tag):
        rng = np.random.RandomState(1)
        return [
            ("s0", "a0", "Z map", _write(f"{tag}_z0", _map(rng))),
            ("s0", "a1", "Z map", _write(f"{tag}_z1", _map(rng))),
            # Same study, an entirely zero upload.
            ("s0", "a2", "Z map", _write(f"{tag}_zero", np.zeros(SHAPE))),
            ("s1", "a3", "Z map", _write(f"{tag}_z3", _map(rng))),
            ("s2", "a4", "Z map", _write(f"{tag}_z4", _map(rng))),
        ]

    collector = Collect()
    logging.getLogger("nimare").addHandler(collector)
    estimator = Stouffers()
    result = estimator.fit(studyset(build("lib")))
    logging.getLogger("nimare").removeHandler(collector)

    ids = [str(i) for i in estimator.inputs_["id"]]
    z = np.asarray(estimator.inputs_["z_maps"], dtype=float)
    valid = np.isfinite(z) & (z != 0)
    print(f"  inputs_['id']               : {ids}")
    print(f"  per-image valid voxel counts: {dict(zip(ids, valid.sum(1).tolist()))}")
    print(f"  contrast_names              : {estimator.inputs_['contrast_names']}")
    print(
        f"  warnings about them         : "
        f"{[m for m in collector.messages if 'voxel' in m.lower()] or 'none'}"
    )

    # study_mask is an index array, not a boolean mask.
    bag = estimator.inputs_["data_bags"]["z_maps"][0]
    print(
        f"\n  aggressive_mask=False: bag study_mask {bag['study_mask']} "
        f"-- index 2 absent, so the empty image is in no bag"
    )
    print(f"                         finite voxels per output map: {_finite(result)}")

    collector = Collect()
    logging.getLogger("nimare").addHandler(collector)
    aggressive = Stouffers(aggressive_mask=True).fit(studyset(build("agg")))
    logging.getLogger("nimare").removeHandler(collector)
    print(f"  aggressive_mask=True : {collector.messages or 'no warnings'}")
    print(
        f"                         finite voxels per output map: {_finite(aggressive)}"
    )


def _finite(result):
    return {
        name: int(np.isfinite(np.asarray(values, dtype=float)).sum())
        for name, values in result.maps.items()
        if values is not None and not name.startswith("label_")
    }


def repro_single_analysis():
    section(
        "C. A meta-analysis of one analysis returns all-NaN maps, and does not say so"
    )
    rng = np.random.RandomState(2)
    entries = [("s0", "a0", "Z map", _write("one_z", _map(rng)))]
    collector = Collect()
    logging.getLogger("nimare").addHandler(collector)
    estimator = Stouffers()
    try:
        result = estimator.fit(studyset(entries))
        finite = {
            name: int(np.isfinite(np.asarray(values, dtype=float)).sum())
            for name, values in result.maps.items()
            if values is not None and not name.startswith("label_")
        }
        print(f"  fit succeeded, inputs: {len(estimator.inputs_['id'])}")
        print(f"  finite voxels per map: {finite}")
    except Exception as exc:  # noqa: BLE001
        print(f"  {type(exc).__name__}: {exc}")
    logging.getLogger("nimare").removeHandler(collector)
    print(f"  warnings             : {collector.messages or 'none'}")


def repro_cancelling_group():
    section("D. A cancelling dependence group is named by an integer code")
    rng = np.random.RandomState(7)
    mirrored = rng.normal(0, 1.0, size=SHAPE)
    entries = [
        ("s0", "a0", "Z map", _write("c_z0", mirrored)),
        ("s0", "a1", "Z map", _write("c_z1", -mirrored)),
        ("s1", "a2", "Z map", _write("c_z2", _map(rng))),
        ("s2", "a3", "Z map", _write("c_z3", _map(rng))),
        ("s3", "a4", "Z map", _write("c_z4", _map(rng))),
    ]
    try:
        Stouffers().fit(studyset(entries))
        print("  fit succeeded (no cancellation)")
    except Exception as exc:  # noqa: BLE001
        print(f"  {type(exc).__name__}: {exc}")
        print("  the studies are s0, s1, s2, s3; 'Group 0' is an internal code")

    # The same pair, but sharing signal: the estimated null correlation lands at
    # -0.937 instead of -1, the block sums to 0.125 rather than 0, and the guard
    # never fires -- so that group's z is inflated ~5.7x instead.
    rng = np.random.RandomState(3)
    shared = _map(rng)
    near = [
        ("s0", "a0", "Z map", _write("n_z0", shared)),
        ("s0", "a1", "Z map", _write("n_z1", -shared)),
        ("s1", "a2", "Z map", _write("n_z2", _map(rng))),
        ("s2", "a3", "Z map", _write("n_z3", _map(rng))),
        ("s3", "a4", "Z map", _write("n_z4", _map(rng))),
    ]
    estimator = Stouffers()
    estimator.fit(studyset(near))
    corr = np.asarray(estimator.inputs_["corr_matrix"])
    block = corr[np.ix_([0, 1], [0, 1])]
    z = np.asarray(estimator.inputs_["z_maps"], dtype=float)
    print(
        f"  near-cancellation: empirical corr {np.corrcoef(z[:2])[0, 1]:+.4f}, "
        f"estimated {corr[0, 1]:+.4f}"
    )
    print(
        f"  block variance {block.sum() / 4:.4f} -> no error, group z scaled by "
        f"{1 / np.sqrt(block.sum() / 4):.1f}x"
    )


def repro_fwe_kwargs():
    section("E. An unset voxel_thresh reaches the estimator's correction method")
    rng = np.random.RandomState(4)
    entries = [
        (f"s{i}", f"a{i}", "univariate-beta map", _write(f"b{i}", _map(rng)))
        for i in range(5)
    ]
    corrector = FWECorrector(method="montecarlo", n_iters=5, voxel_thresh=None)
    print(f"  FWECorrector.parameters: {corrector.parameters}")
    estimator = PermutedOLS()
    result = estimator.fit(studyset(entries))
    try:
        corrector.transform(result)
        print("  PermutedOLS: corrected")
    except Exception as exc:  # noqa: BLE001
        print(f"  PermutedOLS -> {type(exc).__name__}: {exc}")
    print(
        "  ALE.correct_fwe_montecarlo defaults voxel_thresh to 0.001; "
        "passing None reaches _p_to_summarystat(None)."
    )


def repro_per_bag_membership():
    section("F. Which images a group holds differs per liberal-mask bag")
    rng = np.random.RandomState(11)

    # s0 contributes an exact mirror pair, but the second map is valid over only
    # part of the volume -- what a thresholded upload looks like. So s0's group
    # holds two images in one bag and one in the other.
    half = np.zeros(SHAPE, dtype=bool)
    half[:5] = True
    mirror = rng.normal(0, 1.0, size=SHAPE)
    partial = -mirror.copy()
    partial[~half] = 0.0

    entries = [
        ("s0", "a0", "Z map", _write("pb_z0", mirror)),
        ("s0", "a1", "Z map", _write("pb_z1", partial)),
        ("s1", "a2", "Z map", _write("pb_z2", rng.normal(0, 1, SHAPE))),
        ("s2", "a3", "Z map", _write("pb_z3", rng.normal(0, 1, SHAPE))),
        ("s3", "a4", "Z map", _write("pb_z4", rng.normal(0, 1, SHAPE))),
    ]
    estimator = Stouffers()
    estimator.fit(studyset(entries))

    ids = [str(i) for i in estimator.inputs_["id"]]
    codes = estimator.inputs_["contrast_names"]
    corr = np.asarray(estimator.inputs_["corr_matrix"])
    print(f"  ids   : {ids}")
    print(f"  codes : {codes}\n")
    print(f"  {'bag':>3} {'voxels':>7}  {'members':<30} per-group block variance")

    # Exactly what _fit_model already builds: corr[np.ix_(study_mask, study_mask)],
    # then PyMARE's block.sum() / size ** 2 per group.
    for i, bag in enumerate(estimator.inputs_["data_bags"]["z_maps"]):
        mask = bag["study_mask"]
        verdicts = []
        for code in dict.fromkeys(codes[mask]):
            rows = mask[codes[mask] == code]
            block = corr[np.ix_(rows, rows)]
            variance = block.sum() / len(rows) ** 2
            verdicts.append(
                f"g{code}={variance:.3g}"
                + ("  <-- cancels" if variance <= 1e-12 else "")
            )
        members = ",".join(ids[j] for j in mask)
        print(
            f"  {i:>3} {bag['values'].shape[1]:>7}  {members:<30} {'  '.join(verdicts)}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
    print(f"maps in {TMP}")
    repro_unsigned_p()
    repro_empty_image()
    repro_single_analysis()
    repro_cancelling_group()
    repro_fwe_kwargs()
    repro_per_bag_membership()
