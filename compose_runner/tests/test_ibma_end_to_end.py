"""End-to-end IBMA runs through the Runner, using real NIfTI images.

These exercise the whole image-based path -- staging images, filling in sample
sizes, keeping a study's analyses separate, and dispatching to IBMAWorkflow --
against locally generated maps, so no network or cassette is involved.
"""

import nibabel as nib
import numpy as np
import pytest

from compose_runner.run import Runner

SHAPE = (6, 7, 6)
AFFINE = np.eye(4) * 2
AFFINE[3, 3] = 1


def _write_map(path, rng, offset=0.0, scale=1.0):
    """Write a small NIfTI with a real signal blob in the middle."""
    data = rng.normal(offset, scale, size=SHAPE).astype(np.float32)
    data[2:4, 3:5, 2:4] += 3.0  # a consistent effect across studies
    nib.save(nib.Nifti1Image(data, AFFINE), str(path))
    return path


def _make_studyset(tmp_path, n_studies=6, contrasts_for_first=2, value_type="Z map"):
    """Build a NIMADS studyset backed by real files on disk.

    The first study contributes several analyses, so the dependence handling
    has something to correct for.
    """
    rng = np.random.RandomState(0)
    image_dir = tmp_path / "source_images"
    image_dir.mkdir(parents=True, exist_ok=True)

    studies = []
    for i_study in range(n_studies):
        n_analyses = contrasts_for_first if i_study == 0 else 1
        analyses = []
        for i_analysis in range(n_analyses):
            name = f"s{i_study}a{i_analysis}"
            path = _write_map(image_dir / f"{name}.nii.gz", rng)
            analyses.append(
                {
                    "id": f"analysis{name}",
                    "name": name,
                    "metadata": {"sample_size": 20 + i_study},
                    "images": [
                        {
                            "id": f"image{name}",
                            "value_type": value_type,
                            "filename": str(path),
                            "url": f"http://neurovault.org/images/{i_study}{i_analysis}/",
                            "space": "MNI",
                        }
                    ],
                    "points": [],
                    "conditions": [],
                    "weights": [],
                }
            )
        studies.append(
            {
                "id": f"study{i_study}",
                "name": f"study {i_study}",
                "metadata": {},
                "analyses": analyses,
            }
        )

    return {"id": "studyset1", "name": "test", "studies": studies}


def _make_annotation(studyset):
    """Include every analysis, and record its sample size on the note."""
    notes = []
    for study in studyset["studies"]:
        for analysis in study["analyses"]:
            notes.append(
                {
                    "study": study["id"],
                    "analysis": analysis["id"],
                    "note": {"include": True, "sample_size": 25},
                }
            )
    return {
        "id": "annotation1",
        "name": "inclusion",
        "note_keys": {"include": "boolean", "sample_size": "number"},
        "notes": notes,
    }


def _prepared_runner(
    tmp_path,
    estimator_type,
    value_type="Z map",
    estimator_args=None,
    corrector=None,
):
    """Build a Runner with its bundle already populated, skipping the API."""
    runner = Runner(
        meta_analysis_id="meta-id",
        environment="production",
        result_dir=tmp_path / "results",
    )
    studyset = _make_studyset(tmp_path, value_type=value_type)
    runner.cached_studyset = studyset
    runner.cached_annotation = _make_annotation(studyset)
    runner.cached_specification = {
        "type": "IBMA",
        "estimator": {"type": estimator_type, "args": estimator_args or {}},
        "corrector": corrector,
        "filter": "include",
        "conditions": [True],
        "weights": [1],
    }
    return runner


@pytest.mark.parametrize("estimator_type", ["Fishers", "Stouffers"])
def test_ibma_runs_end_to_end(tmp_path, estimator_type):
    """A z-map IBMA should run through to finite results.

    Before this work, every image-based estimator hit a ValueError in
    run_meta_analysis, and the images were never fetched at all.
    """
    runner = _prepared_runner(tmp_path, estimator_type)

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.meta_results is not None
    assert "z" in runner.meta_results.maps
    assert np.isfinite(runner.meta_results.maps["z"]).any()


def test_ibma_stages_images_locally(tmp_path):
    """The studyset must end up pointing at files nibabel can open."""
    runner = _prepared_runner(tmp_path, "Fishers")

    runner.process_bundle()

    for study in runner.cached_studyset["studies"]:
        for analysis in study["analyses"]:
            for image in analysis["images"]:
                assert nib.load(image["filename"]) is not None


def test_ibma_keeps_a_studys_analyses_separate(tmp_path):
    """Merging would discard the first study's second contrast."""
    runner = _prepared_runner(tmp_path, "Fishers")

    runner.process_bundle()

    n_analyses = sum(
        len(study["analyses"]) for study in runner.cached_studyset["studies"]
    )
    # 6 studies, the first contributing 2 analyses.
    assert n_analyses == 7


def test_ibma_fills_in_sample_sizes(tmp_path):
    """Sample sizes come from the annotation, in the shape NiMARE wants."""
    runner = _prepared_runner(tmp_path, "Fishers")

    runner.process_bundle()

    for study in runner.cached_studyset["studies"]:
        for analysis in study["analyses"]:
            assert analysis["metadata"]["sample_sizes"] == [25.0]


def test_ibma_dependence_correction_engages(tmp_path):
    """The first study contributes two images, so the correction should fire."""
    runner = _prepared_runner(tmp_path, "Stouffers")

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.estimator.inputs_["corr_matrix"] is not None


def test_ibma_dependence_can_be_opted_out(tmp_path):
    """groupby=False has to reach NiMARE and actually disable the correction."""
    runner = _prepared_runner(tmp_path, "Stouffers", estimator_args={"groupby": False})

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.estimator.groupby is False
    # Every image is its own group, so there is nothing to correct for.
    assert runner.estimator._dependence().labels is None


@pytest.mark.parametrize(
    "estimator_args",
    [
        {"dependance": False},  # a typo
        {"dependence": "independent"},  # the name NiMARE used to have
    ],
)
def test_unknown_estimator_arg_is_rejected(tmp_path, estimator_args):
    """A stale or misspelled argument must fail before any images are fetched.

    NiMARE raises for these too, but only once the estimator is constructed,
    which is after the studyset's maps have been downloaded.
    """
    runner = _prepared_runner(tmp_path, "Stouffers", estimator_args=estimator_args)

    with pytest.raises(ValueError, match="does not accept"):
        runner.process_bundle()


def test_n_cores_does_not_reach_an_ibma_estimator(tmp_path):
    """No IBMA estimator takes n_cores; passing it would just be ignored."""
    runner = _prepared_runner(tmp_path, "Stouffers")

    runner.process_bundle(n_cores=2)

    assert not hasattr(runner.estimator, "n_cores")


def test_n_cores_becomes_n_jobs_for_permuted_ols(tmp_path):
    """PermutedOLS is the one image-based estimator that parallelizes."""
    runner = _prepared_runner(
        tmp_path,
        "PermutedOLS",
        value_type="univariate-beta map",
        corrector={
            "type": "FWECorrector",
            "args": {"method": "montecarlo", "n_iters": 10},
        },
    )

    runner.process_bundle(n_cores=2)

    assert runner.estimator.n_jobs == 2


def test_ibma_dependence_changes_the_result(tmp_path):
    """Correcting for dependence must actually move the statistics."""
    corrected = _prepared_runner(tmp_path / "corrected", "Stouffers")
    corrected.process_bundle()
    corrected.run_meta_analysis()

    naive = _prepared_runner(
        tmp_path / "naive", "Stouffers", estimator_args={"groupby": False}
    )
    naive.process_bundle()
    naive.run_meta_analysis()

    corrected_z = corrected.meta_results.maps["z"]
    naive_z = naive.meta_results.maps["z"]
    valid = np.isfinite(corrected_z) & np.isfinite(naive_z)

    assert valid.any()
    assert not np.allclose(corrected_z[valid], naive_z[valid])


def test_permuted_ols_runs_end_to_end(tmp_path):
    """PermutedOLS routes its blocks through nilearn's exchangeability support.

    It is paired with montecarlo FWE because that is its own correction path.
    NiMARE's default corrector for an IBMA workflow is FDR, which needs a ``p``
    map, and PermutedOLS emits only ``t``, ``z`` and ``dof`` -- so leaving the
    corrector unset raises. That is upstream behaviour, not something this
    runner can paper over.
    """
    runner = _prepared_runner(
        tmp_path,
        "PermutedOLS",
        value_type="univariate-beta map",
        corrector={
            "type": "FWECorrector",
            "args": {"method": "montecarlo", "n_iters": 10},
        },
    )

    runner.process_bundle()
    runner.run_meta_analysis()

    assert np.isfinite(runner.meta_results.maps["z"]).any()


def test_permuted_ols_uses_exchangeability_blocks(tmp_path):
    """The study grouping must reach nilearn's permuted_ols."""
    runner = _prepared_runner(
        tmp_path,
        "PermutedOLS",
        value_type="univariate-beta map",
        corrector={
            "type": "FWECorrector",
            "args": {"method": "montecarlo", "n_iters": 10},
        },
    )

    runner.process_bundle()
    runner.run_meta_analysis()

    blocks = runner.estimator._dependence().blocks
    # The first study contributes two images, so there are fewer blocks than
    # images and the sign flips are drawn per study rather than per image.
    assert np.unique(blocks).size < len(blocks)


def test_cbma_specification_still_combines_analyses(tmp_path):
    """A coordinate-based specification must keep its existing behaviour."""
    runner = _prepared_runner(tmp_path, "Fishers")
    runner.cached_specification["type"] = "CBMA"

    assert runner._is_image_based() is False


def _messy_studyset(tmp_path):
    """A studyset of the shape real compose input has.

    Every row is a case the runner has to survive: a study that uploaded what
    the estimator wants, one that needs a transform, one that cannot be
    transformed, one whose only map NiMARE has no use for, and one contributing
    two contrasts.

    The ids deliberately contain no hyphens. ``Studyset.slice`` composes a full
    id as ``<study>-<analysis>`` and splits on the hyphen to recover the parts,
    so a hyphenated id silently matches nothing and the filtered studyset comes
    back empty. Real Neurostore ids are hyphen-free base62, so this only bites
    synthetic data -- but it bites hard, and quietly.
    """
    rng = np.random.RandomState(0)
    image_dir = tmp_path / "source"
    image_dir.mkdir(parents=True, exist_ok=True)

    def image(name, value_type):
        path = _write_map(image_dir / f"{name}.nii.gz", rng)
        return {
            "id": f"image-{name}",
            "value_type": value_type,
            "filename": str(path),
            "url": str(path),
            "space": "MNI",
        }

    def analysis(analysis_id, images, sample_size=None):
        return {
            "id": analysis_id,
            "name": analysis_id,
            "conditions": [],
            "weights": [],
            "points": [],
            "metadata": {"sample_size": sample_size} if sample_size else {},
            "images": images,
        }

    studies = [
        # Has the z-map outright.
        {
            "id": "ready",
            "name": "ready",
            "metadata": {},
            "analyses": [analysis("readyA", [image("ready", "Z map")])],
        },
        # t-map plus a sample size: NiMARE derives the z-map.
        {
            "id": "derivable",
            "name": "derivable",
            "metadata": {},
            "analyses": [
                analysis("derivableA", [image("derivable", "T map")], sample_size=24)
            ],
        },
        # t-map with no sample size: nothing to derive from.
        {
            "id": "noN",
            "name": "noN",
            "metadata": {},
            "analyses": [analysis("noNA", [image("noN", "T map")])],
        },
        # Only a map NiMARE cannot use at all.
        {
            "id": "maskOnly",
            "name": "maskOnly",
            "metadata": {},
            "analyses": [analysis("maskOnlyA", [image("mask", "ROI/mask")])],
        },
        # Two contrasts, one usable and one not.
        {
            "id": "mixed",
            "name": "mixed",
            "metadata": {},
            "analyses": [
                analysis("mixedA", [image("mixedA", "Z map")]),
                analysis("mixedB", [image("mixedB", "univariate-beta map")]),
            ],
        },
    ]
    return {"id": "messy", "name": "messy", "studies": studies}


def _messy_runner(tmp_path, estimator_type="Stouffers"):
    runner = Runner(
        meta_analysis_id="meta-id",
        environment="production",
        result_dir=tmp_path / "results",
    )
    studyset = _messy_studyset(tmp_path)
    runner.cached_studyset = studyset
    runner.cached_annotation = _make_annotation(studyset)
    runner.cached_specification = {
        "type": "IBMA",
        "estimator": {"type": estimator_type, "args": {"aggressive_mask": True}},
        "corrector": {"type": "FDRCorrector", "args": {"method": "indep"}},
        "filter": "include",
        "conditions": [True],
        "weights": [1],
    }
    return runner


def test_messy_studyset_still_runs(tmp_path):
    """Partial coverage is normal input, not a failure."""
    runner = _messy_runner(tmp_path)

    runner.process_bundle()
    runner.run_meta_analysis()

    assert np.isfinite(runner.meta_results.maps["z"]).any()


def test_messy_studyset_reports_what_it_could_and_could_not_use(tmp_path):
    """The account has to distinguish supplied, converted, and impossible.

    Read after the fit, not before it: the workflow converts and drops on its
    own, and the report is an introspection of what it did.
    """
    runner = _messy_runner(tmp_path)

    runner.process_bundle()
    runner.run_meta_analysis()
    report = runner.coverage_report

    by_analysis = {a.analysis_id: a for a in report.analyses}
    assert by_analysis["readyA"].included
    assert by_analysis["readyA"].derived == frozenset()
    # The annotation carries a sample size for every analysis, so NiMARE can
    # convert the t-map study.
    assert by_analysis["derivableA"].included
    assert by_analysis["derivableA"].derived == {"z"}
    # No usable map at all, so nothing to transform from.
    assert not by_analysis["maskOnlyA"].included
    assert "ROI/mask" in by_analysis["maskOnlyA"].reason
    # A beta map cannot become a z-map on its own.
    assert not by_analysis["mixedB"].included
    assert "could not produce z" in by_analysis["mixedB"].reason


def test_messy_studyset_writes_a_coverage_table(tmp_path):
    """The account has to outlive the log line."""
    runner = _messy_runner(tmp_path)

    runner.process_bundle()
    runner.run_meta_analysis()

    table = (runner.result_dir / "ibma_coverage.tsv").read_text().strip().splitlines()
    assert table[0].split("\t")[:5] == [
        "study_id",
        "study_name",
        "analysis_id",
        "analysis_name",
        "included",
    ]
    assert len(table) == 1 + len(runner.coverage_report.analyses)


def test_nothing_usable_fails_with_the_account_attached(tmp_path):
    """NiMARE's message alone does not say which studies were unusable.

    When nothing survives there is no fitted estimator to introspect, so the
    submitted studyset is described instead and the account is appended to the
    error NiMARE raised.
    """
    runner = _messy_runner(tmp_path, estimator_type="SampleSizeBasedLikelihood")
    # Leave only the study whose single map NiMARE cannot use. The annotation is
    # rebuilt too, because it references analyses that no longer exist.
    runner.cached_studyset["studies"] = [
        s for s in runner.cached_studyset["studies"] if s["id"] == "maskOnly"
    ]
    runner.cached_annotation = _make_annotation(runner.cached_studyset)
    runner.process_bundle()

    with pytest.raises(ValueError) as excinfo:
        runner.run_meta_analysis()

    message = str(excinfo.value)
    assert "Used 0 of 1 analyses" in message
    assert "ROI/mask" in message
    # The table is written even though the run failed.
    assert (runner.result_dir / "ibma_coverage.tsv").is_file()


@pytest.mark.parametrize("fill", [0.0, np.nan], ids=["all-zero", "all-nan"])
def test_an_all_nan_result_is_rejected(tmp_path, fill):
    """A run that computes nothing must not pass as a success.

    aggressive_mask=True keeps only voxels valid in every input map, so one
    degenerate map empties the mask however good the others are. The workflow
    still finishes and writes maps -- entirely NaN ones, which would then be
    uploaded to NeuroVault as a result.

    Both fills are degenerate to NiMARE, which counts a voxel as valid only
    where it is finite *and* non-zero. The all-zero case is the one seen in real
    data: an empty NeuroVault upload. The message has to name the map, because
    "set aggressive_mask=False" is not the fix when a study uploaded an empty
    file.
    """
    runner = _prepared_runner(
        tmp_path, "Stouffers", estimator_args={"aggressive_mask": True}
    )
    studyset = runner.cached_studyset
    degenerate = tmp_path / "degenerate.nii.gz"
    nib.save(
        nib.Nifti1Image(np.full(SHAPE, fill, dtype=np.float32), AFFINE), str(degenerate)
    )
    studyset["studies"][0]["analyses"][0]["images"][0]["filename"] = str(degenerate)
    studyset["studies"][0]["analyses"][0]["images"][0]["url"] = str(degenerate)

    runner.process_bundle()
    with pytest.raises(ValueError) as excinfo:
        runner.run_meta_analysis()

    message = str(excinfo.value)
    assert "no value at any voxel" in message
    assert "no finite non-zero voxel" in message
