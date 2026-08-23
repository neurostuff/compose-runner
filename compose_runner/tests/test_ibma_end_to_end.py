"""End-to-end IBMA runs through the Runner, over locally generated NIfTIs."""

from copy import deepcopy

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

    The first study contributes two analyses, so the dependence handling has
    something to correct for.
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
    runner = _prepared_runner(tmp_path, estimator_type)

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.meta_results is not None
    assert "z" in runner.meta_results.maps
    assert np.isfinite(runner.meta_results.maps["z"]).any()


def test_ibma_stages_a_studyset_nimare_can_read(tmp_path):
    """Local paths, annotation sample sizes, and no analyses merged away."""
    runner = _prepared_runner(tmp_path, "Fishers")

    runner.process_bundle()

    analyses = [
        analysis
        for study in runner.staged_studyset["studies"]
        for analysis in study["analyses"]
    ]
    # 6 studies, the first contributing 2 analyses.
    assert len(analyses) == 7
    for analysis in analyses:
        assert analysis["metadata"]["sample_sizes"] == [25.0]
        for image in analysis["images"]:
            assert nib.load(image["filename"]) is not None


def test_staging_leaves_the_studyset_snapshot_alone(tmp_path):
    """cached_studyset is uploaded as the result's snapshot.

    Staging rewrites locations to local paths and drops maps NiMARE cannot use,
    neither of which belongs in a record of what Neurostore served.
    """
    runner = _prepared_runner(tmp_path, "Fishers")
    before = deepcopy(runner.cached_studyset)

    runner.process_bundle()

    assert runner.cached_studyset == before


def test_ibma_dependence_correction_engages(tmp_path):
    """The first study contributes two images, so the correction should fire."""
    runner = _prepared_runner(tmp_path, "Stouffers")

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.estimator.inputs_["corr_matrix"] is not None


def test_estimator_args_reach_nimare(tmp_path):
    """groupby=False has to arrive and actually disable the correction."""
    runner = _prepared_runner(tmp_path, "Stouffers", estimator_args={"groupby": False})

    runner.process_bundle()
    runner.run_meta_analysis()

    assert runner.estimator.groupby is False
    # Every image is its own group, so there is nothing to correct for.
    assert runner.estimator._dependence().labels is None


def test_an_estimator_arg_nimare_rejects_fails_before_any_download(tmp_path):
    runner = _prepared_runner(
        tmp_path, "Stouffers", estimator_args={"dependance": False}
    )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        runner.process_bundle()

    assert not runner.image_dir.exists()


def test_n_cores_does_not_reach_an_ibma_estimator(tmp_path):
    """No IBMA estimator takes n_cores; NiMARE would reject it."""
    runner = _prepared_runner(tmp_path, "Stouffers")

    runner.process_bundle(n_cores=2)

    assert not hasattr(runner.estimator, "n_cores")


def test_permuted_ols_runs_end_to_end(tmp_path):
    """PermutedOLS takes n_cores as n_jobs and blocks by study.

    Paired with montecarlo FWE because that is its own correction path: it emits
    no ``p`` map, so the workflow's default FDR corrector cannot be used.
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

    runner.process_bundle(n_cores=2)
    runner.run_meta_analysis()

    assert runner.estimator.n_jobs == 2
    assert np.isfinite(runner.meta_results.maps["z"]).any()
    # The first study contributes two images, so the sign flips are drawn per
    # study rather than per image.
    blocks = runner.estimator._dependence().blocks
    assert np.unique(blocks).size < len(blocks)


def _messy_studyset(tmp_path):
    """A studyset of the shape real compose input has.

    Every row is a case the runner has to survive: a study that uploaded what
    the estimator wants, one that needs a transform, one that cannot be
    transformed, one whose only map NiMARE has no use for, and one contributing
    two contrasts.

    Ids carry no hyphen: ``Studyset.slice`` composes a full id as
    ``<study>-<analysis>`` and splits it back on the hyphen, so a hyphenated id
    resolves to nothing and the filtered studyset comes back empty.
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
    """The account has to distinguish supplied, converted, and impossible."""
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
    """NiMARE's message alone does not say which studies were unusable."""
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


def test_an_unreadable_upload_is_dropped_rather_than_failing_the_run(tmp_path):
    """A file that is not a NIfTI costs its own analysis, not the meta-analysis.

    It passes the magic-byte check that guards the download, so nothing notices
    until nibabel opens it from inside the fit, where it is neither caught nor
    attributed to the analysis it came from. A map that parses but holds no
    finite non-zero voxel is NiMARE's to drop, and is not covered here.
    """
    runner = _prepared_runner(tmp_path, "Stouffers")
    corrupt = tmp_path / "corrupt.nii.gz"
    corrupt.write_bytes(b"\x1f\x8b" + b"not actually a nifti" * 8)
    analysis = runner.cached_studyset["studies"][0]["analyses"][0]
    analysis["images"][0]["filename"] = str(corrupt)
    analysis["images"][0]["url"] = str(corrupt)

    runner.process_bundle()
    runner.run_meta_analysis()

    assert "could not be read as a NIfTI" in " ".join(
        runner.dropped_maps[analysis["id"]]
    )
    assert analysis["id"] in {a.analysis_id for a in runner.coverage_report.excluded}


def test_an_all_nan_result_is_rejected(tmp_path):
    """A run that computes nothing must not pass as a success.

    Every map here carries signal, so none is dropped on the way in; they simply
    do not overlap, which under ``aggressive_mask=True`` leaves the intersection
    mask empty and every output map NaN. The message has to say that, since the
    fix is the mask rather than the uploads.
    """
    runner = _prepared_runner(
        tmp_path, "Stouffers", estimator_args={"aggressive_mask": True}
    )
    # Each analysis keeps a different single voxel, so no voxel is valid in all.
    for i_study, study in enumerate(runner.cached_studyset["studies"]):
        for i_analysis, analysis in enumerate(study["analyses"]):
            data = np.zeros(SHAPE, dtype=np.float32)
            data[i_study % SHAPE[0], i_analysis, 0] = 3.0
            path = tmp_path / f"disjoint{i_study}{i_analysis}.nii.gz"
            nib.save(nib.Nifti1Image(data, AFFINE), str(path))
            analysis["images"][0]["filename"] = str(path)
            analysis["images"][0]["url"] = str(path)

    runner.process_bundle()
    with pytest.raises(ValueError) as excinfo:
        runner.run_meta_analysis()

    message = str(excinfo.value)
    assert "no value at any voxel" in message
    assert "aggressive_mask" in message
