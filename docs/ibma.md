# Image-based meta-analysis

Compose cannot yet create an image-based meta-analysis, so this describes the
bundle the frontend *will* produce, what this runner does with it, and what still
has to change upstream. `scripts/simulate_ibma_bundle.py` assembles such a bundle
out of real Neurostore studies and real NeuroVault maps and pushes it through
`Runner`.

`docs/staging-ibma-matrix.md` is what happened when that was done at scale: 16
studysets cut from staging against 30 specifications, the eleven bugs it found,
and what remains upstream.

## Versions this is written against

| Package | Version | Why |
| --- | --- | --- |
| NiMARE | `main`, pinned in `pyproject.toml` until 0.21.0 ships | `groupby`, IBMA dependence handling, `aggressive_mask=False` by default, `Stouffers` without `normalize_contrast_weights`, [#1102](https://github.com/neurostuff/NiMARE/pull/1102) for the jackknife, [#1099](https://github.com/neurostuff/NiMARE/pull/1099) for the `logp` maps, and [#1103](https://github.com/neurostuff/NiMARE/pull/1103) for the columnar studyset |
| PyMARE | 0.0.11rc1 | `Dataset(g=...)`, `encode_groups`, `group_mean`, `estimate_null_correlation`; required by NiMARE 0.21.0 |
| Neurostore | [PR #1695](https://github.com/neurostuff/neurostore/pull/1695) | the IBMA algorithm config and the `IBMA` specification type |

## What the bundle looks like

The studyset and annotation are the same shape as for a coordinate-based run.
Only the specification differs:

```json
{
  "type": "IBMA",
  "estimator": {
    "type": "Stouffers",
    "args": {
      "aggressive_mask": false,
      "use_sample_size": false,
      "two_sided": true,
      "**kwargs": {}
    }
  },
  "corrector": {"type": "FDRCorrector", "args": {"method": "indep", "alpha": 0.05}},
  "filter": "included",
  "conditions": [true],
  "weights": [1.0],
  "database_studyset": null
}
```

Two things about the studyset matter more for IBMA than for CBMA.

**An image's location is split across two fields, inconsistently.** Which of
`url` and `filename` holds the downloadable NIfTI depends on how the image
reached Neurostore, and both shapes are live in production:

| Provenance | `url` | `filename` |
| --- | --- | --- |
| Ingested from NeuroVault | `https://neurovault.org/media/images/4248/spmT_0001_2.nii.gz` | `spmT_0001_2.nii.gz` |
| Uploaded by compose | `http://neurovault.org/images/1040266/` | `http://neurovault.org/media/images/24371/z.nii.gz` |

So neither field can be trusted by position. `compose_runner.images` chooses the
candidate that is both fetchable and named like a NIfTI, and drops the image when
neither is — a landing page downloads happily as HTML, and a `.nii.gz` full of
HTML fails much later and much less legibly.

**`value_type` is a human-readable NeuroVault label**, not a NiMARE image type:
`"Z map"`, `"T map"`, `"univariate-beta map"`, `"variance"`,
`"P map (given null hypothesis)"`. `compose_runner.images` maps these onto
NiMARE's `z`/`t`/`beta`/`varcope`/`p` and drops the labels NiMARE has no use for
(`ROI/mask`, `parcellation`, `anatomical`, `F map`, `Chi squared map`, `other`).

## What the runner does differently for `type: "IBMA"`

| Step | Behaviour |
| --- | --- |
| `prepare_images` | Downloads every usable map into `<result_dir>/images` and returns a studyset pointing at the local files, caching across reruns. NiMARE hands paths to nibabel, which cannot read over HTTP. Staged on a copy, since `cached_studyset` is uploaded as the result's studyset snapshot. Records which maps it dropped and why, per analysis. |
| `apply_sample_sizes` | Copies a sample size onto each analysis's `metadata["sample_sizes"]`, preferring the annotation note, then analysis metadata, then study metadata. |
| `apply_filter(combine=False)` | Keeps each analysis separate. `combine_analyses()` is right for CBMA, where the point is pooling foci, but for IBMA it concatenates a study's images into one analysis and the conversion to a Dataset keeps only one map per type, silently discarding every extra contrast — and destroying the study grouping the dependence correction needs. |
| `load_specification` | Runs before any download, so a specification NiMARE rejects costs nothing. Drops the injected `n_cores`, which no image-based estimator takes, or passes it as `n_jobs` to `PermutedOLS`, the only one that parallelizes its fit. |
| `run_meta_analysis` | Dispatches to `IBMAWorkflow` with `diagnostics="jackknife"`. Group comparisons are rejected: no image-based estimator is pairwise. Needs a NiMARE carrying [#1102](https://github.com/neurostuff/NiMARE/pull/1102) — before it, `Studyset.slice` dropped the maps `ImageTransformer` derived, so every leave-one-out refit lost every converted analysis rather than the one being left out, which made the diagnostic table this runner uploads meaningless. |
| `upload_results` | Also publishes the `logp` maps [#1099](https://github.com/neurostuff/NiMARE/pull/1099) adds. They matter because the maps are float32: `p` loses precision below 1.18e-38 and reaches exactly 0 at \|z\| >= 14.17, which real studysets approach. `logp` carries the tail past that. |
| `_fit_image_based` | Fits the workflow, then reports what it used. On the failure NiMARE raises when nothing survives, describes the submitted studyset instead and appends that to the message. |
| `_describe_coverage` | Logs the account and writes `ibma_coverage.tsv` beside the results. |
| `_check_result_is_not_empty` | Rejects a result that is NaN at every voxel, naming the input maps that have no finite non-zero value. |

Task sizing is unchanged: `_requires_large_task` escalates a run to the large ECS
task for a montecarlo FWE corrector whatever the specification type, which for an
image-based run means `PermutedOLS`'s permutation test — the only image-based
path where the extra cores are worth paying for. Everything else fits the
standard task comfortably. Note that the choice is made in
`aws_lambda/run_handler.py`, so invoking `compose_runner.ecs_task` or the CLI
directly bypasses it.

## Uneven input, and what gets left out

Real studysets are not tidy. A base-study search for image data on "language"
returns 27 studies carrying nine distinct `value_type` labels between them:

```
Z map, T map, univariate-beta map, P map (given null hypothesis),
F map, 1-P map ("inverted" probability), ROI/mask, anatomical, other
```

NiMARE closes the gap between that and what an estimator needs on its own.
`IBMAWorkflow.fit` runs `ImageTransformer` over the estimator's
`_required_inputs`, converting what it can — a z-map from a t-map and a sample
size, a varcope from a t-map and a beta map — and `drop_invalid` discards
whatever is still incomplete. Both steps are silent, and that is the only
problem: a meta-analysis can rest on a third of the studies the user selected and
say nothing.

`compose_runner.coverage` does none of that work and owns no transform rules. It
introspects the fitted estimator, which already holds both answers:

| | |
| --- | --- |
| `estimator.inputs_["id"]` | the analyses that made it in, after everything was dropped |
| `estimator.dataset.images` | the image table *after* the transform, so comparing it with the table submitted says which maps were converted rather than supplied |

Each analysis is named in `<result_dir>/ibma_coverage.tsv`, one row each:

```
study_name                          analysis_name   included  supplied  converted  reason
The neural basis of free language…  Figure 2a       true      t         z          converted z
Functional organisation for verb…   DLD: VG> Rest   false                          ... dropped other
```

Partial coverage is not an error — it is the normal case, and the run proceeds.
Two things do stop it:

* **Nothing usable at all.** NiMARE raises `The collection has no data for
  'z_maps'`, which does not say which studies were unusable. There is no fitted
  estimator to introspect at that point, so the submitted studyset is described
  instead and the account is appended to NiMARE's message.
* **A result that is empty everywhere.** An image-based meta-analysis can finish
  with every map entirely NaN. The run looks successful and the maps it would
  upload hold no values, so this is rejected with the cause named.

  The cause is worth stating precisely, because it is not what it looks like.
  NiMARE counts a voxel as valid only where it is finite *and* non-zero, and
  `aggressive_mask` is the intersection of that across inputs. So a single map
  that is exactly zero everywhere — an empty NeuroVault upload, which does occur
  — empties the mask on its own, however well the rest overlap. The error names
  the empty maps rather than only suggesting `aggressive_mask=False`, since
  excluding an empty upload is the real fix.

## Changes needed in Neurostore

Neurostore PR #1695 is merged, and its `meta_analysis_params.json` carries
`weight_scheme`/`rho` with legal defaults, no `dependence`, and no
`normalize_contrast_weights`. `groupby` is deliberately left out, so every run
takes NiMARE's default of grouping images by the study that contributed them: a
JSON form cannot express the array and `False` forms, and `False` inflates
significance.

1. **Give sample size a real home.** Two estimators require it
   (`FixedEffectsHedges`, `SampleSizeBasedLikelihood`), `Stouffers` and `Fishers`
   can weight by it, and NiMARE needs it to derive a `z` map from a `t` map —
   the common case, since most NeuroVault maps are T maps. On the "language"
   corpus, having it is worth 17 analyses and 9 studies out of 27. Today it is a
   free-form study-level metadata key (a string, `"42"`). NiMARE wants one value
   per *analysis*, and a study contributing several contrasts with different `n`
   cannot be described study-level. A typed, analysis-level field would also let
   the frontend tell a user up front which studies an estimator will drop.

2. **Distinguish `se`/`sd` from `varcope`.** Four estimators need `beta` +
   `varcope`. Neurostore inherits NeuroVault's vocabulary, whose only
   variance-ish map type is `"variance"` — ambiguous between a variance of the
   contrast estimate and a standard error or standard deviation. NiMARE can
   derive `varcope` from `se`, from `sd` + `sample_sizes`, or from `t` + `beta`,
   but only if it is told which it has. Without the distinction, `"variance"` is
   taken as `varcope` and a study that actually published a standard error is
   silently analysed with the wrong scale.

3. **Drop the hardcoded `aggressive_mask` override.** The config generator sets
   it to `False` explicitly, which now restates NiMARE's own default and will go
   stale the moment upstream reconsiders. Regenerating picks up `False` on its
   own.

## Changes needed in NiMARE

Moved to `docs/nimare-asks.md`, which carries the two asks that used to sit here
(`Studyset.slice` on an unresolvable id, and `inputs_` bloating a pickled
`MetaResult`) plus five that came out of the staging run, each with a
NiMARE-only reproduction in `scripts/nimare_asks_repro.py`.

## Running the simulation

```bash
# Which estimators the config exposes, and what each needs
python scripts/simulate_ibma_bundle.py --list

# Build a bundle and print it without running
python scripts/simulate_ibma_bundle.py --estimator Fishers --dry-run

# Build and run, picking studies that already carry the estimator's maps
python scripts/simulate_ibma_bundle.py --estimator Stouffers --n-studies 6

# The messy case: take a real search as-is, whatever it returns
python scripts/simulate_ibma_bundle.py --estimator Stouffers --search language \
    --n-studies 30

# The same search with no sample size recorded, which is what stops NiMARE
# converting a t-map into a z-map
python scripts/simulate_ibma_bundle.py --estimator Stouffers --search language \
    --n-studies 30 --sample-size 0
```

Point `--config` (or `COMPOSE_PARAMS_CONFIG`) at a Neurostore checkout's
`compose/neurosynth-frontend/src/assets/config/meta_analysis_params.json`.

`--search` is the one worth reaching for when testing behaviour on uneven input:
it takes a `/base-studies/` query and uses whatever comes back, rather than
selecting for studies the estimator can already use.

`--dry-run` is enough to check that a config change produces a specification this
runner accepts. A full run is not fast, and `Jackknife` dominates it — one refit
per image.
