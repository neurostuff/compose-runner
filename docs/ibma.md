# Image-based meta-analysis

Compose cannot yet create an image-based meta-analysis, so this describes the
bundle the frontend *will* produce, what this runner does with it, and where the
three repositories still disagree.

Everything below was established by building that bundle and running it:
`scripts/simulate_ibma_bundle.py` assembles real Neurostore studies and real
NeuroVault maps into a bundle whose specification is generated from the
frontend's own `meta_analysis_params.json`, then pushes it through `Runner`.

## Versions this is written against

| Package | Version | Why |
| --- | --- | --- |
| NiMARE | `main` (pinned in `pyproject.toml` as a git reference until 0.21.0 ships; verified at `605e03f`, where [PR #1090](https://github.com/neurostuff/NiMARE/pull/1090) merged) | `groupby`, IBMA dependence handling, `aggressive_mask=False` by default, `Stouffers` without `normalize_contrast_weights` |
| PyMARE | 0.0.11rc1 | `Dataset(g=...)`, `encode_groups`, `group_mean`, `estimate_null_correlation`; NiMARE 0.21.0 requires it |
| Neurostore | [PR #1695](https://github.com/neurostuff/neurostore/pull/1695) | IBMA algorithm config and the `IBMA` specification type |

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
      "groupby": null,
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

So neither field can be trusted by position. `compose_runner.images`
chooses the candidate that is both fetchable and named like a NIfTI, and drops
the image when neither is — a landing page downloads happily as HTML, and a
`.nii.gz` full of HTML fails much later and much less legibly.

**`value_type` is a human-readable NeuroVault label**, not a NiMARE image type:
`"Z map"`, `"T map"`, `"univariate-beta map"`, `"variance"`,
`"P map (given null hypothesis)"`. `compose_runner.images` maps these onto
NiMARE's `z`/`t`/`beta`/`varcope`/`p` and drops the labels NiMARE has no use for
(`ROI/mask`, `parcellation`, `anatomical`, `F map`, `Chi squared map`, `other`).

## What the runner does differently for `type: "IBMA"`

| Step | Behaviour |
| --- | --- |
| `prepare_images` | Downloads every usable map into `<result_dir>/images`, rewrites the studyset to point at the local files, and caches across reruns. NiMARE hands paths to nibabel, which cannot read over HTTP. Records which maps it dropped and why, per analysis. |
| `_report_coverage` | Runs NiMARE's transform, then says which analyses the estimator can use, which maps were derived, and what was left out. Writes `ibma_coverage.tsv`; raises only if nothing is usable. |
| `apply_sample_sizes` | Copies a sample size onto each analysis's `metadata["sample_sizes"]`, preferring the annotation note, then analysis metadata, then study metadata. |
| `apply_filter(combine=False)` | Keeps each analysis separate. `combine_analyses()` is right for CBMA, where the point is pooling foci, but for IBMA it concatenates a study's images into one analysis and the conversion to a Dataset keeps only one map per type, silently discarding every extra contrast — and destroying the study grouping the dependence correction needs. |
| `load_specification` | Resolves estimator arguments through `compose_runner.estimator_args` and **rejects** any the estimator does not accept. |
| `run_meta_analysis` | Dispatches to `IBMAWorkflow` with `diagnostics="jackknife"`. Group comparisons are rejected: no image-based estimator is pairwise. |
| `_requires_large_task` | Routes every IBMA specification to the large ECS task, whatever its corrector. See the caveat below. |

### Task sizing, and what it does not buy

`_requires_large_task` returns True for any specification whose `type` is
`IBMA`, so an image-based run gets 16 vCPU and 64 GiB instead of 4 and 30. Two
things limit that.

**It only applies to runs submitted through the lambda.** `task_size` is chosen
in `aws_lambda/run_handler.py` and reaches the state machine's Choice as
`$.task_size`. Invoking `compose_runner.ecs_task` or the CLI directly bypasses
the decision entirely — neither reads `task_size`.

**And the escalation buys CPU and memory, not disk.** In
`infra/cdk/stacks/compose_runner_stack.py`, `ephemeral_storage_gib` is set only
when `taskEphemeralStorageGiB` exceeds 20, and no deployed context overrides it,
so both task definitions run on Fargate's default ~20 GiB — shared with the
container image.

Measured, that is not tight. The whole "language" run — 27 studies, 53 analyses,
39 fitted images — used **156 MB**, under 1% of the volume:

| | size | share |
| --- | --- | --- |
| `meta_results.pkl` | 107 MB | 69% |
| `images/` (43 downloaded, 17 derived) | 46 MB | 29% |
| output maps and tables | ~4 MB | 2% |

The surprise is that the downloads are not the cost; the pickle is, and 94% of it
is input data that is already on disk in `images/`:

```
whole result      111.9 MB
  estimator       105.5 MB
    inputs_       101.6 MB
      data_bags    66.0 MB     only exists under aggressive_mask=False
      z_maps       35.6 MB     (39, 228483)
    dataset         2.0 MB
  maps              6.4 MB     the actual results
```

Maps are resampled into the 2mm target space, so the voxel count is fixed at
228,483 and higher-resolution inputs inflate the *downloads*, not the pickle.
That puts the per-analysis cost at ~2.7 MB of pickle plus 1–4.4 MB of download
(~10 MB if the source is 1mm) — call it 13 MB at worst, so roughly 1,300
analyses before ~17 GiB of usable volume matters. Real studysets are tens to low
hundreds, so there is room; it is finite rather than generous, and
`prepare_images` has no accounting to fail early if the volume does fill.

Worth noting separately: `_persist_meta_results` spends 100 MB to persist 6.4 MB
of results. Dropping `inputs_` before pickling would reclaim almost all of the
largest consumer, if nothing downstream reads it.

The size is also a guess whenever the submit lambda cannot read the
specification: `_fetch_meta_analysis` is an unauthenticated GET with a 10 second
timeout, and a transient failure falls back to `standard`. That fallback now logs
`workflow.task_size_selected` with `reason=specification_unavailable` or
`reason=specification_not_nested`, so it is at least visible afterwards rather
than looking like an unexplained OOM.

### Uneven input, and what gets left out

Real studysets are not tidy. A base-study search for image data on "language"
returns 27 studies carrying nine distinct `value_type` labels between them:

```
Z map, T map, univariate-beta map, P map (given null hypothesis),
F map, 1-P map ("inverted" probability), ROI/mask, anatomical, other
```

NiMARE closes the gap between that and what an estimator needs entirely on its
own. `IBMAWorkflow.fit` runs `ImageTransformer` over the estimator's
`_required_inputs`, converting what it can -- a z-map from a t-map and a sample
size, a varcope from a t-map and a beta map -- and `drop_invalid` discards
whatever is still incomplete. Both steps are silent, and that is the only
problem: a meta-analysis can rest on a third of the studies the user selected and
say nothing.

`compose_runner.coverage` does none of that work and owns no transform rules. It
introspects the fitted estimator, which already holds both answers:

| | |
| --- | --- |
| `estimator.inputs_["id"]` | the analyses that made it in, after everything was dropped |
| `estimator.dataset.images` | the image table *after* the transform, so comparing it with the table submitted says which maps were converted rather than supplied |

On that "language" studyset, running `Stouffers` (which needs z-maps):

| | analyses used | studies | converted |
| --- | --- | --- | --- |
| sample sizes recorded | 39 of 53 | 21 | `z` for 17 |
| no sample sizes | 22 of 53 | 12 | -- |

The 17 are t-map studies: with a sample size NiMARE converts them, without one
it cannot, and they drop out. That difference is the strongest argument for
giving sample size a real home in Neurostore (see below) -- it is worth nine
studies on this corpus alone.

The 14 excluded either way carry only maps NiMARE has no use for. Each is named
in `<result_dir>/ibma_coverage.tsv`, one row per analysis:

```
study_name                          analysis_name   included  supplied  converted  reason
The neural basis of free language…  Figure 2a       true      t         z          converted z
Functional organisation for verb…   DLD: VG> Rest   false                          ... dropped other
```

With the liberal mask that studyset produces a full-brain result: 39 images from
21 studies, dependence corrected, `z` finite at all 228,483 voxels and ranging
[-7.014, +13.093].

Partial coverage is not an error -- it is the normal case, and the run proceeds.
Two things do stop it:

* **Nothing usable at all.** NiMARE raises `No images were found for a required
  input`, which does not say which studies were unusable. There is no fitted
  estimator to introspect at that point, so the submitted studyset is described
  instead and the account is appended to NiMARE's message.
* **A result that is empty everywhere.** An image-based meta-analysis can finish
  with every map entirely NaN — `z` finite at 0 of 228,483 voxels, against all of
  them under the liberal mask. The run looks successful and the maps it would
  upload hold no values, so this is now rejected with the cause named.

  The cause is worth stating precisely, because it is not what it looks like.
  NiMARE counts a voxel as valid only where it is finite *and* non-zero, and
  `aggressive_mask` is the intersection of that across inputs. On the "language"
  studyset one map was **exactly zero at every voxel** — an empty NeuroVault
  upload — and that single input emptied the mask on its own:

  ```
  image                            valid       %      zeros
  735cFJENA7cw-5sSdKtcd37Gh            0    0.0%     228483   <- empty upload
  8DzjYSWuiXNn-55bN7jfniJMt       101998   44.6%     126485
  ...
  59hYAQHW3YKg-5BEJFqcyG6bq       228483  100.0%          0

  intersection, worst first : [0, 0, 0, ...]              zero after 1 image
  intersection, best first  : [228483, ..., 204690, ...]  zero only at image 22
  ```

  The other 21 maps intersect to ~204,690 voxels and stay there, so the mask was
  not being over-strict about differing fields of view — it was correctly
  rejecting one worthless input. There were no NaNs at all; it is exact zeros
  throughout. So the error names the empty maps rather than only suggesting
  `aggressive_mask=False`, since excluding an empty upload is the real fix.

### Why unknown estimator arguments are an error

NiMARE's IBMA estimators used to take `**kwargs` and only *log* what they did
not recognize:

```
WARNING nimare.meta.ibma: Unused keyword arguments found: (('dependence', 'independent'),)
```

which meant a specification asking for `dependence="independent"` got the
dependence-corrected answer it was trying to avoid, with nothing in the output
saying so. `IBMAEstimator.__init__` now raises a `TypeError` instead.

`resolve_ibma_estimator_args` still validates, for two reasons. It runs before
`prepare_images`, so a stale specification costs nothing rather than a studyset's
worth of downloads; and it names what replaced a retired argument instead of only
reporting it as unexpected.

There is nothing to be backwards compatible with — compose cannot create an
image-based meta-analysis, so no stored specification names an argument an older
NiMARE had. So this targets one API and rejects everything else, including the
names an earlier draft of the frontend config used. `RETIRED_ESTIMATOR_ARGS`
exists only to make the error message say what replaced them:

* `dependence` → `groupby`, renamed and widened. `groupby=None` groups images by
  the study that contributed them (what `"auto"` meant), `groupby=False` gives
  every image its own group (what `"independent"` meant), and a string names a
  metadata field to group by.
* `normalize_contrast_weights` → gone; images sharing a group are now combined
  into one variance-standardized statistic.

The one argument that *is* translated is `n_cores`, which the runner injects for
every estimator: it is dropped, or becomes `n_jobs` for `PermutedOLS`, the only
image-based estimator that parallelizes its fit.

### What a completed run produces

For the record, the simulated `Stouffers` bundle above:

```
  images fitted        : 11
  independent groups   : 6
  dependence corrected : True
  maps                 : dof, label_corr-FDR_method-indep_tail-{negative,positive},
                         p, p_corr-FDR_method-indep, z, z_corr-FDR_method-indep
    z                       finite 228483/228483  range [-4.239, +6.815]
    z_corr-FDR_method-indep finite 228483/228483  range [-2.954, +5.815]
    dof                     finite 228483/228483  range [+0.000, +5.000]
```

The `dof` map topping out at 5 is the check worth keeping: for a combination test
it is `n_groups - 1`, so 5 says the 11 images were correctly resolved to the 6
studies that contributed them. Under `groupby=False` it would read 10.

## Changes needed in Neurostore

PR #1695 is merged (`ce72cc36`), and the two config items below are done. The
committed `meta_analysis_params.json` now carries `weight_scheme`/`rho` with
legal defaults, no `dependence`, and no `normalize_contrast_weights`; all nine
IBMA specifications built from it construct against pinned NiMARE. `groupby` is
deliberately left out, so every run takes NiMARE's default of grouping images by
the study that contributed them -- a JSON form cannot express the array and
`False` forms, and `False` inflates significance.

~~1. Regenerate the IBMA config.~~ Done.

~~2. Derive defaults from the MRO, not the subclass signature.~~ Done -- the
generator now walks `inspect.getmro`, which is what makes `weight_scheme` and
`rho` come out as `'rescale'` and `0.8` rather than `null`.

3. **Give sample size a real home.** Two estimators require it
   (`FixedEffectsHedges`, `SampleSizeBasedLikelihood`), `Stouffers` and `Fishers`
   can weight by it, and NiMARE needs it to derive a `z` map from a `t` map —
   which is the common case, since most NeuroVault maps are T maps. Measured on
   the "language" corpus above, having it is worth 17 analyses and 9 studies out
   of 27: 39 of 53 analyses are usable with it, 22 without. Today it is a
   free-form study-level metadata key (a string, `"42"`). NiMARE wants one value
   per *analysis*, and a study contributing several contrasts with different `n`
   cannot be described study-level. A typed, analysis-level field would also let
   the frontend tell a user up front which studies an estimator will drop.

4. **Distinguish `se`/`sd` from `varcope`.** Four estimators need `beta` +
   `varcope`. Neurostore inherits NeuroVault's vocabulary, whose only
   variance-ish map type is `"variance"` — ambiguous between a variance of the
   contrast estimate and a standard error or standard deviation. NiMARE can
   derive `varcope` from `se`, from `sd` + `sample_sizes`, or from `t` + `beta`,
   but only if it is told which it has. Without the distinction, `"variance"` is
   taken as `varcope` and a study that actually published a standard error is
   silently analysed with the wrong scale.

5. **Drop the hardcoded `aggressive_mask` override.** The generator sets it to
   `False` explicitly, with a comment explaining why the liberal mask is the
   better default. NiMARE agreed: `IBMAEstimator` defaults `aggressive_mask` to
   `False` as of 0.21.0, so the override now restates upstream and will go stale
   the moment upstream reconsiders. Regenerating picks up `False` on its own.

   The cost is worth knowing either way. Measured on the simulated bundle —
   6 real NeuroVault studies, 11 Z maps, `Stouffers`, estimator fit only:

   | `aggressive_mask` | fit | voxel bags | voxels with values |
   | --- | --- | --- | --- |
   | `true` | 3.5 s | — | 44,771 / 228,483 (20%) |
   | `false` | 77.8 s | 74 | 228,483 / 228,483 (100%) |

   The default is the right call on statistical grounds, but it is ~22x slower,
   and `Jackknife` refits once per image on top of that. Halving the refits (see
   below) helps; for the 40+ study studysets compose users build, the diagnostic
   rather than the estimator is still what will dominate.

## NiMARE IBMA diagnostics: fixed upstream

All three of these were found by running the simulated bundle and have since been
fixed in the `enh/ibma_dependence` working tree. Kept here because the runner's
behaviour was shaped by them, and because the numbers are the check that the
fixes hold.

1. **Jackknife ran every leave-one-out refit twice for a two-tailed IBMA.**
   `Diagnostics.transform` builds `meta_ids_lst` as the id list *twice* for a
   non-pairwise two-tailed result, once per tail. But the leave-one-out refit in
   `Jackknife._transform` is tail-independent -- it computes
   `1 - temp_stat_vals / stat_values` across the whole brain, and only the final
   `_summarize_cluster_values` call needs the tail. So half the refits were
   recomputed identically and discarded.

   Fixed by `_transform_batch`, which batches a tail pair against one refit.
   Measured on the same 11-image bundle with two tails and 52 clusters:

   | | estimator fits |
   | --- | --- |
   | before (`2 * n_images + 1`) | 23 |
   | after (`n_images + 1`) | 12 |

   The `copy.deepcopy(result.estimator)` per refit looked like a second problem,
   since an IBMA estimator's `inputs_` holds every input map rather than a small
   coordinate table -- but measured, it is not: 27.6 MB and under 10 ms per copy
   for this bundle (10.3 MB with `aggressive_mask=True`).

2. **`Workflow` built its diagnostics with its own deprecated argument.**
   `diag_kwargs` passed `voxel_thresh`, which diagnostics have deprecated in
   favour of `target_threshold`, so every workflow run -- coordinate-based
   included -- emitted a `FutureWarning` about a parameter the caller never set.
   Now passed as `target_threshold`.

3. **An already-constructed diagnostic silently ignored the workflow's
   thresholds.** `_check_input` returned an instance untouched, so
   `IBMAWorkflow(diagnostics=Jackknife())` left `target_threshold` and
   `cluster_threshold` at None while `diagnostics="jackknife"` got 1.65 and 10 --
   two spellings that read identically at the call site and defined clusters
   differently. `_check_input` now fills in whatever the caller left at its
   default, on a copy, and leaves explicit settings alone.

   The runner still names the diagnostic rather than constructing it, matching
   the coordinate-based branches. `test_naming_and_constructing_a_diagnostic_agree`
   pins the equivalence.

## Changes that would help in NiMARE

Almost everything the simulation turned up here has since been fixed on main.
One item is still open:

1. **`Studyset.slice` silently returns nothing for a hyphenated id.** A full id
   is composed as `<study_id>-<analysis_id>` and split back on the hyphen, so an
   id that contains one never resolves -- and `slice` returns an empty studyset
   rather than raising:

   ```python
   ss.slice(analyses=["ready-a", "mixed-a"])   # -> no analyses
   ss.slice(analyses=["analysis0"])            # -> works
   ```

   Real Neurostore ids are hyphen-free base62, so this does not bite production
   today. It cost an afternoon on synthetic fixtures, and it would cost far more
   the first time an imported id carries a hyphen. Raising on an unresolvable id,
   or delimiting on something that cannot occur in an id, would close it.

### Already fixed on main

Kept for the record, since this runner's shape was decided against the old
behaviour:

* **Unknown constructor arguments were silently ignored.**
  `IBMAEstimator.__init__` logged `Unused keyword arguments found` and continued,
  so a stale specification produced a complete, plausible, wrong result. Now
  raises a `TypeError`.

* **`generate_description` was dropped.** `IBMAEstimator.__init__` did not forward
  it to `Estimator.__init__`, so the attribute stayed `True` while every
  coordinate-based estimator honoured it. Now forwarded.

* **The image location was read as `image.url or image.filename`**, which for
  compose-uploaded images is the NeuroVault landing page. `_select_image_path`
  now picks by content. That makes this runner's `select_image_url` redundant; it
  can be dropped once the pinned NiMARE carries the fix, though it still earns
  its place while the runner also has to reject unfetchable bare basenames.

* **`DEFAULT_MAP_TYPE_CONVERSION` missed two labels Neurostore emits** —
  `"multivariate-beta map"` and `"P map (given null hypothesis)"`. Both are in the
  table now, and the fallback returns None for a label NiMARE cannot use rather
  than inventing the image type `"f"` from `"F map"`.

* **`images[image_type] = path` silently dropped duplicates** — two z maps on one
  analysis kept whichever came last. `_add_image_path` now keeps the first and
  warns about what it dropped.

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

`--dry-run` is the fast path and is enough to check that a config change produces
a specification this runner accepts. A full run is not fast, and `Jackknife`
dominates it -- one refit per image, so the 39-image "language" studyset costs 40
fits. See the diagnostics section above.
