# Running hand-rolled bundles against staging

Compose cannot create an image-based meta-analysis yet, so the only way to find
out what breaks is to assemble the bundle a frontend would POST and push it
through `Runner`. `docs/ibma.md` describes that bundle; this describes what
happened when 16 studysets cut from **staging** Neurostore were run through 30
specifications — 76 combinations — what failed, why, and what was changed here
in response.

| | Cases | Succeeded | Failed |
| --- | --- | --- | --- |
| First pass | 65 | 42 | 23 |
| After the fixes below | 76 | 55 | 21 |

Eleven cases were added as failures pointed at gaps the first pass did not
cover, so the two rows are not the same set; the second is a superset. Finding 1
below had to be fixed before the first pass could run at all.

Every one of the 21 remaining failures is a refusal the specification or the
corpus earned, delivered with a message naming what to change. None is a defect.
Coordinate-based cases are included where the studysets carry coordinates, since
`apply_filter`, `load_specification` and `process_bundle` are shared — and two of
the bugs below live on that path.

Run on 2026-08-22 against `https://staging.neurostore.xyz/api`, with NiMARE
0.21.0rc3, PyMARE 0.0.12, nilearn 0.13.1 on Python 3.12 (`.venv-dev`). The
harness is `scripts/staging_matrix/`; see [Reproducing](#reproducing).

## What staging's image corpus can actually support

`/api/base-studies/?data_type=image` returns **684 base studies**. Their
precomputed map flags:

| Flag | Base studies |
| --- | --- |
| `has_t_maps` | 390 |
| `has_z_maps` | 129 |
| `has_beta_and_variance_maps` | **0** |

Across the 90 studies pulled for this run, an analysis carries exactly one
NiMARE image type almost without exception:

```
('t',)               324
('z',)               237
('beta',)             30
('beta', 'p', 'z')     1
```

That shape decides which estimators are reachable, because NiMARE derives
`varcope` only from `se`, `sd`, `samplevar_dataset`, or `t` **and** `beta`
together — and no analysis in this corpus carries `t` and `beta` together.

| Estimator | Needs | Reachable on staging |
| --- | --- | --- |
| `Stouffers`, `Fishers` | z | yes — directly, or derived from t + sample size |
| `FixedEffectsHedges` | t + sample_sizes | yes |
| `PermutedOLS` | beta | only the ~30 beta analyses |
| `SampleSizeBasedLikelihood` | beta + sample_sizes | those, and only with *varying* sample sizes |
| `DerSimonianLaird`, `Hedges`, `WeightedLeastSquares`, `VarianceBasedLikelihood` | beta + varcope | **no** |

Four of the nine estimators the frontend config offers cannot run on anything
cut from this corpus. They fail cleanly — `The collection has no data for
'varcope_maps'` plus the coverage report — but a user picking one is choosing
an estimator that cannot work, and nothing says so before the run.

## Errors found

Each is written as symptom, cause, and what was done. "Fixed here" means this
repository changed and the case now passes; "upstream" means it did not. The
five that are really NiMARE's are written up for upstream in
`docs/nimare-asks.md`, with reproductions that do not involve compose.

**Five of the fixes below were later removed from this repository.** Once
`docs/nimare-asks.md` 1-5 were implemented, the workarounds here duplicated them
one layer down, so findings 2, 3, 10 and 11 are now handled entirely
upstream, and part of finding 5 is. What each section describes is still what was wrong and
how it was diagnosed; the "what was done" is now upstream's. See
[What changed here](#what-changed-here) for where each one ended up.

### 1. A relative `result_dir` sends every map missing — fixed here

```
FileNotFoundError: No such file or no access:
'<result_dir>/images/<result_dir>/images/7u4t6uDkEuGF_..._zmap.nii.gz'
```

`download_studyset_images` wrote whatever path `image_dir` produced into the
studyset, so a relative `result_dir` left relative image references. NiMARE
resolves a relative reference against the studyset's base path, which
`process_bundle` sets to that same directory — so the directory is prepended to
a path already containing it. Every map is then missing, at the point where
nibabel opens it.

`Runner.result_dir` is now resolved in `__init__`, and staged image paths are
made absolute. Either alone is enough; both are cheap. Reachable from the CLI
today (`compose-run --result-dir results/run1`).

### 2. One study's mirrored contrasts fail the whole meta-analysis — reported better here, worth an upstream change

```
ValueError: Group 0 pools 2 estimates whose aggregated z statistic has variance
1.39e-16, which carries no usable information: its members cancel. Check the
correlation matrix supplied for this group, or drop the group.
```

Raised by PyMARE's combination test (`pymare/estimators/combination.py`), from
`Stouffers` on 3 of the 16 studysets — `face`, `motor` and `corpus_t`, the last
under two annotations — so 5 of the 76 cases. NiMARE groups an estimator's images
by the study that contributed them and corrects for the dependence between them;
when a group's correlation block sums to zero, the group's pooled z has no
variance and PyMARE refuses it.

The two traced to their studies are ordinary NeuroVault uploads, not corrupt
data:

- **`face`** — *Individual face- and house-related eye movement patterns
  distinctively activate FFA and PPA* uploaded `Exp1_FFA_localizer` and
  `Exp1_PPA_localizer`. They correlate **-1.000**: the same contrast in both
  directions.
- **`motor`** — *Differences in Resting State Functional Connectivity between
  Young Adult Endurance Athletes and Healthy Controls* uploaded default-mode,
  frontoparietal and motor network maps. Pairwise correlations run -0.547 to
  +0.243 and the 3x3 block sums to nothing, which is what complementary networks
  from one cohort do.

Neither is rare, and the message named the group by the integer code NiMARE
assigned it, which is meaningless to whoever chose the studies. `Runner` now
translates it: which study the group is, which analyses it holds, whether a pair
is a mirror (with the correlation) or the block merely cancels, and the two ways
out — keep one analysis per group, or set the estimator's `groupby` to `false`.
`groupby: false` was verified to run `face` and `motor` to completion.

**Upstream:** naming the group is the smaller half. One study's uploads should
not end the whole meta-analysis — NiMARE should drop a group that cancels from
the liberal-mask bags where it cancels, warn naming the study, and fit the rest,
and should warn loudly about a perfect -1 pair that does *not* cancel. That decision and its implementation notes are
ask 4 of `docs/nimare-asks.md`; once it lands, the translation added here
becomes unnecessary.

### 3. An all-zero upload counts as an analysis that contributed — fixed here

Also in the `motor` studyset: *Inter-subject Similarity of Brain Activity in
Expert Musicians*/`Fig3 CC Video post - pre` is an entirely zero t map on
NeuroVault. NiMARE counts a voxel as valid only where it is finite **and**
non-zero, so it has no valid voxel anywhere.

Under `aggressive_mask=True` that one map empties the intersection mask by
itself and every output map comes back NaN, reported only as
`Masking out 228483 additional voxels` — a count, never the map. Under the
liberal default the fit is fine, because the map joins no bag of voxels; but it
still holds a row in `inputs_["id"]` and a slot in the dependence grouping, so
the run reports one more contributing analysis than it had.

*(An earlier draft of this section claimed the empty map caused the `motor`
cancellation in finding 2. It does not: that failure names the same group with
the same variance before and after this fix, and the grouping each model is
fitted with is restricted to its own bag, which excludes the empty map.)*

Staging now opens each downloaded map and drops it when it has no finite
non-zero voxel, or cannot be read as a NIfTI, recording the reason like any
other dropped map:

```
study_name                              analysis_name           dropped_maps
Inter-subject Similarity of Brain …     Fig3 CC Video post-pre  T map (has no finite non-zero voxel)
```

2 of 242 maps downloaded for this run were empty (0.8%). The scan is one pass
per file over data already on disk.

`Runner._check_result_is_not_empty` still guards the case this does not cover:
maps that each carry signal but do not overlap, which empties an aggressive
mask.

### 4. A failure after the transform was reported as if nothing had been transformed — fixed here

When `workflow.fit` raised, `_fit_image_based` described the *submission*, which
assumes NiMARE converted nothing. For the `face` failure that produced:

```
Used 0 of 18 analyses, from 0 study/studies.
     8 x dropped by NiMARE as invalid -- …
     8 x NiMARE could not produce z from t -- …
```

Every line of which is wrong: NiMARE had converted all eight t maps and fitted
16 of 18 analyses before failing for an unrelated reason. A reader would go
looking at the maps.

`coverage.describe_estimator` now reports from an estimator that collected its
inputs but failed, and `_fit_image_based` uses it whenever `inputs_["id"]`
exists, falling back to the submission description only when the fit never got
that far. The same failure now reports `Used 16 of 18 analyses, from 6
study/studies. NiMARE converted: z for 8.`

### 5. "Every map is entirely NaN" did not say that only one analysis survived — fixed here

`corpus_beta` under `Fishers`: 11 of 12 analyses carry only a beta map, from
which z cannot be derived, so exactly one analysis reached the estimator and
every output map came out NaN. The message named neither fact.

It now opens with `N analysis/analyses reached the estimator, of M submitted`
and, below two, says a meta-analysis has nothing to pool. The coverage table
already carried the per-analysis reasons.

### 6. Coordinate-based runs never got their sample sizes — fixed here

```
ValueError: This estimator's kernel requires per-experiment sample sizes, but 1
experiment id has no reported sample size: 7D7TrLGZhcyp-iBmoqQXoVFMy_CzDYjcJXokb9.
```

Every coordinate-based case failed this way: `ALE` on `motor` and `language`,
`ALESubtraction` two-group, and `ALESubtraction` against the `neurostore`
database studyset. `apply_sample_sizes` — which copies a sample size from the
annotation note onto the analysis, where NiMARE looks for it — was called only
on the image-based path, from `prepare_images`. A coordinate-based kernel needs
one just as much, ALE sizes its Gaussian from it, and **one** analysis without
one fails the entire run.

`process_bundle` now applies it on both paths, on a copy, since
`cached_studyset` is uploaded as the result's snapshot. `motor` + `ALE` now runs
to completion.

This is not IBMA-specific and is reachable in production today, for any studyset
holding a study whose analyses carry no sample-size metadata — which is normal
for NeuroVault-sourced studies.

### 7. An image-based group comparison was rejected only after the work was done — fixed here

`run_meta_analysis` correctly refuses a two-group image-based specification, but
that is the last step: the maps have already been staged, and for
`database_studyset: "neurostore"` the whole 85 MB nightly release has already
been downloaded and parsed. `process_bundle` now refuses it right after the
specification is loaded, before anything is fetched. The case went from 17.5 s
to 6.2 s, and from 85 MB to nothing.

### 8. A filter naming a column the annotation does not have raised a bare `KeyError` — fixed here

`self.cached_annotation["note_keys"][column]` — `KeyError: 'included'` and
nothing else. Now:

```
The specification selects analyses by 'included', which this annotation does not
record. It has: ['analysis_group', 'sample_size'].
```

### 9. A selection matching nothing was reported as a missing image type — fixed here

An annotation with `included: false` everywhere produced `No images were found
for a required input of Stouffers` with `Used 0 of 0 analyses`, which sends the
reader to the maps rather than to the selection. `apply_filter` now says:

```
No analysis is selected: none of the 22 analyses in the studyset has an
'included' note that is true.
```

This changed a documented behaviour — `apply_filter` used to return an empty
studyset — and `test_a_filter_column_no_note_carries_selects_nothing` was
updated to pin the new contract. The half that mattered is unchanged: an
un-noted column still selects nothing rather than everything.

### 10. A p-only analysis joins the meta-analysis unsigned — upstream

`compose_runner.images` mapped Neurostore's `P map (given null hypothesis)` onto
NiMARE's `p`, and NiMARE's only route from a p map is `p_to_z`, which is
documented to return an **unsigned** z. An analysis whose only usable map is a p
map therefore contributed an all-positive map to a Stouffers or Fishers
meta-analysis, silently, and the coverage report called it a successful
conversion.

No analysis in these 16 studysets is p-only — the one carrying a p map carries a
z map too, which takes precedence — so `scripts/staging_matrix/probe_p_only.py`
builds the case out of that same real staging map by keeping only its p map. The
five-study run succeeds, reports `NiMARE converted: z for 1`, and the input maps
read:

| Analysis | z range | negative voxels |
| --- | --- | --- |
| `3GSBdy6kxwWe-UcibZsQV2tSg` | -7.081 .. +5.152 | 57.2% |
| `3uj2L2hXRs9G-794UmU2Hx3Pk` | -3.225 .. +3.296 | 59.0% |
| `5D2wQxcdwZt2-6kXDsMhjpYiK` | -6.434 .. +9.629 | 40.0% |
| **`8DzjYSWuiXNn-55bN7jfniJMt`** (p-only) | **+0.302 .. +6.490** | **0.0%** |
| `qJk7k8rMaU26-k8hTTy9oiJn4` | -6.857 .. +7.963 | 36.8% |

**Fixed upstream, not here.** NiMARE ask 1 warns at the point of conversion and
still converts, so the analysis contributes and the caller is told why the map is
one-sided. This repository briefly dropped `P map` and its relatives instead;
that was reverted, because two places deciding a p map is unusable is one too
many and dropping costs a real study. Where an analysis carries a t map as well,
ask 1 now recovers the sign from it and no warning is needed.

### 11. A null `voxel_thresh` broke every montecarlo FWE run — fixed here

The frontend's `FWECorrector` config lists `voxel_thresh` with a default of
`null`, and `load_specification` passed every corrector argument through.
`FWECorrector` keeps what it does not recognise and hands it to the estimator's
correction method, so:

| Estimator | Result |
| --- | --- |
| `PermutedOLS` | `TypeError: correct_fwe_montecarlo() got an unexpected keyword argument 'voxel_thresh'` |
| `ALE` | `TypeError: '<=' not supported between instances of 'float' and 'NoneType'`, from `_p_to_summarystat(None)` |

Both are total failures of the only FWE path either estimator has. Corrector
arguments whose value is `None` are now dropped, which defers to the estimator's
own default — 0.001 for ALE, nothing at all for PermutedOLS — and is what an
unset parameter asks for. `FWECorrector` already works this way for `n_iters`.
Both cases now run to completion.

### 12. An ALE-family comparison against the `neurostore` database cannot work — upstream

```
ValueError: This estimator's kernel requires per-experiment sample sizes, but
32118 experiment ids have no reported sample size: …
```

`ALESubtraction` against `database_studyset: "neurostore"`. The comparison group
is the nightly studyset release, and its `metadata.parquet` carries
`sample_sizes` for **633 of 115,748** analyses (0.5%) — 326 of 32,444 studies
once analyses are combined. ALE's kernel sizes its Gaussian from the sample
size, so it refuses.

Nothing here can fix that: `apply_sample_sizes` fills from the annotation, and
the reference studyset has no annotation and is not the user's to annotate. The
fix is for the release to carry sample sizes, which Neurostore has for these
studies.

`MKDAChi2` against the same reference **works** (verified, 96 s including the
85 MB download): its kernel is radius-based and needs no sample size. That is
the estimator a database comparison normally uses, so this is a gap rather than
a blocker — but an ALE-family selection fails after the download, with a message
about the user's estimator and a list of 32,118 ids from a studyset they never
saw.

## Confirmed working

- **t -> z conversion**, on every t-only studyset, with the sample size taken
  from the annotation note (`hand`: 16 of 16 analyses converted).
- **The jackknife diagnostic**, still carrying NiMARE #1102's fix: on `hand`,
  16 of 16 rows distinct and values centred on zero (median +0.0065, range
  -0.371 to +0.611). Before #1102 the converted analyses shared one row and the
  values scattered.
- **Partial coverage as the normal case**: `attention` fitted 8 of 18 analyses
  and `pain` 7 of 18, both to completion, with the coverage table naming every
  exclusion.
- **The aggressive-mask guard**: `aggressive_mask: true` on `corpus_z` fails
  with a message that names the setting and what to do about it.
- **Two studies is enough**: the `tiny` studyset (2 studies, 2 analyses) runs.
- **The frontend config's arguments are accepted by NiMARE.** The
  `weight_scheme` and `rho` that `meta_analysis_params.json` v0.6.1 lists for
  `FixedEffectsHedges` and `SampleSizeBasedLikelihood` are taken by
  `_PyMARERegressionEstimator`, despite not appearing in either subclass's own
  signature. The config does not list `small_sample_correction`, which NiMARE
  offers on `DerSimonianLaird` and `Hedges`; that is a missing option, not a
  mismatch.
- **`n_cores`**: passed as `n_jobs` to `PermutedOLS`, dropped for every other
  image-based estimator, passed to the montecarlo corrector.
- **String and boolean filter columns**, and `groupby: false`.
- **Pairwise coordinate-based comparisons**: `MKDAChi2` both between two groups
  of the user's own studyset and against the `neurostore` database studyset, and
  `ALESubtraction` between two groups of the user's own.
- **A `groupby` naming a metadata field no analysis carries** fails with
  `The collection has no data for 'dependence_groups'`, and the coverage report
  names the field: `Estimator needs images ['z'] and metadata ['study']`, then
  `21 x has the maps but was dropped; check study`. Note that grouping by study
  is what `groupby: null` already does; `groupby: "study"` asks for a metadata
  field of that name.

## Data-quality observations, for neurostore rather than here

- **No study in the image corpus carries variance maps**, so half the
  meta-regression estimators are unusable. Worth surfacing in the frontend
  rather than discovering per-run.
- **`SampleSizeBasedLikelihood` needs sample sizes that vary.** With one sample
  size noted for every analysis — which is what compose's annotation makes easy
  — it fails with `cannot work with all-equal sample sizes, and 3057 of 3057
  parallel datasets have them`.
- One analysis in `corpus_beta` reports a sample size of **1369**, which is not
  a subject count for an fMRI contrast.
- **Thresholded and one-sided maps are present and indistinguishable.** Of the
  134 distinct z and t maps staged for this run, **14 cover less than 5% of the
  volume** (two of the `motor` t maps carry 1,515 and 2,500 valid voxels out of
  902,629) and **16 carry no negative value at all** — a positive-only map
  labelled `Z map`, which biases a meta-analysis exactly the way the p maps of
  finding 10 do. Neurostore exposes neither NeuroVault's `is_thresholded` nor
  anything about a map's tail, so nothing in this pipeline can tell one from a
  real unthresholded z. This is the largest correctness gap the run found, and
  it is not one compose-runner can close: the fix is for Neurostore to carry the
  flag NeuroVault already records.

## What changed here

| Finding | Where it lives now |
| --- | --- |
| 1 relative `result_dir` | here: `run.py` `Runner.__init__`, `images.py` `_fetch` |
| 2 cancelling dependence group | **upstream only** (ask 4 drops the group and fits the rest) |
| 3 unusable staged map | **upstream** (ask 2), except an unreadable file: `images.py` `_unusable_reason` |
| 4 coverage after a failed fit | here: `coverage.py` `describe_estimator`, `run.py` `_fit_image_based` |
| 5 all-NaN message | here: `run.py` `_check_result_is_not_empty`, minus the fewer-than-two sentence (ask 3 raises first) |
| 6 coordinate-based sample sizes | here: `run.py` `process_bundle` |
| 7 early rejection of a comparison | here: `run.py` `_reject_image_based_comparison` |
| 8 missing filter column | here: `run.py` `apply_filter` |
| 9 empty selection | here: `run.py` `apply_filter` |
| 10 unsigned p maps | **upstream only** (ask 1 warns at the conversion, and recovers the sign from a t map when there is one) |
| 11 null corrector arguments | **upstream only** (ask 5 drops them in `Corrector.__init__`) |

Because of those removals the runner now needs a NiMARE newer than the pinned
0.21.0rc3; the pin comment in `pyproject.toml` records what it is waiting on.

Regression tests cover what remains: `test_ibma_dispatch.py` (7, 6, 1),
`test_ibma_end_to_end.py` (4, the unreadable-file half of 3, and the
aggressive-mask guard 5 does not cover), `test_images.py` (staging now opens what it
downloads), and `test_apply_filter.py` (9, which changed a
documented contract). The tests pinning the five removed workarounds went with
them. The suite is 129 tests.

## Reproducing

```bash
cd scripts/staging_matrix
../../.venv-dev/bin/python build_bundles.py   # 12 searches -> studysets + annotations
../../.venv-dev/bin/python build_extra.py     # z / t / beta corpora, and a 2-study one
../../.venv-dev/bin/python build_specs.py     # 30 specifications
../../.venv-dev/bin/python prefetch.py        # warm the shared image cache
../../.venv-dev/bin/python matrix.py --workers 3
../../.venv-dev/bin/python summarize.py
../../.venv-dev/bin/python probe_p_only.py    # the unsigned-z demonstration
```

Each cell runs in its own process and writes `runs/<case>/result.json` holding
its status, the exception and traceback, the coverage counts, and per-map finite
counts and ranges. Re-running skips completed cells; `--force` repeats them.
