# Changes wanted in NiMARE

Seven asks, each reproducible against NiMARE alone. Five came out of running 16
hand-rolled staging studysets through 30 specifications
(`docs/staging-ibma-matrix.md`). compose-runner works around four of those; the
fifth (ask 4) it can only report better, because the run still fails. The
workarounds protect compose and nothing else, and three of the five are
silent-wrong-answer bugs that every NiMARE caller has.

`scripts/nimare_asks_repro.py` reproduces asks 1–5 with synthetic NIfTIs, a
NIMADS studyset dict and the public API — no compose-runner — so each section
can be pasted into an issue as it stands. Output below is from that script
against NiMARE 0.21.0rc3 / PyMARE 0.0.12.

| # | Ask | Kind |
| --- | --- | --- |
| 1 | Warn when `z` is derived from an unsigned `p` map | silent wrong answer |
| 2 | Drop an input image with no valid voxel | silent wrong answer |
| 3 | Refuse a meta-analysis of fewer than two analyses | silent wrong answer |
| 4 | Drop a dependence group that cancels instead of failing the run | fails the run |
| 5 | Stop `FWECorrector` passing arguments the estimator cannot take | crash |
| 6 | Raise on an unresolvable id in `Studyset.slice` | silent no-op |
| 7 | Drop `inputs_` before pickling a `MetaResult` | disk |

---

## 1. Warn when `z` is derived from an unsigned `p` map

`transforms.py:335`, in `resolve_transforms`:

```python
elif "p" in available_data.keys():
    p = masker.transform(available_data["p"])
    z = p_to_z(p)
```

`p_to_z` is documented to return an **unsigned** z, so an analysis whose only
usable map is a p map enters a signed test as an all-positive map. Nothing
warns, and the coverage a caller can compute reports it as a successful
conversion. The t → z path 800 lines later gets this right —
`transforms.py:1135` is `np.sign(t_values) * nlogp_to_z(nlogp, tail="one")` — so
the asymmetry is on this one branch.

Four real z maps and one analysis carrying only a p map, through
`ImageTransformer(target="z")` and `Stouffers`:

```
s0-a0: min -2.553 max  +2.383 negative  49.5%
s1-a1: min -2.773 max  +2.696 negative  53.4%
s2-a2: min -3.046 max  +2.497 negative  54.9%
s3-a3: min -2.583 max  +2.759 negative  50.0%
s4-a4: min +0.061 max +38.485 negative   0.0%  <-- derived from p
warnings mentioning sign: none
```

**The sign is the finding; ignore the magnitude.** `+38.485` is `p_to_z(0.0)`,
the float64 clip bound, and it appears only because resampling this small
synthetic volume produces exact-zero p-values. Converted directly, the source p
map's z tops out at +2.846 — the same scale as the genuine maps. On real
staging data the p-derived map ran +0.302 to +6.490 against genuine maps at
±3 to ±9. So the bias is one-sided evidence, not an outlier that swamps the
pool.

**Wanted:** keep converting, and warn. Any pipeline currently relying on this
path keeps working, which an outright refusal would not allow — but the warning
has to be explicit that the result is unsigned and that the analysis contributes
only positive evidence.

The right seam is `resolve_transforms` itself rather than `transform_images`: it
is where the branch is taken, and it protects direct callers too. It does not
know the analysis id, so if per-analysis attribution matters, `transform_images`
(which has `id_` in scope) can aggregate the warning over the loop — an
implementer's choice, not part of the ask.

compose-runner relies on this warning rather than dropping p maps itself. It
briefly did drop them, and that was reverted: two places deciding a p map is
unusable is one too many, and the analysis it costs is a real study.

## 2. Drop an input image with no valid voxel

`ibma.py:308`, in `IBMAEstimator._preprocess_input`, already computes exactly
the array needed:

```python
validity = np.logical_and.reduce(
    [np.isfinite(self.inputs_[name]) & (self.inputs_[name] != 0) for name in image_names]
)
```

It uses `validity` for the aggressive mask and for the liberal-mask bags, but
never asks whether a whole *image* is empty — `~validity.any(axis=1)`. Directly
above it NiMARE warns about non-finite and tiny varcope values, and directly
below about voxels the aggressive mask removes, so the gap is this one check in
code that already holds the answer.

An entirely zero upload — 2 of the 242 maps staged from staging NeuroVault, so
not a corner case — survives collection with no warning of any kind:

```
inputs_['id']                 : ['s0-a0', 's0-a1', 's0-a2', 's1-a3', 's2-a4']
per-image valid voxel counts  : {'s0-a0': 204, 's0-a1': 204, 's0-a2': 0,
                                 's1-a3': 204, 's2-a4': 204}
contrast_names                : [0 0 0 1 2]
warnings about them           : none

aggressive_mask=False: the only bag's study_mask is [0 1 3 4] -- the empty image
                       is in no bag; finite voxels per output map: 204
aggressive_mask=True : WARNING Masking out 228483 additional voxels
                       finite voxels per output map: 0
```

Two different harms:

- **`aggressive_mask=True`: total loss.** One empty upload empties the
  intersection mask by itself and every output map is NaN. The only signal is a
  voxel count, which never names the map responsible — and the caller has to
  work backwards from a successful-looking `MetaResult` full of NaN.
- **`aggressive_mask=False`: a miscount.** The image joins no bag, so it changes
  no number in the result, but it holds a row in `inputs_["id"]` and a slot in
  `contrast_names`. Anything reporting what the meta-analysis rested on — a
  coverage table, `_dependence().n_groups`, a paper's methods section — counts
  an analysis that contributed nothing anywhere.

**Wanted:** drop it and warn, exactly as any other unusable input is dropped —
so the analysis follows if nothing else supplies the requirement, and what is
reported as an input is something that actually contributed. Under
`drop_invalid=False`, raise instead, since that is what that flag already means.

This does silently change which analyses contribute, but no more so than the
existing `drop_invalid` drops, and the alternative is contributing an image that
by NiMARE's own definition of validity contains nothing.

## 3. Refuse a meta-analysis of fewer than two analyses

`view.py:295` raises when a required input has **zero** rows —
`The collection has no data for 'z_maps'` — and nothing guards one row. One
analysis fits happily:

```
fit succeeded, inputs: 1
finite voxels per map: {'z': 0, 'p': 0, 'logp': 0, 'dof': 0}
warnings             : none
```

Every map is entirely NaN. The caller gets a `MetaResult`, writes it out, and
finds out later — this is how compose-runner ended up with a guard of its own.

**Wanted:** raise, with the message naming how many analyses reached the
estimator and how many were submitted, so the answer is "your studyset lost 11
of 12 analyses" rather than "the maps are NaN". The floor moves from zero to
two; nothing useful is lost, because the one-input result carries no information
at any voxel.

**Scoping question for the maintainer:** the demonstrated harm is IBMA-only, so
the natural home is `IBMAEstimator._preprocess_input`, where the image count is
already known. A single-experiment CBMA produces a real map rather than NaN, so
extending the floor to `Estimator._collect_inputs` would be a stricter change
with no evidence behind it. I would put it on the IBMA path and leave CBMA
alone unless you want the symmetry.

## 4. Drop a dependence group that cancels, rather than failing the run

`ibma.py:621` assigns integer codes:

```python
label_to_int = {label: i for i, label in enumerate(sorted(set(labels), key=str))}
self.inputs_["contrast_names"] = np.array([label_to_int[label] for label in labels])
```

and PyMARE raises against those codes
(`pymare/estimators/combination.py:313`):

```
ValueError: Group 0 pools 2 estimates whose aggregated z statistic has variance
2.78e-17, which carries no usable information: its members cancel. Check the
correlation matrix supplied for this group, or drop the group.
```

The studies are `s0, s1, s2, s3`. `Group 0` is an internal code, and the
"correlation matrix supplied for this group" was not supplied by the caller —
NiMARE estimated it. So the one actionable instruction in the message points at
something the reader does not have. But naming the group is the smaller half of
the problem: one study's uploads currently end the whole meta-analysis.

This is not rare. On staging it fired on 3 of 16 studysets, in two shapes:

- **An exact mirror pair.** One study contributed `Exp1_FFA_localizer` and
  `Exp1_PPA_localizer` — correlation exactly -1.000, the same contrast both ways
  round. Its 2×2 block sums to zero.
- **A three-way cancellation with no mirror pair.** Another contributed
  default-mode, frontoparietal and motor network maps from one cohort. Pairwise
  correlations run -0.547 to +0.243 — no pair anywhere near -1 — and the 3×3
  block still sums to nothing, which is what complementary networks from one
  cohort do.

### Wanted

**A group that cancels is dropped whole, not fatal.** Wherever a group's
aggregated variance reaches the floor, exclude that group's images from the fit
there, say so in a warning naming the study, and fit the rest. The whole group
rather than one member of it: both shapes above are covered, including the
three-way case where there is no single contrast to blame, and with three or more
members there is no principled choice of which one to remove anyway. A study
whose contrasts carry no joint signal has nothing to contribute either way.

"Wherever" is per liberal-mask bag, not per meta-analysis — a group's membership
differs between bags, so its verdict does too. See
[Where the drop goes](#where-the-drop-goes-and-at-what-granularity).

**A perfect -1 pair that does *not* cancel gets a loud warning, and nothing
else.** A group can hold two maps that are exact negatives and still fit, when
other members keep the block sum off zero. That is worth saying loudly — it
almost always means the same contrast was uploaded twice in opposite directions
— but the data stays in.

**Anything short of -1 that does not cancel stays silent.** No warning band, no
threshold above the floor. Near-cancellation is left alone deliberately: picking
a cutoff is a statistical policy nobody can set for the caller, and warning about
every mildly anti-correlated pair would bury the two cases above.

Note that the two triggers need different correlations. The cancellation test is
on the **estimated null** correlation NiMARE computes at `ibma.py:670`; the -1
warning should be on the **empirical** correlation between the maps, because
that is what says "these are the same contrast flipped". They differ more than
you would expect — a real mirror pair that shares signal estimates at -0.937
while correlating -1.0000 empirically:

```
near-cancellation: empirical corr -1.0000, estimated -0.9373
block variance 0.0313 -> no error, group z scaled by 5.7x
```

That case is exactly the second rule: loud warning, data kept.

### Where the drop goes, and at what granularity

**Per bag, not globally.** A liberal-mask bag already holds only the images
valid over its voxels, so a group's membership — and therefore whether it
cancels — is a per-bag property. A two-image group can only cancel where both
images are valid; where one is invalid it is a lone contrast. Section F of the
reproduction shows both bags of one studyset, where `s0` contributes an exact
mirror pair whose second map covers only part of the volume:

```
bag  voxels  members                        per-group block variance
  0     162  s0-a0,s0-a1,s1-a2,s2-a3,s3-a4  g0=0.1  g1=1  g2=1  g3=1
  1      42  s0-a0,s1-a2,s2-a3,s3-a4        g0=1    g1=1  g2=1  g3=1
```

Dropping `s0` globally would discard `s0-a0` over bag 1's 42 voxels, where
nothing is wrong with it. (An earlier draft of this document recommended the
global drop; that was wrong, and per-bag is what the liberal mask is for.)

**NiMARE already computes the object the decision needs.** `_fit_model` builds

```python
sub_corr = corr[np.ix_(study_mask, study_mask)]   # ibma.py:922
est.fit_dataset(pymare_dset, corr=sub_corr)
```

immediately before handing the bag to PyMARE. The per-group block variance is
PyMARE's own formula over that same matrix — for each group's local row indices,
`block.sum() / size ** 2` — so the verdict is a few lines in the method that
already holds both `study_mask` and `sub_corr`, before the call rather than
after the exception.

**Dropping is then filtering two aligned things.** `study_mask` is the alignment
key on both sides: `_dof_map(study_mask)` writes the degrees of freedom,
`_sample_sizes_for_mask` and `_group_sample_sizes_for_mask` build the weights
that are *passed into* PyMARE, and `_fit_over_bags` writes results back through
`voxel_mask`. Dropping a group is `keep = ~np.isin(study_mask, dropped)` applied
to both `study_mask` and the bag's arrays; everything downstream then describes
the fit that actually happened, by construction.

**There is already a precedent in the right place.** `_fit_over_bags` checks
`self._dependence(study_mask).supports_inference` per bag, skips the bag,
leaves those voxels NaN and emits one aggregated warning at the end. A group
drop belongs beside it — and after dropping, that existing check is what handles
a bag left with fewer than two groups.

### Should PyMARE do it instead?

I do not think so, for one concrete reason rather than a philosophical one:
PyMARE is handed the per-image weights NiMARE derived from `study_mask`, and
NiMARE writes a dof map derived from the same `study_mask`. If PyMARE dropped
rows internally, the weights it received and the dof NiMARE wrote would both
describe a fit that did not happen, unless PyMARE also returned a record of what
it dropped and NiMARE re-derived everything from it — a larger API change than
doing the drop one level up, where the mask can simply be filtered.

PyMARE also cannot say anything useful about *what* it dropped. It sees group
labels, not studies, so the warning would name `Group 0` again — which is half
of what makes the current error unactionable.

Keeping PyMARE's existing guard as a backstop still seems right: it is cheap, it
protects PyMARE's other callers, and once NiMARE drops beforehand it should
never fire.

### One thing to check while implementing

The correlation matrix is estimated **once**, over voxels finite in *every*
image (`ibma.py:663`, `finite_voxels = np.all(np.isfinite(maps), axis=0)`), and
then sliced per bag.

An earlier draft here guessed that this leaves the estimate resting on a small,
unrepresentative slice. Measured on a real 22-image staging studyset it is the
opposite, and worse:

| | voxels | of mask |
| --- | --- | --- |
| finite in every image — what `corr` is estimated on | 228,483 | 100.0% |
| finite **and non-zero** in every image | 0 | 0.0% |
| valid in >= 2 images — what the liberal mask fits | 227,333 | 99.5% |

The estimate covers everything because resampling writes **zeros**, not NaNs,
where a map has no data. So `finite_voxels` is the whole mask, and **20.8% of
the values being correlated are exactly 0** — a placeholder for "this map does
not cover here", not a measured effect. Meanwhile no voxel anywhere carries real
data from all 22 maps: per-image coverage runs from 295 to 218,590 of 228,483
voxels, and three of the 22 cover under half the mask.

The dependence correction — the term that stops two contrasts from the same
participants counting twice — is therefore calibrated partly on placeholders.
The obvious fix makes it worse: requiring finite *and* non-zero, which is
NiMARE's own validity test everywhere else (`ibma.py:308`), leaves zero voxels
on this studyset, so no correlation could be estimated at all and the run would
fall back to no inflation, warning that p-values may be anti-conservative.

**Suggestion, not part of the ask:** under `drop_invalid=False`, raise instead
of dropping, for the same reason as ask 2 — that flag already means "do not
silently narrow my inputs".

Once this lands, compose-runner's `_describe_dependence_group`, which exists
only to turn `Group 0` into a study name and a correlation, becomes unnecessary
and will be removed.

## 5. Stop `FWECorrector` passing arguments the estimator cannot take

`FWECorrector.__init__` keeps every unrecognised keyword and hands it to the
estimator's correction method. `correct.py:332` already establishes the right
rule, for exactly one parameter:

```python
# Only override estimator defaults when values are explicitly provided.
# If ``n_iters`` is None, defer to the estimator's own default ...
if n_iters is not None:
    kwargs["n_iters"] = n_iters
```

Nothing else gets that treatment, so a `voxel_thresh` of `None` — which is the
neurosynth-compose frontend's default for an unset parameter — is stored and
forwarded:

```
FWECorrector.parameters: {'voxel_thresh': None, 'n_iters': 5, 'n_cores': 1}
PermutedOLS -> TypeError: PermutedOLS.correct_fwe_montecarlo() got an
              unexpected keyword argument 'voxel_thresh'
```

and for ALE, which does take `voxel_thresh`, `None` overrides the 0.001 default
and reaches `_p_to_summarystat(None)`:
`TypeError: '<=' not supported between instances of 'float' and 'NoneType'`.
Between them that is the only FWE path either estimator has.

**Wanted:** both halves.

- Drop `None`-valued kwargs, which is the `n_iters` rule applied consistently.
- Validate the remaining kwargs against the signature of the estimator's
  `correct_fwe_*` method at `transform` time, and raise naming the method. A
  misspelled or inapplicable keyword should not travel from construction to the
  inside of a correction routine before anyone notices.

## 6. Raise on an unresolvable id in `Studyset.slice`

*Carried over from `docs/ibma.md`.*
[#1103](https://github.com/neurostuff/NiMARE/pull/1103) closed most of this: a
full `<study_id>-<analysis_id>` id is now matched whole rather than split on the
hyphen, so `slice(analyses=["mixed-a-b"])` resolves where it used to return
nothing. Two pieces are left:

```python
ss.slice(analyses=["a-b"])   # -> no analyses: the short id of "mixed-a-b"
ss.slice(analyses=["nope"])  # -> no analyses, no error
```

The short-id path derives its keys with `rsplit("-", 1)[-1]`, so a hyphenated
*analysis* id is unreachable by its short form, and an id matching nothing at
either level is an empty studyset rather than an error. compose-runner selects
on the full ids `annotations_df` reports, so neither bites it, but raising on an
unresolvable id would make the failure visible to whoever hits it next.

## 7. Drop `inputs_` before pickling a `MetaResult`

*Carried over from `docs/ibma.md`.* Most of a pickled `MetaResult` is
`estimator.inputs_`, which for an IBMA holds every input map — data already on
disk under `images/`. Dropping `inputs_` before pickling would reclaim almost
all of it, if nothing downstream reads it.

## Reproducing

```bash
.venv-dev/bin/python scripts/nimare_asks_repro.py
```

Sections A–E correspond to asks 1–5. The script writes its synthetic maps to a
temporary directory and prints the path.
