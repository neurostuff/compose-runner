"""Hand-roll the specifications compose would POST.

Argument names and defaults follow the frontend's ``meta_analysis_params.json``
(v0.6.1) so a disagreement with the installed NiMARE shows up as a failure
rather than being papered over. ``ibma_config_as_written`` cases send exactly
what that config lists, including arguments NiMARE may no longer take.
"""

import json
from pathlib import Path

SPECS = Path(__file__).resolve().parent / "specs"
SPECS.mkdir(exist_ok=True)

FDR = {"type": "FDRCorrector", "args": {"method": "indep", "alpha": 0.05}}
FDR_NEG = {"type": "FDRCorrector", "args": {"method": "negcorr", "alpha": 0.05}}
FWE_BONF = {
    "type": "FWECorrector",
    "args": {"method": "bonferroni", "voxel_thresh": None, "**kwargs": None},
}
FWE_MC = {
    "type": "FWECorrector",
    "args": {"method": "montecarlo", "n_iters": 50, "voxel_thresh": None},
}


def ibma(name, estimator, args, corrector=FDR, **extra):
    spec = {
        "id": f"spec-{name}",
        "type": "IBMA",
        "estimator": {"type": estimator, "args": {**args, "**kwargs": {}}},
        "corrector": corrector,
        "filter": "included",
        "conditions": [True],
        "weights": [1.0],
        "database_studyset": None,
    }
    spec.update(extra)
    return name, spec


def cbma(name, estimator, args, corrector=FDR, **extra):
    spec = {
        "id": f"spec-{name}",
        "type": "CBMA",
        "estimator": {"type": estimator, "args": {**args, "**kwargs": {}}},
        "corrector": corrector,
        "filter": "included",
        "conditions": [True],
        "weights": [1.0],
        "database_studyset": None,
    }
    spec.update(extra)
    return name, spec


CASES = [
    # --- z-map estimators, the corpus's common case ------------------------
    ibma(
        "stouffers_fdr",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
    ),
    ibma(
        "stouffers_fdr_negcorr",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
        corrector=FDR_NEG,
    ),
    ibma(
        "stouffers_fwe_bonferroni",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
        corrector=FWE_BONF,
    ),
    ibma(
        "stouffers_samplesize",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": True, "two_sided": True},
    ),
    ibma(
        "stouffers_onesided",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": False},
    ),
    ibma(
        "stouffers_aggressive_mask",
        "Stouffers",
        {"aggressive_mask": True, "use_sample_size": False, "two_sided": True},
    ),
    ibma(
        "fishers_fdr",
        "Fishers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
    ),
    ibma(
        "fishers_samplesize",
        "Fishers",
        {"aggressive_mask": False, "use_sample_size": True, "two_sided": True},
    ),
    # --- t-map estimator ---------------------------------------------------
    ibma(
        "fixedeffectshedges_fdr",
        "FixedEffectsHedges",
        {"aggressive_mask": False, "tau2": 0},
    ),
    # The config also lists weight_scheme and rho for this estimator.
    ibma(
        "fixedeffectshedges_config_as_written",
        "FixedEffectsHedges",
        {"aggressive_mask": False, "weight_scheme": "rescale", "rho": 0.8, "tau2": 0},
    ),
    # --- beta-map estimators ----------------------------------------------
    ibma(
        "permutedols_fdr",
        "PermutedOLS",
        {
            "aggressive_mask": False,
            "use_sample_size": False,
            "two_sided": True,
            "random_state": 42,
        },
    ),
    ibma(
        "permutedols_fwe_montecarlo",
        "PermutedOLS",
        {
            "aggressive_mask": False,
            "use_sample_size": False,
            "two_sided": True,
            "random_state": 42,
        },
        corrector=FWE_MC,
    ),
    ibma(
        "samplesizebasedlikelihood_ml",
        "SampleSizeBasedLikelihood",
        {"aggressive_mask": False, "method": "ml"},
    ),
    ibma(
        "samplesizebasedlikelihood_config_as_written",
        "SampleSizeBasedLikelihood",
        {
            "aggressive_mask": False,
            "weight_scheme": "rescale",
            "rho": 0.8,
            "method": "reml",
        },
    ),
    # --- beta + varcope estimators (nothing in the corpus supplies varcope) -
    ibma(
        "dersimonianlaird_fdr",
        "DerSimonianLaird",
        {"aggressive_mask": False, "weight_scheme": "rescale", "rho": 0.8},
    ),
    ibma(
        "hedges_fdr",
        "Hedges",
        {"aggressive_mask": False, "weight_scheme": "rescale", "rho": 0.8},
    ),
    ibma(
        "weightedleastsquares_fdr",
        "WeightedLeastSquares",
        {"aggressive_mask": False, "weight_scheme": "rescale", "rho": 0.8, "tau2": 0},
    ),
    ibma(
        "variancebasedlikelihood_fdr",
        "VarianceBasedLikelihood",
        {
            "aggressive_mask": False,
            "weight_scheme": "rescale",
            "rho": 0.8,
            "method": "ml",
        },
    ),
    # groupby=False is NiMARE's escape hatch from the dependence correction,
    # and the only way past a study whose contrasts cancel.
    ibma(
        "stouffers_groupby_false",
        "Stouffers",
        {
            "aggressive_mask": False,
            "use_sample_size": False,
            "two_sided": True,
            "groupby": False,
        },
    ),
    ibma(
        "fishers_groupby_false",
        "Fishers",
        {
            "aggressive_mask": False,
            "use_sample_size": False,
            "two_sided": True,
            "groupby": False,
        },
    ),
    # --- selection shapes --------------------------------------------------
    ibma(
        "stouffers_string_filter",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
        filter="analysis_group",
        conditions=["left"],
        weights=[1.0],
    ),
    ibma(
        "stouffers_two_group",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
        conditions=[True, False],
        weights=[1.0, -1.0],
    ),
    ibma(
        "stouffers_database_studyset",
        "Stouffers",
        {"aggressive_mask": False, "use_sample_size": False, "two_sided": True},
        database_studyset="neurostore",
    ),
    ibma(
        "stouffers_groupby_study",
        "Stouffers",
        {
            "aggressive_mask": False,
            "use_sample_size": False,
            "two_sided": True,
            "groupby": "study",
        },
    ),
    # --- coordinate-based, for the studysets that carry points -------------
    cbma("ale_fdr", "ALE", {"null_method": "approximate"}),
    cbma(
        "ale_fwe_montecarlo",
        "ALE",
        {"null_method": "approximate"},
        corrector={
            "type": "FWECorrector",
            "args": {"method": "montecarlo", "n_iters": 50, "voxel_thresh": None},
        },
    ),
    cbma(
        "alesubtraction_two_group",
        "ALESubtraction",
        {"n_iters": 50},
        conditions=[True, False],
        weights=[1.0, -1.0],
    ),
    cbma(
        "alesubtraction_database_neurostore",
        "ALESubtraction",
        {"n_iters": 50},
        database_studyset="neurostore",
    ),
    # The pairwise estimator a database comparison normally uses. Its kernel is
    # radius-based, so unlike ALE's it needs no per-experiment sample size --
    # which the studyset release almost never carries.
    cbma(
        "mkdachi2_two_group",
        "MKDAChi2",
        {"prior": 0.5},
        conditions=[True, False],
        weights=[1.0, -1.0],
    ),
    cbma(
        "mkdachi2_database_neurostore",
        "MKDAChi2",
        {"prior": 0.5},
        database_studyset="neurostore",
    ),
]


def main():
    for name, spec in CASES:
        (SPECS / f"{name}.json").write_text(json.dumps(spec, indent=1))
    print(f"{len(CASES)} specifications written to {SPECS}")


if __name__ == "__main__":
    main()
