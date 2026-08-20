"""Tests for resolving a specification's estimator arguments.

These pin the boundary between what compose stores and what NiMARE accepts.
NiMARE raises on an argument it does not recognize, so these mostly check that
the runner raises first -- before a studyset's worth of maps is downloaded -- and
with a message that names what replaced a retired argument. The ``n_cores``
cases are the exception: without them a run would reach NiMARE with an argument
no image-based estimator takes.
"""

import inspect

import pytest
from nimare.meta.ibma import (
    DerSimonianLaird,
    Fishers,
    PermutedOLS,
    Stouffers,
)

from compose_runner.estimator_args import (
    accepted_parameters,
    resolve_ibma_estimator_args,
)


def test_accepted_parameters_sees_inherited_arguments():
    """aggressive_mask and groupby live on the base class, not the subclass.

    inspect.signature(Stouffers) shows only its own arguments, so anything
    validating against that alone would reject the two arguments a compose
    specification is most likely to set.
    """
    accepted = accepted_parameters(Stouffers)

    assert {"aggressive_mask", "groupby", "mask", "memory"} <= accepted
    assert {"use_sample_size", "two_sided"} <= accepted


def test_unknown_argument_is_rejected():
    with pytest.raises(ValueError, match="does not accept"):
        resolve_ibma_estimator_args(Stouffers, {"tw0_sided": True})


def test_error_names_the_accepted_arguments():
    """The message has to be enough to fix the specification."""
    with pytest.raises(ValueError, match="aggressive_mask"):
        resolve_ibma_estimator_args(Stouffers, {"nonsense": 1})


def test_argument_removed_upstream_is_rejected():
    """Stouffers dropped normalize_contrast_weights in NiMARE 0.21.0.

    Rejected rather than dropped: this branch targets one NiMARE, and accepting
    the argument would report a result computed without the reweighting the
    specification asked for.
    """
    with pytest.raises(ValueError, match="normalize_contrast_weights"):
        resolve_ibma_estimator_args(Stouffers, {"normalize_contrast_weights": False})


def test_dependence_is_rejected_and_points_at_groupby():
    """The frontend config's old vocabulary must not be silently reinterpreted."""
    with pytest.raises(ValueError) as excinfo:
        resolve_ibma_estimator_args(Stouffers, {"dependence": "independent"})

    message = str(excinfo.value)
    assert "dependence" in message
    assert "groupby" in message


def test_a_retired_argument_is_caught_inside_nested_kwargs():
    """The frontend can put it under "**kwargs" instead."""
    with pytest.raises(ValueError, match="normalize_contrast_weights"):
        resolve_ibma_estimator_args(
            Stouffers, {"**kwargs": {"normalize_contrast_weights": True}}
        )


def test_groupby_passes_through_untouched():
    """A metadata field name is a legitimate groupby value."""
    resolved = resolve_ibma_estimator_args(Fishers, {"groupby": "subject_group"})

    assert resolved == {"groupby": "subject_group"}


def test_n_cores_is_dropped_for_a_serial_estimator():
    """No IBMA estimator takes n_cores, so injecting it changes nothing."""
    resolved = resolve_ibma_estimator_args(Stouffers, {}, n_cores=4)

    assert resolved == {}


def test_n_cores_becomes_n_jobs_for_permuted_ols():
    resolved = resolve_ibma_estimator_args(PermutedOLS, {}, n_cores=4)

    assert resolved == {"n_jobs": 4}


def test_an_explicit_n_jobs_wins_over_n_cores():
    """The specification is more specific than the runner's invocation."""
    resolved = resolve_ibma_estimator_args(PermutedOLS, {"n_jobs": 1}, n_cores=4)

    assert resolved == {"n_jobs": 1}


def test_nested_kwargs_key_is_flattened():
    """The compose frontend sends a literal "**kwargs" entry."""
    resolved = resolve_ibma_estimator_args(
        Stouffers, {"**kwargs": {"two_sided": False}, "use_sample_size": True}
    )

    assert resolved == {"two_sided": False, "use_sample_size": True}


def test_resample_passthrough_is_allowed():
    """Resampling options reach the base class through **kwargs by design."""
    resolved = resolve_ibma_estimator_args(
        Stouffers, {"resample__interpolation": "nearest"}
    )

    assert resolved == {"resample__interpolation": "nearest"}


def test_new_upstream_arguments_are_accepted():
    """weight_scheme and rho arrived with the dependence work."""
    resolved = resolve_ibma_estimator_args(
        DerSimonianLaird, {"weight_scheme": "collapse", "rho": 0.5}
    )

    assert resolved == {"weight_scheme": "collapse", "rho": 0.5}


def test_n_iters_belongs_to_the_corrector_not_the_estimator():
    """PermutedOLS reads n_iters in correct_fwe_montecarlo, not __init__.

    A specification that puts it on the estimator is asking for a permutation
    count that would never be applied.
    """
    with pytest.raises(ValueError, match=r"does not accept \['n_iters'\]"):
        resolve_ibma_estimator_args(PermutedOLS, {"n_iters": 100})


def _concrete_ibma_estimators():
    """Every image-based estimator NiMARE exposes, as the config generator does."""
    import nimare.meta.ibma as ibma
    from nimare.meta.ibma import IBMAEstimator

    return sorted(
        (name, cls)
        for name, cls in inspect.getmembers(ibma, inspect.isclass)
        if IBMAEstimator in inspect.getmro(cls)
        and cls is not IBMAEstimator
        and cls.__module__.startswith("nimare")
        and not getattr(cls, "__abstractmethods__", None)
    )


@pytest.mark.parametrize(
    ("name", "estimator_cls"),
    _concrete_ibma_estimators(),
    ids=lambda v: getattr(v, "__name__", v),
)
def test_every_estimator_accepts_its_own_documented_arguments(name, estimator_cls):
    """A specification built from an estimator's own defaults has to construct.

    This is what a regenerated frontend config amounts to: every documented
    argument, sent with its documented default. Parametrizing over NiMARE's
    estimators rather than a fixture means an argument added, removed or renamed
    upstream shows up here instead of in a run.
    """
    args = {}
    for cls in inspect.getmro(estimator_cls):
        init = cls.__dict__.get("__init__")
        if init is None:
            continue
        for param_name, param in inspect.signature(init).parameters.items():
            if param_name == "self" or param.kind in (
                param.VAR_POSITIONAL,
                param.VAR_KEYWORD,
            ):
                continue
            if param.default is not param.empty:
                args.setdefault(param_name, param.default)

    # IBMAEstimator.__init__ does not forward generate_description to
    # Estimator.__init__, so NiMARE ignores it and leaves the attribute True.
    # It is also undocumented on the estimators, so a generated frontend config
    # never sends it. Dropped here so this test measures the specifications
    # compose can actually produce.
    args.pop("generate_description", None)

    resolved = resolve_ibma_estimator_args(estimator_cls, args, n_cores=2)
    estimator_cls(**resolved)
