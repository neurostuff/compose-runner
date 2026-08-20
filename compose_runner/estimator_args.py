"""Turn a specification's estimator arguments into NiMARE constructor keywords.

Compose stores whatever the frontend collected and hands it straight to the
estimator. NiMARE now rejects a keyword it does not recognize, so a stale name no
longer produces a silently wrong result -- but two things still have to happen
here.

``n_cores`` is the load-bearing one. The runner injects it for every estimator,
and no image-based estimator accepts it, so passing it through would now raise
rather than be ignored. :class:`~nimare.meta.ibma.PermutedOLS` calls its
equivalent ``n_jobs``; the rest are serial.

The rest is a better error, earlier. Validating against the estimator's own
signature lets ``process_bundle`` reject a specification before it downloads a
studyset's worth of maps, and lets the message name what replaced an argument
rather than only saying it was unexpected.

There is nothing to be backwards compatible with -- compose cannot create an
image-based meta-analysis yet, so no stored specification names an argument an
older NiMARE had. So this targets one API, the installed NiMARE's.
"""

import inspect
import logging

LGR = logging.getLogger(__name__)

# Arguments an earlier draft of the frontend config named, and what replaced
# them. Used only to say something more useful than "not accepted" -- they are
# still rejected, because guessing what a retired argument meant is how a
# specification ends up describing an analysis nobody ran.
RETIRED_ESTIMATOR_ARGS = {
    "dependence": (
        "renamed to 'groupby' in NiMARE 0.21.0, and widened: groupby=None groups "
        "images by the study that contributed them (what 'auto' meant), "
        "groupby=False gives every image its own group (what 'independent' "
        "meant), and a string names a metadata field to group by"
    ),
    "normalize_contrast_weights": (
        "removed in NiMARE 0.21.0: images sharing a group are now combined into "
        "one variance-standardized statistic, so there is nothing left to "
        "normalize"
    ),
}

# Resampling options reach IBMAEstimator through **kwargs by design, so they
# cannot be validated against a signature.
_KWARG_PREFIXES = ("resample__",)


def accepted_parameters(estimator_cls):
    """Return every keyword an estimator's constructors name explicitly.

    Walks the MRO because the arguments that matter most for IBMA
    (``aggressive_mask``, ``groupby``, ``mask``) live on
    :class:`~nimare.meta.ibma.IBMAEstimator` and reach the subclasses through
    ``**kwargs``, so ``inspect.signature`` on the subclass cannot see them.
    """
    names = set()
    for cls in inspect.getmro(estimator_cls):
        init = cls.__dict__.get("__init__")
        if init is None:
            continue
        for name, param in inspect.signature(init).parameters.items():
            if name == "self":
                continue
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            names.add(name)
    return names


def _expand_nested_kwargs(args):
    """Flatten the literal ``"**kwargs"`` key the compose frontend sends."""
    nested = args.pop("**kwargs", None)
    if isinstance(nested, dict):
        args.update(nested)
    return args


def _resolve_n_cores(estimator_cls, args, n_cores):
    """Put the core count where this estimator actually reads it.

    Image-based estimators do not take ``n_cores``. Only PermutedOLS
    parallelizes its fit, under the name ``n_jobs``.
    """
    args.pop("n_cores", None)
    if n_cores is None:
        return args

    accepted = accepted_parameters(estimator_cls)
    if "n_jobs" in accepted:
        args.setdefault("n_jobs", n_cores)
    else:
        LGR.debug("%s does not parallelize; ignoring n_cores.", estimator_cls.__name__)
    return args


def _describe_unaccepted(estimator_cls, unknown, accepted):
    """Build the error for arguments the estimator will not apply."""
    lines = [
        f"{estimator_cls.__name__} does not accept {unknown}, so the "
        "specification has to be fixed."
    ]
    for name in unknown:
        reason = RETIRED_ESTIMATOR_ARGS.get(name)
        if reason:
            lines.append(f"  {name}: {reason}.")
    lines.append(f"Accepted arguments are {sorted(accepted)}.")
    return "\n".join(lines)


def resolve_ibma_estimator_args(estimator_cls, args, n_cores=None):
    """Return constructor keywords for an image-based estimator.

    Parameters
    ----------
    estimator_cls : :obj:`type`
        The :class:`~nimare.meta.ibma.IBMAEstimator` subclass named by the
        specification.
    args : :obj:`dict` or None
        The specification's ``estimator.args``.
    n_cores : :obj:`int`, optional
        Core count the runner was invoked with.

    Returns
    -------
    :obj:`dict`
        Keywords the estimator names explicitly, plus any ``resample__*``
        passthroughs.

    Raises
    ------
    ValueError
        If an argument is not one the estimator accepts. NiMARE would raise a
        ``TypeError`` for the same argument, but only once it is constructed;
        raising here happens before the studyset's images are fetched, and names
        what replaced an argument that used to exist.
    """
    resolved = _expand_nested_kwargs(dict(args or {}))
    resolved = _resolve_n_cores(estimator_cls, resolved, n_cores)

    accepted = accepted_parameters(estimator_cls)
    unknown = sorted(
        name
        for name in resolved
        if name not in accepted and not name.startswith(_KWARG_PREFIXES)
    )
    if unknown:
        raise ValueError(_describe_unaccepted(estimator_cls, unknown, accepted))

    return resolved
