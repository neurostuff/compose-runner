"""Fill in the analysis metadata that IBMA estimators need.

``FixedEffectsHedges`` and ``SampleSizeBasedLikelihood`` require a
``sample_sizes`` entry per analysis, and it also unlocks
``Stouffers(use_sample_size=True)`` and NiMARE's z<->t conversions. Compose
records sample size on the annotation note or on the study rather than on the
analysis, so it has to be copied across before the Dataset is built.
"""

import logging

LGR = logging.getLogger(__name__)

# Keys that have been used for sample size across compose and neurostore.
SAMPLE_SIZE_KEYS = ("sample_sizes", "sample_size", "n", "sample-size")


def _coerce_sample_size(value):
    """Return a positive numeric sample size, or None.

    Annotation columns are free-form, so a value can be a string or nonsense.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0 or number != number:  # reject non-positive and NaN
        return None
    return number


def _coerce_sample_sizes(value):
    """Return a list of valid sample sizes from a scalar or a sequence.

    NiMARE wants a sequence, compose records a scalar, so accept both.
    """
    if isinstance(value, (list, tuple)):
        numbers = [_coerce_sample_size(item) for item in value]
        numbers = [number for number in numbers if number is not None]
        return numbers or None

    number = _coerce_sample_size(value)
    return [number] if number is not None else None


def _first_sample_size(source):
    """Pull sample sizes out of a metadata or note dict."""
    if not isinstance(source, dict):
        return None
    for key in SAMPLE_SIZE_KEYS:
        numbers = _coerce_sample_sizes(source.get(key))
        if numbers is not None:
            return numbers
    return None


def _notes_by_analysis(annotation_dict):
    """Index annotation notes by the analysis they describe."""
    notes = {}
    for note in (annotation_dict or {}).get("notes") or []:
        analysis_id = note.get("analysis")
        if analysis_id is not None:
            notes[analysis_id] = note.get("note") or {}
    return notes


def apply_sample_sizes(studyset_dict, annotation_dict=None):
    """Copy sample sizes onto each analysis's metadata.

    Precedence is annotation note, then analysis metadata, then study metadata:
    the annotation is the most specific of the three.

    Parameters
    ----------
    studyset_dict : :obj:`dict`
        A NIMADS studyset. Modified in place and returned.
    annotation_dict : :obj:`dict`, optional
        The matching NIMADS annotation, if there is one.

    Returns
    -------
    :obj:`dict`
        The studyset, with ``metadata['sample_sizes']`` filled in wherever a
        sample size could be found.
    """
    notes = _notes_by_analysis(annotation_dict)

    n_filled = 0
    n_missing = 0
    for study in studyset_dict.get("studies") or []:
        study_metadata = study.get("metadata") or {}
        for analysis in study.get("analyses") or []:
            metadata = analysis.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
                analysis["metadata"] = metadata

            # Already in the shape NiMARE wants, so leave it alone.
            if _coerce_sample_sizes(metadata.get("sample_sizes")) is not None:
                n_filled += 1
                continue

            sample_sizes = (
                _first_sample_size(notes.get(analysis.get("id")))
                or _first_sample_size(metadata)
                or _first_sample_size(study_metadata)
            )

            if sample_sizes is None:
                n_missing += 1
                continue

            metadata["sample_sizes"] = sample_sizes
            n_filled += 1

    if n_missing:
        LGR.warning(
            "%d analysis/analyses have no sample size; estimators that require "
            "one will drop them.",
            n_missing,
        )
    LGR.info("Sample sizes available for %d analysis/analyses.", n_filled)

    return studyset_dict
