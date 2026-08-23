"""Report what an image-based meta-analysis used, after it has run.

:class:`~nimare.workflows.IBMAWorkflow` converts the maps it can and silently
drops the rest, so a meta-analysis can rest on a fraction of the studies the
user selected without saying so. Nothing here repeats that work; the fitted
estimator already holds the answer in ``inputs_["id"]`` (the analyses that made
it in) and ``estimator.dataset.images`` (the image table after the transform,
which compared against the submitted table says what was converted).

``estimator.dataset`` rather than ``estimator.studyset_``, which NiMARE also
keeps: ``studyset_`` is narrowed to the analyses that satisfied every
requirement, so an analysis whose map NiMARE *did* convert but which was dropped
for some other reason -- no sample size, say -- has no row in it, and would be
reported as one NiMARE could not convert. ``dataset`` is the whole transformed
studyset, which is the comparison this wants.
"""

import logging
from dataclasses import dataclass, field

import pandas as pd

LGR = logging.getLogger(__name__)

# Columns of an image table that describe the row rather than name a map.
_NON_IMAGE_COLUMNS = frozenset({"id", "study_id", "contrast_id", "space"})


def estimator_requirements(estimator):
    """Read an estimator's ``_required_inputs`` as image and metadata targets."""
    required = getattr(estimator, "_required_inputs", {}) or {}
    images, metadata = [], []
    for input_name, (kind, target) in required.items():
        target = target or input_name
        if kind == "image":
            images.append(target)
        elif kind == "metadata":
            metadata.append(target)
    return tuple(sorted(set(images))), tuple(sorted(set(metadata)))


def _held_types(images_df):
    """Map each row's full id to the image types it holds.

    ``notna`` rather than truthiness: the table has a column per type across the
    whole studyset, and an absent path comes back as ``NaN``, which is truthy.
    """
    if images_df is None:
        return {}
    columns = [
        column
        for column in images_df.columns
        if column not in _NON_IMAGE_COLUMNS and not column.endswith("__relative")
    ]
    return {
        row["id"]: frozenset(column for column in columns if pd.notna(row.get(column)))
        for _, row in images_df.iterrows()
    }


@dataclass
class AnalysisCoverage:
    """Whether one analysis contributed, and what became of its maps."""

    study_id: str
    study_name: str
    analysis_id: str
    analysis_name: str
    #: Image types the analysis supplied itself.
    supplied: frozenset = frozenset()
    #: Targets NiMARE produced by converting something the analysis did supply.
    derived: frozenset = frozenset()
    #: Targets NiMARE could not produce for this analysis.
    missing_images: frozenset = frozenset()
    #: Metadata the estimator requires, when that is the remaining explanation.
    missing_metadata: frozenset = frozenset()
    #: Maps dropped before NiMARE saw them, with a reason for each.
    dropped_maps: tuple = ()
    included: bool = False

    @property
    def reason(self):
        """One line saying how the analysis qualified, or why it did not."""
        if self.included:
            if self.derived:
                return f"converted {', '.join(sorted(self.derived))}"
            return "supplied the required maps"

        parts = []
        if self.missing_images:
            had = ", ".join(sorted(self.supplied)) or "no usable maps"
            parts.append(
                f"NiMARE could not produce {', '.join(sorted(self.missing_images))} "
                f"from {had}"
            )
        elif self.missing_metadata:
            parts.append(
                f"has the maps but was dropped; check "
                f"{', '.join(sorted(self.missing_metadata))}"
            )
        else:
            parts.append("dropped by NiMARE as invalid")
        if self.dropped_maps:
            parts.append(f"dropped {'; '.join(self.dropped_maps)}")
        return "; ".join(parts)


@dataclass
class CoverageReport:
    """What an estimator saw, and what it did not, across a whole studyset."""

    image_targets: tuple = ()
    metadata_targets: tuple = ()
    analyses: list = field(default_factory=list)

    @property
    def included(self):
        return [a for a in self.analyses if a.included]

    @property
    def excluded(self):
        return [a for a in self.analyses if not a.included]

    @property
    def n_studies_included(self):
        """Independent studies backing the result."""
        return len({a.study_id for a in self.included})

    def summary(self):
        """A short, log-friendly account of the whole studyset."""
        needs = f"images {list(self.image_targets)}"
        if self.metadata_targets:
            needs += f" and metadata {list(self.metadata_targets)}"
        lines = [
            f"Estimator needs {needs}.",
            f"Used {len(self.included)} of {len(self.analyses)} analyses, "
            f"from {self.n_studies_included} study/studies.",
        ]

        counts = {}
        for analysis in self.included:
            for target in analysis.derived:
                counts[target] = counts.get(target, 0) + 1
        if counts:
            described = ", ".join(f"{target} for {n}" for target, n in sorted(counts.items()))
            lines.append(f"NiMARE converted: {described}.")

        if self.excluded:
            grouped = {}
            for analysis in self.excluded:
                grouped.setdefault(analysis.reason, []).append(analysis)
            lines.append(f"Left out {len(self.excluded)} analysis/analyses:")
            for reason, group in sorted(grouped.items(), key=lambda kv: -len(kv[1])):
                names = ", ".join(
                    f"{a.study_name or a.study_id}/{a.analysis_name or a.analysis_id}"
                    for a in group[:3]
                )
                more = f", and {len(group) - 3} more" if len(group) > 3 else ""
                lines.append(f"  {len(group):>4} x {reason} -- {names}{more}")

        return "\n".join(lines)

    def to_tsv(self):
        """Render the per-analysis verdict, for writing alongside the results."""
        header = [
            "study_id",
            "study_name",
            "analysis_id",
            "analysis_name",
            "included",
            "supplied_maps",
            "converted_maps",
            "missing_maps",
            "dropped_maps",
            "reason",
        ]
        rows = ["\t".join(header)]
        for a in self.analyses:
            rows.append(
                "\t".join(
                    [
                        str(a.study_id or ""),
                        str(a.study_name or ""),
                        str(a.analysis_id or ""),
                        str(a.analysis_name or ""),
                        "true" if a.included else "false",
                        ",".join(sorted(a.supplied)),
                        ",".join(sorted(a.derived)),
                        ",".join(sorted(a.missing_images)),
                        "; ".join(a.dropped_maps),
                        a.reason,
                    ]
                )
            )
        return "\n".join(rows) + "\n"


def _build(studyset, estimator, before, after, fitted_ids, dropped_maps):
    """Assemble the report from the two tables and the fitted id list."""
    image_targets, metadata_targets = estimator_requirements(estimator)
    dropped_maps = dropped_maps or {}

    report = CoverageReport(image_targets=image_targets, metadata_targets=metadata_targets)
    for study in studyset.studies:
        for analysis in study.analyses:
            # NiMARE keys images by "<study_id>-<analysis_id>". Rebuilt rather
            # than split back out of inputs_["id"], where a hyphen in either id
            # would make the split ambiguous.
            full_id = f"{study.id}-{analysis.id}"
            supplied = before.get(full_id, frozenset())
            produced = after.get(full_id, supplied)
            available = {target for target in image_targets if target in produced}

            report.analyses.append(
                AnalysisCoverage(
                    study_id=study.id,
                    study_name=getattr(study, "name", None),
                    analysis_id=analysis.id,
                    analysis_name=getattr(analysis, "name", None),
                    supplied=frozenset(supplied),
                    derived=frozenset(available - supplied),
                    missing_images=frozenset(set(image_targets) - available),
                    missing_metadata=frozenset(
                        key for key in metadata_targets if not (analysis.metadata or {}).get(key)
                    ),
                    dropped_maps=tuple(dropped_maps.get(analysis.id, ())),
                    included=full_id in fitted_ids,
                )
            )

    return report


def describe_result(results, studyset, dropped_maps=None):
    """Report what a fitted meta-analysis used, and what it left out.

    Parameters
    ----------
    results : :obj:`~nimare.results.MetaResult`
        The fitted result. Its estimator is read, not refitted.
    studyset : :obj:`~nimare.nimads.Studyset`
        The studyset that was submitted, before NiMARE transformed anything.
    dropped_maps : :obj:`dict`, optional
        Analysis id to descriptions of maps that never reached NiMARE, as
        recorded while the images were staged.

    Returns
    -------
    :obj:`CoverageReport`
    """
    estimator = results.estimator
    fitted_ids = {str(image_id) for image_id in estimator.inputs_.get("id", [])}
    transformed = getattr(estimator, "dataset", None)

    return _build(
        studyset,
        estimator,
        before=_held_types(studyset.images),
        after=_held_types(getattr(transformed, "images", None)),
        fitted_ids=fitted_ids,
        dropped_maps=dropped_maps,
    )


def describe_estimator(studyset, estimator, dropped_maps=None):
    """Report coverage from an estimator that collected its inputs but failed.

    Between ``describe_result``, which needs a ``MetaResult``, and
    ``describe_submission``, which assumes nothing was transformed: a fit that
    raises after ``_preprocess_input`` has both answers on the estimator
    already, and describing it as an untransformed submission would report
    every converted analysis as one NiMARE could not convert.
    """
    fitted_ids = {
        str(image_id)
        for image_id in (getattr(estimator, "inputs_", None) or {}).get("id", [])
    }
    transformed = getattr(estimator, "dataset", None)

    return _build(
        studyset,
        estimator,
        before=_held_types(studyset.images),
        after=_held_types(getattr(transformed, "images", None)),
        fitted_ids=fitted_ids,
        dropped_maps=dropped_maps,
    )


def describe_submission(studyset, estimator, dropped_maps=None):
    """Report what was submitted, for when the fit never got far enough to ask.

    NiMARE raises ``No images were found for a required input`` when nothing
    survives, and at that point there is no fitted estimator to read.
    """
    return _build(
        studyset,
        estimator,
        before=_held_types(studyset.images),
        after={},
        fitted_ids=frozenset(),
        dropped_maps=dropped_maps,
    )
