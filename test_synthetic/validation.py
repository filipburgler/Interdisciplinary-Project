import json
import math
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

from generate_synthetic.config import (
    DEFAULT_DATA_OUTPUT_DIR,
    DEFAULT_FILE_METADATA_PAIRS,
    DEFAULT_METADATA_DIR,
    DEFAULT_VALIDATION_REPORT_PATH,
    LARGE_FILE_TMP_FOLDER,
)
from generate_synthetic.pipeline.pipeline_core import filter_metadata_by_panel_structure
from generate_synthetic.types.type_detection import detect_var_type


@dataclass
class Difference:
    dataset: str
    metric: str
    expected: object
    observed: object
    column: str | None = None
    detail: str | None = None

    def format(self):
        prefix = f"{self.dataset}"
        if self.column:
            prefix = f"{prefix}.{self.column}"
        line = f"- {prefix} [{self.metric}] expected={self.expected} observed={self.observed}"
        if self.detail:
            line = f"{line} ({self.detail})"
        return line


@dataclass
class Note:
    dataset: str
    metric: str
    column: str | None = None
    detail: str | None = None

    def format(self):
        prefix = f"{self.dataset}"
        if self.column:
            prefix = f"{prefix}.{self.column}"
        line = f"- {prefix} [{self.metric}]"
        if self.detail:
            line = f"{line} {self.detail}"
        return line


@dataclass
class DatasetValidationResult:
    dataset: str
    metadata_file: str
    differences: list[Difference] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)
    checked_columns: int = 0

    @property
    def ok(self):
        return not self.differences


@dataclass
class ValidationReport:
    dataset_results: list[DatasetValidationResult]
    report_path: Path | None = None

    @property
    def difference_count(self):
        return sum(len(result.differences) for result in self.dataset_results)

    @property
    def ok(self):
        return self.difference_count == 0


def _load_filtered_metadata(metadata_path):
    with open(metadata_path, "r", encoding="utf-8") as file_obj:
        metadata = json.load(file_obj)
    return filter_metadata_by_panel_structure(metadata)


def _is_missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def _normalize_year_map(values, years):
    return {int(year): int(value) for year, value in zip(years, values)}


def _normalize_level(value):
    if hasattr(value, "item"):
        value = value.item()
    return str(value)


def _normalize_numeric_summary(summary):
    if isinstance(summary, dict):
        return summary
    if isinstance(summary, list) and summary:
        return summary[0]
    return {}


def _compare_value(result, metric, expected, observed, column=None, detail=None):
    if expected != observed:
        result.differences.append(
            Difference(
                dataset=result.dataset,
                column=column,
                metric=metric,
                expected=expected,
                observed=observed,
                detail=detail,
            )
        )


def _add_note(result, metric, column=None, detail=None):
    result.notes.append(
        Note(
            dataset=result.dataset,
            column=column,
            metric=metric,
            detail=detail,
        )
    )


def _compare_float(result, metric, expected, observed, column=None, tolerance=1e-6, note_threshold=0.15):
    if expected is None and observed is None:
        return
    if expected is None or observed is None:
        _compare_value(result, metric, expected, observed, column=column)
        return
    expected_value = float(expected)
    observed_value = float(observed)
    if math.isclose(expected_value, observed_value, rel_tol=tolerance, abs_tol=tolerance):
        return

    baseline = max(abs(expected_value), tolerance)
    relative_gap = abs(observed_value - expected_value) / baseline
    if relative_gap <= note_threshold:
        _add_note(
            result,
            metric,
            column=column,
            detail="within 15% of metadata",
        )
        return

    if not math.isclose(expected_value, observed_value, rel_tol=tolerance, abs_tol=tolerance):
        result.differences.append(
            Difference(
                dataset=result.dataset,
                column=column,
                metric=metric,
                expected=round(expected_value, 6),
                observed=round(observed_value, 6),
            )
        )


def _new_column_accumulator(years, meta, var_type):
    return {
        "missing_per_year": Counter({int(year): 0 for year in years}),
        "unique_per_year": {int(year): set() for year in years},
        "all_unique": set(),
        "level_counts": Counter(),
        "numeric": var_type == "numeric",
        "collect_level_counts": bool(meta.get("level_counts")),
        "sum_value": 0.0,
        "value_count": 0,
        "min": None,
        "max": None,
    }


def _finalize_column_accumulator(accumulator):
    return {
        "missing_per_year": dict(sorted(accumulator["missing_per_year"].items())),
        "unique_per_year": {year: len(values) for year, values in sorted(accumulator["unique_per_year"].items())},
        "unique_values_per_year": {
            year: sorted(_normalize_level(value) for value in values)
            for year, values in sorted(accumulator["unique_per_year"].items())
        },
        "total_missing": int(sum(accumulator["missing_per_year"].values())),
        "total_unique_values": len(accumulator["all_unique"]),
        "level_counts": dict(sorted(accumulator["level_counts"].items())),
        "mean": (
            accumulator["sum_value"] / accumulator["value_count"] if accumulator["value_count"] else None
        ),
        "min": accumulator["min"],
        "max": accumulator["max"],
    }


def _collect_dataset_stats(parquet_path, metadata, observed_columns, years):
    parquet_file = pq.ParquetFile(parquet_path)
    tracked_columns = [column_name for column_name in metadata if column_name in observed_columns]
    accumulators = {
        column_name: _new_column_accumulator(years, metadata[column_name], detect_var_type(metadata[column_name]))
        for column_name in tracked_columns
    }
    year_counts = Counter()

    for batch in parquet_file.iter_batches(batch_size=100_000, columns=["year", *tracked_columns]):
        batch_years = [int(year) for year in batch.column(0).to_pylist()]
        for year in batch_years:
            year_counts[year] += 1

        for column_offset, column_name in enumerate(tracked_columns, start=1):
            accumulator = accumulators[column_name]
            batch_values = batch.column(column_offset).to_pylist()
            for year, value in zip(batch_years, batch_values):
                if _is_missing(value):
                    accumulator["missing_per_year"][year] += 1
                    continue
                accumulator["all_unique"].add(value)
                accumulator["unique_per_year"][year].add(value)
                if accumulator["collect_level_counts"]:
                    accumulator["level_counts"][_normalize_level(value)] += 1
                if accumulator["numeric"]:
                    numeric_value = float(value)
                    accumulator["sum_value"] += numeric_value
                    accumulator["value_count"] += 1
                    accumulator["min"] = numeric_value if accumulator["min"] is None else min(accumulator["min"], numeric_value)
                    accumulator["max"] = numeric_value if accumulator["max"] is None else max(accumulator["max"], numeric_value)

    return {
        "year_counts": dict(sorted(year_counts.items())),
        "total_rows": parquet_file.metadata.num_rows,
        "column_stats": {
            column_name: _finalize_column_accumulator(accumulator)
            for column_name, accumulator in accumulators.items()
        },
    }


def _compare_yearly_counts(result, metric, expected_by_year, observed_by_year, column=None):
    all_years = sorted(set(expected_by_year) | set(observed_by_year))
    for year in all_years:
        expected_value = expected_by_year.get(year)
        observed_value = observed_by_year.get(year)
        if expected_value != observed_value:
            result.differences.append(
                Difference(
                    dataset=result.dataset,
                    column=column,
                    metric=metric,
                    expected=expected_value,
                    observed=observed_value,
                    detail=f"year={year}",
                )
            )


def compare_dataset_to_metadata(file_name, metadata_name, data_dir=None, metadata_dir=None):
    data_dir = Path(data_dir or DEFAULT_DATA_OUTPUT_DIR)
    metadata_dir = Path(metadata_dir or DEFAULT_METADATA_DIR)
    parquet_path = data_dir / file_name
    metadata_path = metadata_dir / metadata_name
    result = DatasetValidationResult(dataset=file_name, metadata_file=metadata_name)

    if not parquet_path.exists():
        result.differences.append(
            Difference(file_name, "dataset_exists", True, False, detail=f"missing file at {parquet_path}")
        )
        return result

    if not metadata_path.exists():
        result.differences.append(
            Difference(file_name, "metadata_exists", True, False, detail=f"missing file at {metadata_path}")
        )
        return result

    metadata = _load_filtered_metadata(metadata_path)
    years = [int(year) for year in next(iter(metadata.values()))["years_available"]]
    expected_year_counts = _normalize_year_map(next(iter(metadata.values()))["total_rows_per_year"], years)
    observed_columns = pq.ParquetFile(parquet_path).schema_arrow.names
    expected_columns = {"year", *metadata.keys()}
    dataset_stats = _collect_dataset_stats(parquet_path, metadata, observed_columns, years)

    _compare_value(result, "columns_missing", [], sorted(expected_columns - set(observed_columns)))
    _compare_value(result, "columns_extra", [], sorted(set(observed_columns) - expected_columns))
    _compare_value(result, "total_rows", sum(expected_year_counts.values()), dataset_stats["total_rows"])
    _compare_value(result, "year_counts", expected_year_counts, dataset_stats["year_counts"])

    for column_name, meta in metadata.items():
        if column_name not in observed_columns:
            continue
        var_type = detect_var_type(meta)
        column_stats = dataset_stats["column_stats"][column_name]
        result.checked_columns += 1

        expected_missing = _normalize_year_map(meta["missing_per_year"], years)
        expected_unique = _normalize_year_map(meta["unique_values_per_year"], years)

        _compare_value(result, "total_missing", int(meta["total_missing"]), column_stats["total_missing"], column=column_name)
        _compare_value(
            result,
            "missing_per_year",
            expected_missing,
            column_stats["missing_per_year"],
            column=column_name,
        )
        _compare_value(
            result,
            "total_unique_values",
            int(meta["total_unique_values"]),
            column_stats["total_unique_values"],
            column=column_name,
        )
        _compare_yearly_counts(result, "unique_values_per_year", expected_unique, column_stats["unique_per_year"], column=column_name)

        if meta.get("level_counts"):
            expected_levels = {str(key): int(value) for key, value in dict(meta["level_counts"]).items()}
            _compare_value(result, "level_counts", expected_levels, column_stats["level_counts"], column=column_name)

        if var_type == "numeric" and meta.get("numeric_summary"):
            numeric_summary = _normalize_numeric_summary(meta["numeric_summary"])
            _compare_float(result, "numeric_mean", numeric_summary.get("mean"), column_stats["mean"], column=column_name)
            _compare_float(result, "numeric_min", numeric_summary.get("m_min"), column_stats["min"], column=column_name)
            _compare_float(result, "numeric_max", numeric_summary.get("m_max"), column_stats["max"], column=column_name)

        if var_type == "year_like":
            observed_year_values = column_stats["unique_values_per_year"]
            expected_year_values = {year: [str(year)] for year in years}
            _compare_value(
                result,
                "year_like_values",
                expected_year_values,
                observed_year_values,
                column=column_name,
                detail="unique non-missing value per year should equal the year itself",
            )

    return result


def format_validation_report(report):
    lines = ["Synthetic Metadata Validation Report", ""]
    lines.append(f"Datasets checked: {len(report.dataset_results)}")
    lines.append(f"Total differences: {report.difference_count}")
    lines.append("")

    for dataset_result in report.dataset_results:
        status = "OK" if dataset_result.ok else "DIFFERENCES FOUND"
        lines.append(f"{dataset_result.dataset} vs {dataset_result.metadata_file}: {status}")
        lines.append(f"Columns checked: {dataset_result.checked_columns}")
        if dataset_result.differences:
            lines.extend(diff.format() for diff in dataset_result.differences)
        else:
            lines.append("- no differences")
        if dataset_result.notes:
            lines.append("Notes:")
            lines.extend(note.format() for note in dataset_result.notes)
        lines.append("")

    if report.report_path:
        lines.append(f"Report path: {report.report_path}")
    return "\n".join(lines).rstrip()


def write_validation_report(report, report_path=None):
    output_path = Path(report_path or report.report_path or DEFAULT_VALIDATION_REPORT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(format_validation_report(report) + "\n", encoding="utf-8")
    report.report_path = output_path
    return output_path


def run_full_validation(file_metadata_pairs=None, fail_on_differences=False, report_path=None):
    dataset_results = []
    for file_name, metadata_name in (file_metadata_pairs or DEFAULT_FILE_METADATA_PAIRS):
        dataset_results.append(compare_dataset_to_metadata(file_name, metadata_name))

    parts_dir = Path(DEFAULT_DATA_OUTPUT_DIR) / LARGE_FILE_TMP_FOLDER
    if parts_dir.exists():
        temp_result = DatasetValidationResult(dataset=str(parts_dir), metadata_file="-")
        temp_result.differences.append(
            Difference(str(parts_dir), "temporary_parts_dir", False, True, detail="temporary chunk directory still exists")
        )
        dataset_results.append(temp_result)

    report = ValidationReport(dataset_results=dataset_results)
    write_validation_report(report, report_path=report_path or DEFAULT_VALIDATION_REPORT_PATH)

    if fail_on_differences and not report.ok:
        raise AssertionError(format_validation_report(report))

    return report
