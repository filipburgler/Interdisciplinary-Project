from collections import Counter
from hashlib import blake2b
from pathlib import Path
from shutil import rmtree

import numpy as np
import pyarrow as pa

from ..config import LARGE_FILE_ROW_CUTOFF, LARGE_FILE_TMP_FOLDER
from ..finite.finite_generators import build_column_for_finite, build_year_for_finite, generate_finite_categorical_column_math
from ..pool_generators import (
    generate_coded_pool,
    generate_logical_pool,
    generate_number_categorical_pool,
    return_n_countries,
)
from ..rows.row_builders import build_all_rows_for_column, build_numeric_column, build_year_rows_for_column
from ..types.type_detection import detect_category_type, detect_var_type
from .io_utils import load_metadata_file, merge_large_parquet, save_df


def make_rng(global_seed, var_name):
    digest = blake2b(f"{global_seed}:{var_name}".encode("utf-8"), digest_size=8).digest()
    seed = int.from_bytes(digest, byteorder="little", signed=False)
    return np.random.default_rng(seed)


def compute_row_layout(metadata):
    metadata_items = list(metadata.values())
    ref = metadata_items[0]
    years = np.array(ref["years_available"], dtype=int)
    rows_per_year = np.array(ref["total_rows_per_year"], dtype=int)

    for name, meta in metadata.items():
        if list(meta["years_available"]) != list(ref["years_available"]):
            raise ValueError(f"Variable {name} has inconsistent years_available")
        if list(meta["total_rows_per_year"]) != list(ref["total_rows_per_year"]):
            raise ValueError(f"Variable {name} has inconsistent total_rows_per_year")

    offsets = np.empty(len(rows_per_year) + 1, dtype=np.int64)
    offsets[0] = 0
    np.cumsum(rows_per_year, out=offsets[1:])
    total_rows = int(offsets[-1])
    return years, rows_per_year, offsets, total_rows


def generate_year_like_column(meta, years, total_rows_per_year, offsets, large_file=False, year_index=None):
    dtype = object
    if str(meta.get("class", "")).startswith("integer"):
        dtype = np.int32

    if large_file:
        year_value = years[year_index]
        return np.full(total_rows_per_year[year_index], year_value, dtype=dtype)

    out = np.empty(int(offsets[-1]), dtype=dtype)
    for index, year_value in enumerate(years):
        out[offsets[index] : offsets[index + 1]] = year_value
    return out


def generate_column(meta, var_name, years, total_rows_per_year, missing_per_year, offsets, seed, large_file=False, year_index=None):
    unique_values = meta["total_unique_values"]
    unique_per_year = meta["unique_values_per_year"]
    rng = make_rng(seed, var_name)
    vtype = detect_var_type(meta)

    if isinstance(unique_per_year, int):
        raise ValueError(f"Variable {var_name} has scalar unique_values_per_year; expected a per-year sequence")

    if np.sum(unique_per_year) == 0:
        return None

    if vtype == "year_like":
        return generate_year_like_column(
            meta=meta,
            years=years,
            total_rows_per_year=total_rows_per_year,
            offsets=offsets,
            large_file=large_file,
            year_index=year_index,
        )

    if vtype == "logical":
        pool = generate_logical_pool(meta)
    elif vtype == "numeric":
        return build_numeric_column(
            meta=meta,
            years=years,
            unique_per_year=unique_per_year,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            offsets=offsets.copy(),
            rng=rng,
            large_file=large_file,
            year_index=year_index,
        )
    elif vtype == "coded_countries":
        pool = return_n_countries(unique_values, rng)
    elif vtype == "coded_categorical":
        param_key, overlap_ratio = detect_category_type(var_name)
        pool = generate_coded_pool(unique_values, rng, param_key, overlap_ratio)
    elif vtype == "number_categorical":
        pool = generate_number_categorical_pool(unique_values, rng)
    elif vtype == "finite_categorical":
        levels_per_year, levels = generate_finite_categorical_column_math(meta, rng)
        if large_file:
            return build_year_for_finite(
                year_index=year_index,
                levels_per_year=levels_per_year,
                levels=levels,
                total_rows_per_year=total_rows_per_year,
                missing_per_year=missing_per_year,
                rng=rng,
            )
        return build_column_for_finite(
            levels_per_year=levels_per_year,
            levels=levels,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            offsets=offsets.copy(),
            rng=rng,
        )
    else:
        raise ValueError(f"Unsupported variable type {vtype} for {var_name}")

    if large_file:
        return build_year_rows_for_column(
            pool=pool,
            year_index=year_index,
            unique_per_year=unique_per_year,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            rng=rng,
        )

    return build_all_rows_for_column(
        pool=pool,
        years=years,
        unique_per_year=unique_per_year,
        total_rows_per_year=total_rows_per_year,
        missing_per_year=missing_per_year,
        offsets=offsets.copy(),
        rng=rng,
    )


def build_dataframe(metadata, seed=42, large_file=False, year_index=None):
    years, total_rows_per_year, offsets, total_rows = compute_row_layout(metadata)

    if large_file and year_index is None:
        raise ValueError("year_index must be provided when large_file=True")

    columns = {}
    if large_file:
        columns["year"] = np.full(
            shape=total_rows_per_year[year_index],
            fill_value=years[year_index],
            dtype=np.int16,
        )
    else:
        years_col = np.empty(total_rows, dtype=np.int16)
        for index, year_value in enumerate(years):
            years_col[offsets[index] : offsets[index + 1]] = year_value
        columns["year"] = years_col

    for var_name, meta in metadata.items():
        column = generate_column(
            meta=meta,
            var_name=var_name,
            years=years,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=meta["missing_per_year"],
            offsets=offsets,
            seed=seed,
            large_file=large_file,
            year_index=year_index,
        )
        if column is None:
            print(f"Skipped variable {var_name} since all rows are missing")
            continue
        columns[var_name] = column
    return pa.Table.from_pydict(columns)


def generate_synthetic_from_dict(metadata, save_path, name, large_file=None, seed=42):
    _, _, _, total_rows = compute_row_layout(metadata)

    if large_file is None:
        large_file = total_rows >= LARGE_FILE_ROW_CUTOFF

    if large_file:
        tmp_dir = Path(save_path) / LARGE_FILE_TMP_FOLDER
        if tmp_dir.exists():
            rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        print("File will be saved as a series of DataFrames, each containing one year of data")

        years = np.array(next(iter(metadata.values()))["years_available"], dtype=int)
        for year_index in range(len(years)):
            df = build_dataframe(metadata, seed=seed, large_file=True, year_index=year_index)
            log_path = save_df(df, save_path, name, large_file=True, year=years[year_index])
            print("Saved to", log_path)

        merge_large_parquet(save_path, name, True)
        return Path(save_path) / f"{name}.parquet"

    df = build_dataframe(metadata, seed=seed, large_file=False)
    log_path = save_df(df, save_path, name, large_file=False)
    print("Saved to", log_path)
    return log_path


def normalize_type(values):
    if isinstance(values, (list, tuple)):
        return tuple(values)
    return (values,)


def filter_metadata_by_panel_structure(metadata):
    signatures = []

    for meta in metadata.values():
        years = normalize_type(meta.get("years_available", []))
        rows = normalize_type(meta.get("total_rows_per_year", []))
        signatures.append((years, rows))

    majority_signature = Counter(signatures).most_common(1)[0][0]
    print(f"Available years panel: {majority_signature[0]}")

    filtered = {}
    for var, meta in metadata.items():
        sig = (
            normalize_type(meta.get("years_available", [])),
            normalize_type(meta.get("total_rows_per_year", [])),
        )
        if sig == majority_signature:
            filtered[var] = meta
        else:
            print(f"Skipping {var}: panel structure mismatch")
    return filtered


def generate_synthetic_from_metadata(metadata_path, save_path, name, large_file=None, seed=42):
    print(f"Creating {name} dataframe...")
    metadata = load_metadata_file(metadata_path)
    metadata = filter_metadata_by_panel_structure(metadata)
    return generate_synthetic_from_dict(metadata, save_path, name, large_file=large_file, seed=seed)
