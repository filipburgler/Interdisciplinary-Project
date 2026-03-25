import numpy as np

from ..numeric.numeric_generators import generate_numeric_support


def assign_unique_values_to_single_year(pool, year_index, unique_per_year):
    pool = np.asarray(pool)
    n_unique = int(unique_per_year[year_index])
    pool_size = pool.size
    if n_unique > pool_size:
        raise ValueError("Cannot choose more uniques than pool size")
    offset = int(np.sum(np.asarray(unique_per_year[:year_index], dtype=np.int64))) % pool_size
    indices = (offset + np.arange(n_unique, dtype=np.int64)) % pool_size
    return pool[indices]


def build_all_rows_for_column(pool, years, unique_per_year, total_rows_per_year, missing_per_year, offsets, rng):
    total_rows = int(offsets[-1])
    first = build_year_rows_for_column(pool, 0, unique_per_year, total_rows_per_year, missing_per_year, rng)
    out = np.empty(total_rows, dtype=first.dtype)
    out[offsets[0] : offsets[1]] = first

    for index in range(1, len(years)):
        year_rows = build_year_rows_for_column(pool, index, unique_per_year, total_rows_per_year, missing_per_year, rng)
        if year_rows.dtype.kind != out.dtype.kind:
            raise ValueError("Wrong type of data")
        out[offsets[index] : offsets[index + 1]] = year_rows
    return out


def build_year_rows_for_column(pool, year_index, unique_per_year, total_rows_per_year, missing_per_year, rng):
    chosen_values = assign_unique_values_to_single_year(pool, year_index, unique_per_year)
    rows_in_year = int(total_rows_per_year[year_index])
    missing_rows_in_year = int(missing_per_year[year_index])
    protected = chosen_values.size

    if rows_in_year < 0 or missing_rows_in_year < 0:
        raise ValueError("Row counts and missing counts must be non-negative")
    if protected > rows_in_year:
        raise ValueError("unique_values_per_year cannot exceed total_rows_per_year")
    if protected + missing_rows_in_year > rows_in_year:
        raise ValueError("unique values plus missing rows cannot exceed total rows in a year")

    repeated_unique_values = rows_in_year - protected - missing_rows_in_year

    out = chosen_values.copy()
    if out.dtype.kind in ("i", "u", "b"):
        out = np.empty(rows_in_year, dtype=np.float64 if out.dtype.kind != "b" else object)
        out[:protected] = chosen_values
    elif out.dtype.kind in ("U", "S"):
        out = np.empty(rows_in_year, dtype=object)
        out[:protected] = chosen_values
    else:
        out = np.empty(rows_in_year, dtype=object)
        out[:protected] = chosen_values.astype(object, copy=False)

    if out.dtype == object:
        out[protected : protected + missing_rows_in_year] = None
    else:
        out[protected : protected + missing_rows_in_year] = np.nan

    if repeated_unique_values > 0:
        out[protected + missing_rows_in_year :] = rng.choice(chosen_values, size=repeated_unique_values, replace=True)

    rng.shuffle(out)
    return out


def build_numeric_rows_for_year(support_values, support_weights, year_index, unique_per_year, total_rows_per_year, missing_per_year, rng):
    rows_in_year = max(int(total_rows_per_year[year_index]), 0)
    missing_rows_in_year = max(int(missing_per_year[year_index]), 0)
    max_unique = max(rows_in_year - min(missing_rows_in_year, rows_in_year), 0)

    effective_unique_per_year = list(unique_per_year)
    effective_unique_per_year[year_index] = min(int(unique_per_year[year_index]), max_unique, len(support_values))

    chosen_values = assign_unique_values_to_single_year(support_values, year_index, effective_unique_per_year)
    chosen_weights = assign_unique_values_to_single_year(support_weights, year_index, effective_unique_per_year).astype(np.float64, copy=False)
    chosen_weights = np.maximum(chosen_weights, 0.0)

    protected = chosen_values.size
    missing_rows_in_year = min(missing_rows_in_year, max(rows_in_year - protected, 0))

    if protected == 0 and rows_in_year > missing_rows_in_year:
        chosen_values = np.asarray(support_values[:1], dtype=object)
        chosen_weights = np.array([1.0], dtype=np.float64)
        protected = 1

    if protected == 0:
        year_output = np.empty(rows_in_year, dtype=object)
        year_output[:] = None
        return year_output

    if chosen_weights.sum() == 0.0:
        chosen_weights = np.full(chosen_values.size, 1.0 / chosen_values.size, dtype=np.float64)
    else:
        chosen_weights /= chosen_weights.sum()

    repeated_unique_values = rows_in_year - protected - missing_rows_in_year
    year_output = np.empty(rows_in_year, dtype=object)
    year_output[:protected] = chosen_values.astype(object, copy=False)
    year_output[protected : protected + missing_rows_in_year] = None

    if repeated_unique_values > 0:
        year_output[protected + missing_rows_in_year :] = rng.choice(
            chosen_values,
            size=repeated_unique_values,
            replace=True,
            p=chosen_weights,
        )

    rng.shuffle(year_output)
    return year_output


def build_numeric_column(meta, years, unique_per_year, total_rows_per_year, missing_per_year, offsets, rng, large_file=False, year_index=None):
    support_values, support_weights = generate_numeric_support(meta, rng)

    if large_file:
        return build_numeric_rows_for_year(
            support_values=support_values,
            support_weights=support_weights,
            year_index=year_index,
            unique_per_year=unique_per_year,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            rng=rng,
        )

    total_rows = int(offsets[-1])
    column_output = np.empty(total_rows, dtype=object)
    for index in range(len(years)):
        year_rows = build_numeric_rows_for_year(
            support_values=support_values,
            support_weights=support_weights,
            year_index=index,
            unique_per_year=unique_per_year,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            rng=rng,
        )
        column_output[offsets[index] : offsets[index + 1]] = year_rows

    return column_output
