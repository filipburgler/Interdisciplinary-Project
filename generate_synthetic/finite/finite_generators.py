import numpy as np

from .finite_math import build_b_matrix_capacity_aware, solve_a_with_constraints


def generate_finite_categorical_column_math(meta, rng):
    unique_per_year = meta["unique_values_per_year"]
    total_rows_per_year = meta["total_rows_per_year"]
    missing_per_year = meta["missing_per_year"]
    non_missing_per_year = [rows - missing for rows, missing in zip(total_rows_per_year, missing_per_year)]
    level_counts = dict(meta["level_counts"])

    levels = list(level_counts.keys())
    global_counts = np.array([level_counts[level] for level in levels], dtype=float)
    row_demand = np.array(non_missing_per_year, dtype=float)
    unique_demand = np.array(unique_per_year, dtype=int)

    max_attempts = 5
    last_error = None
    for _ in range(max_attempts):
        try:
            b = build_b_matrix_capacity_aware(
                x=row_demand,
                y=unique_demand,
                u=global_counts,
                rng=rng,
            )
            break
        except ValueError as error:
            last_error = error
            rng = np.random.default_rng(rng.integers(0, 2**32))
    else:
        raise last_error

    a = solve_a_with_constraints(b, row_demand, global_counts)
    counts = a * global_counts
    counts_floor = np.floor(counts).astype(int)
    row_count = np.array(row_demand - counts_floor.sum(axis=1))
    level_deficit = np.array(global_counts - counts_floor.sum(axis=0))

    worst_case = max(level_deficit)
    for year_index in range(len(row_count)):
        if row_count[year_index] < 0:
            while row_count[year_index] < worst_case:
                removable = [
                    level_index
                    for level_index in range(len(levels))
                    if counts_floor[year_index, level_index] > 0 and counts_floor[:, level_index].sum() > 1
                ]
                if not removable:
                    break

                level_index = max(removable, key=lambda idx: counts_floor[:, idx].sum())
                counts_floor[year_index, level_index] -= 1
                row_count[year_index] += 1
                level_deficit[level_index] += 1

    def level_priority(level_index):
        available_years = sum(
            1
            for year_index in range(len(row_count))
            if row_count[year_index] > 0 and b[year_index, level_index] == 1
        )
        return (available_years, level_deficit[level_index])

    for level_index in sorted(range(len(levels)), key=level_priority):
        while level_deficit[level_index] > 0:
            eligible_years = [
                year_index
                for year_index in range(len(row_count))
                if row_count[year_index] > 0 and b[year_index, level_index] == 1
            ]
            if not eligible_years:
                break

            def year_priority(year_index):
                return sum(
                    1
                    for other_level in range(len(levels))
                    if level_deficit[other_level] > 0 and b[year_index, other_level] == 1
                )

            chosen_year = min(eligible_years, key=year_priority)
            counts_floor[chosen_year, level_index] += 1
            row_count[chosen_year] -= 1
            level_deficit[level_index] -= 1

    if all(str(level).lstrip("-").isdigit() for level in levels):
        levels = np.asarray(levels, dtype=int)

    return counts_floor, levels


def build_column_for_finite(levels_per_year, levels, total_rows_per_year, missing_per_year, offsets, rng):
    levels_per_year = np.asarray(levels_per_year)
    total_rows_per_year = np.asarray(total_rows_per_year, dtype=np.int64)

    n_years = levels_per_year.shape[0]
    np.cumsum(total_rows_per_year, out=offsets[1:])
    total_rows = int(offsets[-1])

    out = np.empty(total_rows, dtype=object)
    for year_index in range(n_years):
        year_values = build_year_for_finite(
            year_index=year_index,
            levels_per_year=levels_per_year,
            levels=levels,
            total_rows_per_year=total_rows_per_year,
            missing_per_year=missing_per_year,
            rng=rng,
        )
        out[offsets[year_index] : offsets[year_index + 1]] = year_values

    return out


def build_year_for_finite(year_index, levels_per_year, levels, total_rows_per_year, missing_per_year, rng):
    counts = np.asarray(levels_per_year)[year_index].astype(int, copy=False)
    levels_arr = np.asarray(levels, dtype=object)

    rows = int(total_rows_per_year[year_index])
    missing = int(missing_per_year[year_index])
    if counts.size != levels_arr.size:
        raise ValueError("levels_per_year row length must match number of levels")

    non_missing = int(counts.sum())
    if non_missing + missing > rows:
        raise ValueError(
            f"Year {year_index}: counts.sum() ({non_missing}) + missing ({missing}) exceeds total rows ({rows})"
        )
    if np.any(counts < 0):
        raise ValueError("Negative counts in levels_per_year")

    out = np.empty(rows, dtype=object)
    if non_missing > 0:
        out[:non_missing] = np.repeat(levels_arr, counts)
    if missing > 0:
        out[non_missing:] = None

    rng.shuffle(out)
    return out
