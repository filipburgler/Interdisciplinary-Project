import numpy as np

from .numeric_core import (
    interpolation_epsilon,
    is_non_negative_numeric,
    numeric_bounds,
    sample_interpolated_values,
    use_integer_rounding,
)


def clip_numeric_values(values, ns):
    lower, upper = numeric_bounds(ns)
    clipped_values = np.clip(values, lower, upper)
    if is_non_negative_numeric(ns):
        clipped_values = np.clip(clipped_values, 0.0, upper)
    return clipped_values


def enforce_zero_anchor(values, reserve_zero):
    if reserve_zero and values.size > 0:
        anchored_values = values.astype(np.float64, copy=True)
        anchored_values[0] = 0.0
        return anchored_values
    return values


def repair_continuous_uniques(values, lower, upper):
    if values.size <= 1:
        return values
    order = np.argsort(values, kind="mergesort")
    sorted_values = values[order].astype(np.float64, copy=True)
    if not np.any(np.diff(sorted_values) <= 0.0):
        return values
    epsilon = interpolation_epsilon(lower, upper)
    for index in range(1, sorted_values.size):
        if sorted_values[index] <= sorted_values[index - 1]:
            sorted_values[index] = min(upper, sorted_values[index - 1] + epsilon)
    for index in range(sorted_values.size - 2, -1, -1):
        if sorted_values[index] >= sorted_values[index + 1]:
            sorted_values[index] = max(lower, sorted_values[index + 1] - epsilon)
    repaired_values = np.empty_like(sorted_values)
    repaired_values[np.argsort(order, kind="mergesort")] = sorted_values
    return repaired_values


def fallback_integer_support(lower_i, upper_i, total_unique):
    if total_unique <= 0:
        return np.empty(0, dtype=np.float64)
    if upper_i < lower_i:
        midpoint = int(round((lower_i + upper_i) / 2.0))
        return np.full(total_unique, float(midpoint), dtype=np.float64)
    fallback_values = np.rint(np.linspace(lower_i, upper_i, total_unique)).astype(np.int64)
    fallback_values = np.clip(fallback_values, lower_i, upper_i)
    return fallback_values.astype(np.float64)


def repair_integer_support(values, ns, total_unique, rng, shift_amount, preserve_zero_mass):
    lower, upper = numeric_bounds(ns)
    lower_i = int(np.ceil(lower))
    upper_i = int(np.floor(upper))
    if upper_i < lower_i:
        return fallback_integer_support(lower_i, upper_i, total_unique)
    rounded_values = np.clip(np.round(values), lower_i, upper_i).astype(np.int64)
    unique_values = np.unique(rounded_values)
    attempts = 0
    while unique_values.size < total_unique and attempts < 20:
        draw_size = max((total_unique - unique_values.size) * 8, 256)
        sampled_values = sample_interpolated_values(ns, draw_size, rng, shift_amount, preserve_zero_mass)
        sampled_values = clip_numeric_values(sampled_values, ns)
        rounded_samples = np.clip(np.round(sampled_values), lower_i, upper_i).astype(np.int64)
        unique_values = np.unique(np.concatenate([unique_values, rounded_samples]))
        attempts += 1
    if unique_values.size < total_unique:
        return fallback_integer_support(lower_i, upper_i, total_unique)
    if unique_values.size > total_unique:
        selection_indices = np.linspace(0, unique_values.size - 1, total_unique)
        unique_values = unique_values[np.rint(selection_indices).astype(np.int64)]
        unique_values = np.unique(unique_values)
    if unique_values.size != total_unique:
        return fallback_integer_support(lower_i, upper_i, total_unique)
    return unique_values.astype(np.float64)


def fallback_continuous_support(ns, total_unique, reserve_zero):
    lower, upper = numeric_bounds(ns)
    if total_unique <= 0:
        return np.empty(0, dtype=np.float64)
    if reserve_zero and total_unique == 1:
        return np.array([0.0], dtype=np.float64)
    fallback_values = np.linspace(lower, upper, total_unique, dtype=np.float64)
    return enforce_zero_anchor(fallback_values, reserve_zero)


def repair_continuous_support(values, ns, total_unique, reserve_zero):
    lower, upper = numeric_bounds(ns)
    repaired_values = repair_continuous_uniques(np.sort(values.astype(np.float64, copy=False)), lower, upper)
    repaired_values = enforce_zero_anchor(repaired_values, reserve_zero)
    if reserve_zero and repaired_values.size > 1:
        epsilon = interpolation_epsilon(lower, upper)
        for index in range(1, repaired_values.size):
            repaired_values[index] = max(repaired_values[index], repaired_values[index - 1] + epsilon)
        repaired_values = np.clip(repaired_values, lower, upper)
        repaired_values = repair_continuous_uniques(repaired_values, lower, upper)
        repaired_values = enforce_zero_anchor(repaired_values, reserve_zero)
    if np.unique(repaired_values).size < total_unique:
        return fallback_continuous_support(ns, total_unique, reserve_zero)
    return repaired_values


def repair_support(values, meta, ns, total_unique, reserve_zero, rng, shift_amount):
    if use_integer_rounding(meta, ns):
        return repair_integer_support(values, ns, total_unique, rng, shift_amount, reserve_zero)
    return repair_continuous_support(values, ns, total_unique, reserve_zero)
