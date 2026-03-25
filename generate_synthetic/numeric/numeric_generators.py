import numpy as np

from .numeric_core import (
    apply_mean_correction,
    build_support_weights,
    estimate_zero_mass,
    interpolate_support,
    numeric_bounds,
)
from .numeric_support import clip_numeric_values, enforce_zero_anchor, repair_support


def generate_numeric_pool(meta, rng):
    total_unique = int(meta["total_unique_values"])
    numeric_summary = meta["numeric_summary"]
    if not numeric_summary:
        return np.zeros(max(total_unique, 0), dtype=np.float64)

    lower, upper = numeric_bounds(numeric_summary)
    zero_mass = estimate_zero_mass(numeric_summary)
    reserve_zero = bool(total_unique > 0 and zero_mass > 0.0 and lower <= 0.0 <= upper)

    initial_support = interpolate_support(numeric_summary, total_unique, reserve_zero, zero_mass)
    support_weights = build_support_weights(total_unique, reserve_zero, zero_mass)

    corrected_support, first_shift = apply_mean_correction(initial_support, support_weights, numeric_summary, reserve_zero)
    clipped_support = clip_numeric_values(corrected_support, numeric_summary)
    clipped_support = enforce_zero_anchor(clipped_support, reserve_zero)

    corrected_after_clip, second_shift = apply_mean_correction(clipped_support, support_weights, numeric_summary, reserve_zero)
    corrected_after_clip = clip_numeric_values(corrected_after_clip, numeric_summary)
    corrected_after_clip = enforce_zero_anchor(corrected_after_clip, reserve_zero)

    repaired_support = repair_support(
        values=corrected_after_clip,
        meta=meta,
        ns=numeric_summary,
        total_unique=total_unique,
        reserve_zero=reserve_zero,
        rng=rng,
        shift_amount=first_shift + second_shift,
    )

    final_support = clip_numeric_values(repaired_support, numeric_summary)
    final_support = enforce_zero_anchor(final_support, reserve_zero)
    permutation = rng.permutation(total_unique)
    return final_support[permutation]


def generate_numeric_support(meta, rng):
    total_unique = int(meta["total_unique_values"])
    numeric_summary = meta["numeric_summary"]
    support_values = generate_numeric_pool(meta, rng)
    sorted_support = np.sort(support_values.astype(np.float64, copy=False))

    zero_mass = estimate_zero_mass(numeric_summary)
    if zero_mass > 0.0 and np.any(sorted_support == 0.0):
        non_zero_mask = sorted_support != 0.0
        non_zero_count = int(np.count_nonzero(non_zero_mask))
        support_weights = np.empty(sorted_support.size, dtype=np.float64)
        zero_indices = np.flatnonzero(~non_zero_mask)
        support_weights[zero_indices] = zero_mass / zero_indices.size
        if non_zero_count:
            support_weights[non_zero_mask] = (1.0 - zero_mass) / non_zero_count
        else:
            support_weights.fill(1.0 / total_unique)
    else:
        support_weights = np.full(total_unique, 1.0 / total_unique, dtype=np.float64)

    permutation = rng.permutation(total_unique)
    return sorted_support[permutation], support_weights[permutation]
