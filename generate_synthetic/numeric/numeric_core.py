import numpy as np


_NUMERIC_KNOT_PROBABILITIES = np.array([0.0, 0.25, 0.50, 0.75, 0.90, 1.0], dtype=np.float64)
_NUMERIC_KNOT_FIELDS = ("m_min", "m_p25", "m_p50", "m_p75", "m_p90", "m_max")


def numeric_bounds(ns):
    lower = float(ns.get("m_min", 0.0))
    upper = float(ns.get("m_max", lower))
    if upper < lower:
        lower, upper = upper, lower
    return lower, upper


def interpolation_epsilon(lower, upper):
    return max((upper - lower) * 1e-9, 1e-9)


def numeric_quantile_knots(ns):
    lower, upper = numeric_bounds(ns)
    values = np.array([float(ns.get(field, lower if index == 0 else upper)) for index, field in enumerate(_NUMERIC_KNOT_FIELDS)])
    values[0] = lower
    values[-1] = upper
    if np.any(np.diff(values) < 0.0):
        values = np.maximum.accumulate(values)
        values[-1] = max(values[-1], upper)
    return _NUMERIC_KNOT_PROBABILITIES.copy(), values


def estimate_zero_mass(ns):
    if float(ns.get("m_max", 0.0)) == 0.0:
        return 1.0

    zero_mass = 0.0
    quantile_positions = (
        (0.25, "m_p25"),
        (0.50, "m_p50"),
        (0.75, "m_p75"),
        (0.90, "m_p90"),
    )
    for probability, field in quantile_positions:
        if float(ns.get(field, np.nan)) == 0.0:
            zero_mass = max(zero_mass, probability)
    return min(1.0, zero_mass + 0.05) if zero_mass > 0.0 else 0.0


def is_non_negative_numeric(ns):
    return float(ns.get("m_min", 0.0)) >= 0.0


def use_integer_rounding(meta, ns):
    discrete = str(meta.get("class", "")).startswith("integer") or meta.get("semantic_type") != "continuous"
    if not discrete:
        return False
    lower, upper = numeric_bounds(ns)
    integer_span = int(np.floor(upper) - np.ceil(lower) + 1)
    return integer_span >= int(meta["total_unique_values"])


def use_log_scale(ns):
    lower, upper = numeric_bounds(ns)
    median = max(float(ns.get("m_p50", 0.0)), 1e-9)
    return lower >= 0.0 and upper / median > 1e4


def interpolate_from_knots(probabilities, knot_probabilities, knot_values, ns):
    if not use_log_scale(ns):
        return np.interp(probabilities, knot_probabilities, knot_values)
    lower, upper = numeric_bounds(ns)
    epsilon = interpolation_epsilon(lower, upper)
    log_values = np.log(np.maximum(knot_values, 0.0) + epsilon)
    interpolated = np.interp(probabilities, knot_probabilities, log_values)
    return np.exp(interpolated) - epsilon


def interpolate_quantile_values(ns, probabilities):
    knot_probabilities, knot_values = numeric_quantile_knots(ns)
    return interpolate_from_knots(probabilities, knot_probabilities, knot_values, ns)


def build_support_weights(total_unique, reserve_zero, zero_mass):
    if reserve_zero:
        tail_unique = total_unique - 1
        if tail_unique <= 0:
            return np.array([1.0], dtype=np.float64)
        weights = np.concatenate(
            [
                np.array([zero_mass], dtype=np.float64),
                np.full(tail_unique, max(1.0 - zero_mass, 0.0) / tail_unique, dtype=np.float64),
            ]
        )
    else:
        weights = np.full(total_unique, 1.0 / total_unique, dtype=np.float64)
    weights /= weights.sum()
    return weights


def interpolate_support(ns, total_unique, reserve_zero, zero_mass):
    tail_unique = total_unique - int(reserve_zero)
    if tail_unique <= 0:
        return np.array([0.0], dtype=np.float64)
    start_probability = zero_mass if reserve_zero else 0.0
    step = (1.0 - start_probability) / tail_unique
    tail_probabilities = start_probability + (np.arange(tail_unique, dtype=np.float64) + 0.5) * step
    tail_probabilities = np.clip(tail_probabilities, 1e-12, 1.0 - 1e-12)
    tail_values = interpolate_quantile_values(ns, tail_probabilities)
    if reserve_zero:
        return np.concatenate([np.array([0.0], dtype=np.float64), tail_values])
    return tail_values


def sample_interpolated_values(ns, size, rng, shift_amount, preserve_zero_mass):
    zero_mass = estimate_zero_mass(ns)
    draw_probabilities = rng.random(size)
    sampled_values = interpolate_quantile_values(ns, draw_probabilities)
    if zero_mass > 0.0:
        zero_mask = draw_probabilities < zero_mass
        sampled_values[zero_mask] = 0.0
    else:
        zero_mask = np.zeros(size, dtype=bool)
    if shift_amount != 0.0:
        if preserve_zero_mass:
            sampled_values[~zero_mask] += shift_amount
        else:
            sampled_values += shift_amount
    return sampled_values


def apply_mean_correction(values, weights, ns, preserve_zero_mass):
    target_mean = ns.get("mean")
    if target_mean is None or values.size == 0:
        return values, 0.0
    current_mean = float(np.average(values, weights=weights))
    delta = (float(target_mean) - current_mean) * 0.5
    if delta == 0.0:
        return values, 0.0
    adjusted_values = values.astype(np.float64, copy=True)
    if preserve_zero_mass:
        zero_mask = adjusted_values == 0.0
        movable_weight = float(weights[~zero_mask].sum())
        if movable_weight > 0.0:
            shift_amount = delta / movable_weight
            adjusted_values[~zero_mask] += shift_amount
            return adjusted_values, shift_amount
    adjusted_values += delta
    return adjusted_values, delta
