import math

import numpy as np

from .config import COUNTRY_CODES, ID_CODE_ALPHABET, ID_SCHEMAS


def return_n_countries(unique_values, rng=None):
    if unique_values > len(COUNTRY_CODES):
        raise ValueError("Requested more country codes than available")
    pool = np.array(COUNTRY_CODES[:unique_values], dtype=object)
    if rng is not None:
        rng.shuffle(pool)
    return pool


def _int_to_code(x, width, alphabet):
    chars = []
    base = len(alphabet)
    for _ in range(width):
        x, remainder = divmod(x, base)
        chars.append(alphabet[remainder])
    return "".join(chars)


def generate_coded_pool(
    total_unique,
    rng=None,
    param_key="default",
    overlap_ratio=0.75,
    width=6,
    alphabet=ID_CODE_ALPHABET,
):
    if rng is None:
        rng = np.random.default_rng()

    params = ID_SCHEMAS[param_key]
    a = params["a"]
    b = params["b"]

    base = len(alphabet)
    n_codes = base**width

    if total_unique > n_codes:
        raise ValueError("Requested more codes than possible combinations")

    core_size = math.ceil(overlap_ratio * total_unique)
    tail_size = total_unique - core_size

    core_ids = [(a * index + b) % n_codes for index in range(core_size)]
    used = set(core_ids)
    tail_ids = []

    while len(tail_ids) < tail_size:
        candidate = int(rng.integers(n_codes))
        if candidate not in used:
            used.add(candidate)
            tail_ids.append(candidate)

    ids = core_ids + tail_ids
    pool = np.array([_int_to_code(code_id, width, alphabet) for code_id in ids], dtype=object)
    rng.shuffle(pool)
    return pool


def generate_logical_pool(meta):
    level_counts = meta.get("level_counts") or {}
    if level_counts:
        normalized = [str(value).strip().lower() for value in level_counts.keys()]
        if all(value in {"true", "false"} for value in normalized):
            return np.array([value == "true" for value in normalized], dtype=bool)

    unique_values = int(meta["total_unique_values"])
    if unique_values == 1:
        return np.array([False], dtype=bool)
    if unique_values == 2:
        return np.array([False, True], dtype=bool)
    raise ValueError("Logical variables must have one or two unique values")


def generate_number_categorical_pool(total_unique, rng):
    width = len(str(total_unique))
    start = 10 ** (width - 1)
    end = start + total_unique
    pool = np.arange(start, end, dtype="int64")
    if len(pool) != total_unique:
        raise ValueError("Pool size mismatch in number_categorical")
    rng.shuffle(pool)
    return pool
