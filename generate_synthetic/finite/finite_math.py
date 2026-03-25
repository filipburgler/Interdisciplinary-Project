import numpy as np


def build_b_matrix_capacity_aware(x, y, u, rng):
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=int)
    u = np.array(u, dtype=float)

    n_years = len(x)
    n_levels = len(u)

    if y.sum() < n_levels:
        raise ValueError("Infeasible: sum(y) < number of levels")

    b = np.zeros((n_years, n_levels), dtype=int)
    remaining_slots = y.copy()
    remaining_demand = x.copy()

    levels = list(range(n_levels))
    rng.shuffle(levels)
    for level_index in levels:
        eligible_years = [year_index for year_index in range(n_years) if remaining_slots[year_index] > 0]
        if not eligible_years:
            raise ValueError("No feasible year for level coverage")

        weights = np.asarray(
            [remaining_slots[year_index] * remaining_demand[year_index] for year_index in eligible_years],
            dtype=float,
        )
        weights /= weights.sum()

        chosen_year = rng.choice(eligible_years, p=weights)
        b[chosen_year, level_index] = 1
        remaining_slots[chosen_year] -= 1
        remaining_demand[chosen_year] = max(0, remaining_demand[chosen_year] - u[level_index])

    for year_index in range(n_years):
        needed = remaining_slots[year_index]
        if needed <= 0:
            continue

        available_levels = [level_index for level_index in range(n_levels) if b[year_index, level_index] == 0]
        if len(available_levels) < needed:
            raise ValueError(f"Not enough levels to fill year {year_index}")

        chosen_levels = []
        level_pool = available_levels.copy()
        weights = [u[level_index] for level_index in level_pool]

        for _ in range(needed):
            weights = np.asarray(weights, dtype=float)
            weights /= weights.sum()
            level_index = rng.choice(level_pool, p=weights)
            pool_index = level_pool.index(level_index)
            chosen_levels.append(level_index)
            level_pool.pop(pool_index)
            weights = list(weights)
            weights.pop(pool_index)

        for level_index in chosen_levels:
            b[year_index, level_index] = 1

    for year_index in range(n_years):
        total_capacity = sum(u[level_index] for level_index in range(n_levels) if b[year_index, level_index] == 1)
        if total_capacity < x[year_index]:
            raise ValueError(
                f"Infeasible assignment for year {year_index}: "
                f"capacity {total_capacity} < demand {x[year_index]}"
            )

    return b


def solve_a_with_constraints(b, x, u, max_iter=10_000, tol=1e-10):
    n_years, _ = b.shape
    a = b.astype(float)

    for _ in range(max_iter):
        row_mass = a @ u
        row_scale = np.divide(x, row_mass, out=np.ones_like(x), where=row_mass > 0)
        a *= row_scale[:, None]

        col_sum = a.sum(axis=0)
        col_scale = np.divide(1.0, col_sum, out=np.ones_like(col_sum), where=col_sum > 0)
        a *= col_scale[None, :]

        if np.allclose(a @ u, x, atol=tol) and np.allclose(a.sum(axis=0), 1, atol=tol):
            break
    else:
        raise RuntimeError("IPFP did not converge")

    return a
