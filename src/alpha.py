# src/alpha.py
from __future__ import annotations

import numpy as np
import pandas as pd
import cvxpy as cp
from typing import List


def calculate_target_weights(
    instrument_ids: List[str],
    max_position_weight: float = 0.05,
    max_leverage: float = 1.5,
    use_mosek: bool = False,
) -> pd.Series:
    """
    Calculates mean-variance optimized weights.

    Placeholder alpha: random expected returns (replace with real signals).

    Parameters
    ----------
    instrument_ids : List[str]
        List of instrument ID strings.
    max_position_weight : float, default 0.05
        Maximum weight per position.
    max_leverage : float, default 1.5
        Maximum gross exposure (L1 norm).
    use_mosek : bool, default False
        Use MOSEK solver if available and licensed (otherwise falls back to SCS/ECOS).

    Returns
    -------
    pd.Series
        Optimized weights indexed by instrument_ids.
    """
    n = len(instrument_ids)
    if n == 0:
        return pd.Series(dtype=float)

    # Placeholder alpha model — replace with real expected returns
    mu = np.random.normal(loc=0.0, scale=0.1, size=n)  # Simulated daily excess returns
    Sigma = np.eye(n)  # Identity covariance (low risk correlation placeholder)
    gamma = 1.0  # Risk aversion

    w = cp.Variable(n)
    ret = mu @ w
    risk = cp.quad_form(w, Sigma)

    constraints = [
        cp.sum(w) == 1.0,
        w >= 0,                     # Long-only
        w <= max_position_weight,
        cp.norm(w, 1) <= max_leverage,
    ]

    prob = cp.Problem(cp.Maximize(ret - gamma * risk), constraints)

    solver = cp.MOSEK if use_mosek else cp.SCS
    try:
        prob.solve(solver=solver, verbose=False)
        if w.value is None:
            raise ValueError("Solver returned no solution")
        weights = w.value
    except (cp.SolverError, ValueError) as e:
        print(f"Optimization failed ({solver}): {e}. Falling back to equal weights.")
        weights = np.ones(n) / n

    return pd.Series(weights, index=instrument_ids)
