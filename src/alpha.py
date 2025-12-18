import cvxpy as cp
import numpy as np
import pandas as pd

def calculate_target_weights(
    instrument_ids: list[str],
    max_position_weight: float = 0.05,
    max_leverage: float = 1.5
) -> pd.Series:
    """
    Calculates portfolio weights using Mean-Variance Optimization.
    Currently uses random returns (placeholder for real alpha).
    """
    n = len(instrument_ids)

    # --- Alpha Model (Placeholder) ---
    # In production, pass 'expected_returns' as an argument to this function
    mu = np.random.normal(size=n) * 0.1
    Sigma = np.diag(np.ones(n))
    gamma = 1.0

    # --- Optimization ---
    w = cp.Variable(n)
    ret = mu.T @ w
    risk = cp.quad_form(w, Sigma)

    prob = cp.Problem(
        cp.Maximize(ret - gamma * risk),
        [
            cp.sum(w) == 1,
            w >= 0,
            w <= max_position_weight,
            cp.norm(w, 1) <= max_leverage
        ]
    )

    try:
        prob.solve(solver=cp.SCS, max_iters=10000)
        weights_val = w.value
        if weights_val is None:
            raise ValueError("Solver returned None")
    except (cp.SolverError, ValueError) as e:
        print(f"Optimization failed: {e}")
        # Fallback: Equal weights
        weights_val = np.ones(n) / n

    return pd.Series(weights_val, index=instrument_ids)
