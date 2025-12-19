import numpy as np
import cvxpy as cp
import pandas as pd


def optimize_target_positions_usd(
    alpha: pd.Series,
    current_position_usd: pd.Series,
    trading_cost: pd.Series,
    risk_lambda: pd.Series,
    clip_pos_usd: pd.Series,
    clip_trd_usd: pd.Series,
    factor_exposure: pd.Series | None = None,
    clip_exposure: float | None = None,
    max_delta: float = 0.0,
    solver: str = "SCS",
) -> pd.Series:
    """
    Optimize target positions directly in USD with trading cost penalty.

    Returns
    -------
    pd.Series
        Target positions in USD.
    """
    idx = alpha.index
    n = len(idx)

    # Align everything
    x0 = current_position_usd.loc[idx].values
    alpha = alpha.loc[idx].values
    cost = trading_cost.loc[idx].values
    lam = risk_lambda.loc[idx].values
    pos_cap = clip_pos_usd.loc[idx].values
    trd_cap = clip_trd_usd.loc[idx].values

    x = cp.Variable(n)

    objective = cp.Maximize(
        alpha @ x
        - cost @ cp.abs(x - x0)
        - 0.5 * cp.sum(cp.multiply(lam, cp.square(x)))
    )

    constraints = [
        cp.abs(x) <= pos_cap,
        cp.abs(x - x0) <= trd_cap,
    ]

    if max_delta > 0:
        constraints.append(cp.abs(cp.sum(x)) <= max_delta)

    if factor_exposure is not None and clip_exposure is not None:
        f = factor_exposure.loc[idx].values
        constraints.append(cp.abs(f @ x) <= clip_exposure)

    problem = cp.Problem(objective, constraints)

    try:
        problem.solve(
            solver=cp.MOSEK if solver == "MOSEK" else cp.SCS,
            verbose=False,
        )
        if x.value is None:
            raise ValueError("Solver returned None")
    except Exception as exc:
        raise RuntimeError(f"Optimization failed: {exc}")

    return pd.Series(x.value, index=idx)
