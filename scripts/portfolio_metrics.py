"""Portfolio return metrics for scenario analysis."""

from __future__ import annotations


def calculate_irr(cashflows: list[float], guess: float = 0.1, max_iter: int = 100) -> float:
    """Compute internal rate of return via Newton-Raphson."""

    rate = guess
    for _ in range(max_iter):
        npv = sum(cf / (1 + rate) ** i for i, cf in enumerate(cashflows))
        dnpv = sum(-i * cf / (1 + rate) ** (i + 1) for i, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            break
        rate -= npv / dnpv
    return rate


def simulate_scenario(cashflows: list[float], multiplier: float) -> list[float]:
    """Apply a revenue shock multiplier to all positive cashflows after period 0."""

    return [cashflows[0]] + [cf * multiplier if cf > 0 else cf for cf in cashflows[1:]]
