"""Load and validate reform assumptions (holding period fractions, loss ratios)."""

import json
from pathlib import Path

ASSUMPTIONS_FILE = Path(__file__).resolve().parents[2] / "data" / "parameters" / "reform_assumptions.json"

# All 19 canonical AGI bin IDs
ALL_BIN_IDS = set(range(1, 20))


def load_assumptions(path: Path = ASSUMPTIONS_FILE) -> dict:
    """Load reform_assumptions.json and validate structure."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Validate holding period fractions
    hp = data["holding_period_lt_under_5yr"]
    _validate_agi_groups(hp["by_agi_group"], "fraction", "holding_period_lt_under_5yr")

    # Validate gross loss ratios
    gl = data["gross_loss_ratio"]
    _validate_agi_groups(gl["by_agi_group"], "ratio", "gross_loss_ratio")

    return data


def _validate_agi_groups(groups: list[dict], value_key: str, section: str) -> None:
    """Check all 19 bins are covered and values are in [0, 1]."""
    covered = set()
    for group in groups:
        bin_ids = group["agi_bin_ids"]
        value = group[value_key]
        if not (0 <= value <= 1):
            raise ValueError(
                f"{section}: {value_key}={value} out of range [0,1] "
                f"for bins {bin_ids}"
            )
        for bid in bin_ids:
            if bid in covered:
                raise ValueError(
                    f"{section}: bin {bid} appears in multiple groups"
                )
            covered.add(bid)
    missing = ALL_BIN_IDS - covered
    if missing:
        raise ValueError(f"{section}: bins {sorted(missing)} not covered")


def get_holding_period_fraction(assumptions: dict, agi_bin_id: int) -> float:
    """Return the fraction of LT gains held 1-5 years for a given bin."""
    for group in assumptions["holding_period_lt_under_5yr"]["by_agi_group"]:
        if agi_bin_id in group["agi_bin_ids"]:
            return group["fraction"]
    raise KeyError(f"No holding period fraction for bin {agi_bin_id}")


def get_gross_loss_ratio(assumptions: dict, agi_bin_id: int) -> float:
    """Return the gross loss ratio for a given bin."""
    for group in assumptions["gross_loss_ratio"]["by_agi_group"]:
        if agi_bin_id in group["agi_bin_ids"]:
            return group["ratio"]
    # Fall back to default
    return assumptions["gross_loss_ratio"]["default"]
