"""Output routing registry."""

OUTPUTS = {
    "metrics": ["sqlite", "sheet"],
    "signals": ["sqlite", "sheet", "notify"],
    "raw_bond_curves": ["sqlite"],
}
