"""Explicit scenario resolution for condition-dependent benchmark parameters.

The canonical values remain in ``Conditional_Technical_Values``.  This module
only projects them into scenario-specific comparison views; it never writes a
conditional value back to Products, Comparison, or a selected-default field.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


SCORING_SCENARIO_COLUMNS = [
    "scenario_id",
    "scenario_label",
    "parameter_name",
    "condition_type",
    "condition_value",
    "condition_unit",
    "policy",
    "is_default",
    "scoring_enabled",
    "customer_view_enabled",
    "notes",
]

SCENARIO_COMPARISON_COLUMNS = [
    "product_id",
    "manufacturer",
    "product_name",
    "product_family",
    "scenario_id",
    "flow_rate_lps",
    "flow_rate_resolution_status",
    "flow_rate_resolution_source",
    "flow_rate_condition_type",
    "flow_rate_condition_value",
    "flow_rate_condition_unit",
    "flow_rate_condition_label",
    "scenario_ready_for_benchmark",
    "scenario_blocked_reason",
    "scenario_scoring_note",
]

SCENARIOS = (
    {
        "scenario_id": "no_scenario_selected",
        "scenario_label": "No conditional scoring scenario selected",
        "parameter_name": "flow_rate_lps",
        "condition_type": "",
        "condition_value": "",
        "condition_unit": "",
        "policy": "do_not_resolve_conditional_value",
        "is_default": True,
        "scoring_enabled": False,
        "customer_view_enabled": False,
        "notes": "Safe default: conditional flow remains unresolved and no hidden default is selected.",
    },
    {
        "scenario_id": "flow_head_10mm",
        "scenario_label": "Flow at 10 mm head water level",
        "parameter_name": "flow_rate_lps",
        "condition_type": "head_water_level",
        "condition_value": 10,
        "condition_unit": "mm",
        "policy": "resolve_exact_condition_match",
        "is_default": False,
        "scoring_enabled": True,
        "customer_view_enabled": False,
        "notes": "Benchmark-only scenario; does not change production or customer-view readiness.",
    },
    {
        "scenario_id": "flow_head_20mm",
        "scenario_label": "Flow at 20 mm head water level",
        "parameter_name": "flow_rate_lps",
        "condition_type": "head_water_level",
        "condition_value": 20,
        "condition_unit": "mm",
        "policy": "resolve_exact_condition_match",
        "is_default": False,
        "scoring_enabled": True,
        "customer_view_enabled": False,
        "notes": "Benchmark-only scenario; does not change production or customer-view readiness.",
    },
)

_REQUIRED_TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
_CONDITIONAL_BLOCK_REASON = "blocked_pending_conditional_parameter_scoring"


def scoring_scenarios_dataframe() -> pd.DataFrame:
    """Return the stable scenario registry exported to ``Scoring_Scenarios``."""
    return pd.DataFrame(SCENARIOS, columns=SCORING_SCENARIO_COLUMNS)


def _scenario(scenario_id: str) -> dict[str, Any]:
    for scenario in SCENARIOS:
        if scenario["scenario_id"] == scenario_id:
            return scenario
    raise ValueError(f"Unknown scoring scenario: {scenario_id}")


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _has_value(value: Any) -> bool:
    return _text(value).lower() not in {"", "nan", "none", "null"}


def _product_family(row: pd.Series) -> str:
    return _text(row.get("product_family")) or _text(row.get("family"))


def _base_fields_ready(row: pd.Series, resolved_flow: Any) -> bool:
    values = {field: row.get(field, "") for field in _REQUIRED_TECHNICAL_FIELDS}
    values["flow_rate_lps"] = resolved_flow
    return all(_has_value(value) for value in values.values())


def resolve_flow_rate_for_scenario(
    product_row: pd.Series | dict[str, Any],
    conditional_values: pd.DataFrame,
    scenario_id: str,
) -> dict[str, Any]:
    """Resolve one product's flow for an explicit scenario.

    Conditional products never fall back to a scalar value.  With the default
    no-scenario policy they remain unresolved, preserving the existing safe
    scoring behavior.
    """
    row = product_row if isinstance(product_row, pd.Series) else pd.Series(product_row)
    scenario = _scenario(scenario_id)
    product_id = _text(row.get("product_id"))
    conditional_values = pd.DataFrame() if conditional_values is None else conditional_values

    if conditional_values.empty or "set_id" not in conditional_values.columns:
        product_conditions = conditional_values.iloc[0:0]
    else:
        product_conditions = conditional_values[
            conditional_values["set_id"].fillna("").astype(str).str.strip().eq(product_id)
        ]
    if not product_conditions.empty and "parameter_name" in product_conditions.columns:
        product_conditions = product_conditions[
            product_conditions["parameter_name"].fillna("").astype(str).str.strip().eq("flow_rate_lps")
        ]

    if not product_conditions.empty:
        if scenario_id == "no_scenario_selected":
            return {
                "flow_rate_lps": "",
                "flow_rate_resolution_status": "unresolved",
                "flow_rate_resolution_source": "",
                "flow_rate_condition_type": "",
                "flow_rate_condition_value": "",
                "flow_rate_condition_unit": "",
                "flow_rate_condition_label": "",
                "scenario_ready_for_benchmark": False,
                "scenario_blocked_reason": _CONDITIONAL_BLOCK_REASON,
                "scenario_scoring_note": "Conditional flow is intentionally unresolved until an explicit scenario is selected.",
            }

        condition_values = pd.to_numeric(product_conditions.get("condition_value"), errors="coerce")
        expected_value = float(scenario["condition_value"])
        matches = product_conditions[
            product_conditions.get("condition_type", pd.Series("", index=product_conditions.index)).fillna("").astype(str).str.strip().eq(scenario["condition_type"])
            & condition_values.eq(expected_value)
            & product_conditions.get("condition_unit", pd.Series("", index=product_conditions.index)).fillna("").astype(str).str.strip().eq(scenario["condition_unit"])
        ]
        if len(matches) == 1:
            match = matches.iloc[0]
            resolved = match.get("value", "")
            ready = _base_fields_ready(row, resolved)
            return {
                "flow_rate_lps": resolved,
                "flow_rate_resolution_status": "resolved_from_condition",
                "flow_rate_resolution_source": "Conditional_Technical_Values",
                "flow_rate_condition_type": match.get("condition_type", ""),
                "flow_rate_condition_value": match.get("condition_value", ""),
                "flow_rate_condition_unit": match.get("condition_unit", ""),
                "flow_rate_condition_label": match.get("condition_label", ""),
                "scenario_ready_for_benchmark": ready,
                "scenario_blocked_reason": "" if ready else "other_required_benchmark_fields_missing",
                "scenario_scoring_note": "Conditional flow resolved by exact scenario match; default and customer-facing outputs remain unchanged.",
            }

        return {
            "flow_rate_lps": "",
            "flow_rate_resolution_status": "unresolved",
            "flow_rate_resolution_source": "Conditional_Technical_Values",
            "flow_rate_condition_type": scenario["condition_type"],
            "flow_rate_condition_value": scenario["condition_value"],
            "flow_rate_condition_unit": scenario["condition_unit"],
            "flow_rate_condition_label": scenario["scenario_label"],
            "scenario_ready_for_benchmark": False,
            "scenario_blocked_reason": "missing_matching_conditional_flow_value_for_selected_scenario",
            "scenario_scoring_note": "No canonical conditional value matched the selected scenario.",
        }

    scalar_flow = row.get("flow_rate_lps", "")
    if _has_value(scalar_flow):
        existing_ready = row.get("ready_for_benchmark", None)
        ready = bool(existing_ready) if isinstance(existing_ready, bool) else _base_fields_ready(row, scalar_flow)
        return {
            "flow_rate_lps": scalar_flow,
            "flow_rate_resolution_status": "resolved_from_scalar",
            "flow_rate_resolution_source": "scalar_unconditional",
            "flow_rate_condition_type": "",
            "flow_rate_condition_value": "",
            "flow_rate_condition_unit": "",
            "flow_rate_condition_label": "unconditional",
            "scenario_ready_for_benchmark": ready,
            "scenario_blocked_reason": "" if ready else (_text(row.get("blocked_reason")) or "other_required_benchmark_fields_missing"),
            "scenario_scoring_note": "Unconditional scalar flow is valid in every scoring scenario.",
        }

    return {
        "flow_rate_lps": "",
        "flow_rate_resolution_status": "unresolved",
        "flow_rate_resolution_source": "",
        "flow_rate_condition_type": "",
        "flow_rate_condition_value": "",
        "flow_rate_condition_unit": "",
        "flow_rate_condition_label": "",
        "scenario_ready_for_benchmark": False,
        "scenario_blocked_reason": "missing_scalar_and_matching_conditional_flow_value",
        "scenario_scoring_note": "No scalar flow or matching canonical conditional value is available.",
    }


def build_scenario_comparison(
    comparison_df: pd.DataFrame,
    conditional_values: pd.DataFrame,
    scenario_id: str,
) -> pd.DataFrame:
    """Build a scenario-only comparison projection without mutating inputs."""
    _scenario(scenario_id)
    rows: list[dict[str, Any]] = []
    for _, product in pd.DataFrame(comparison_df).iterrows():
        resolution = resolve_flow_rate_for_scenario(product, conditional_values, scenario_id)
        rows.append(
            {
                "product_id": product.get("product_id", ""),
                "manufacturer": product.get("manufacturer", ""),
                "product_name": product.get("product_name", ""),
                "product_family": _product_family(product),
                "scenario_id": scenario_id,
                **resolution,
            }
        )
    return pd.DataFrame(rows, columns=SCENARIO_COMPARISON_COLUMNS)
