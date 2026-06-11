"""Read-only customer projection for explicitly selected technical conditions.

The canonical product frames intentionally describe the no-scenario state.  This
module resolves approved M+ flow values only in a runtime customer projection;
it never writes a conditional value or readiness flag back to canonical data.
"""
from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

import pandas as pd

NO_SCENARIO_SELECTED = "no_scenario_selected"
FLOW_HEAD_10MM = "flow_head_10mm"
FLOW_HEAD_20MM = "flow_head_20mm"
SUPPORTED_CUSTOMER_SCENARIOS = (
    NO_SCENARIO_SELECTED,
    FLOW_HEAD_10MM,
    FLOW_HEAD_20MM,
)
CUSTOMER_SCENARIO_OPTIONS = {
    NO_SCENARIO_SELECTED: "Select water head condition",
    FLOW_HEAD_10MM: "10 mm head water level",
    FLOW_HEAD_20MM: "20 mm head water level",
}
CUSTOMER_SCENARIO_STATE_KEY = "customer_water_head_scenario_id"
CUSTOMER_SCENARIO_RUN_KEY = "customer_water_head_scenario_run_id"

MPLUS_FAMILY = "showerdrain_mplus"
BLINE_FAMILY = "showerdrain_b"
CONDITIONAL_SOURCE = "Conditional_Technical_Values"
SELECTION_REQUIRED = "explicit_head_water_level_selection_required"

CUSTOMER_PROJECTION_COLUMNS = [
    "product_id",
    "product_family",
    "selected_scenario_id",
    "flow_rate_lps",
    "flow_rate_resolution_status",
    "flow_rate_resolution_source",
    "flow_rate_condition_type",
    "flow_rate_condition_value",
    "flow_rate_condition_unit",
    "flow_rate_condition_label",
    "customer_ready_for_selected_scenario",
    "customer_blocked_reason",
    "customer_presentation_note",
]

_SCENARIO_CONDITIONS = {
    FLOW_HEAD_10MM: {
        "condition_type": "head_water_level",
        "condition_value": 10.0,
        "condition_unit": "mm",
        "condition_label": "10 mm head water level",
        "flow_rate_lps": 0.40,
    },
    FLOW_HEAD_20MM: {
        "condition_type": "head_water_level",
        "condition_value": 20.0,
        "condition_unit": "mm",
        "condition_label": "20 mm head water level",
        "flow_rate_lps": 0.46,
    },
}
_EMPTY_TEXT = {"", "nan", "none", "null"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _has_value(value: Any) -> bool:
    return _text(value).lower() not in _EMPTY_TEXT


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "1.0", "true", "yes", "y"}


def _family(row: pd.Series) -> str:
    return (
        _text(row.get("product_family"))
        or _text(row.get("assembled_family"))
        or _text(row.get("family"))
    )


def _product_id(row: pd.Series) -> str:
    return (
        _text(row.get("product_id"))
        or _text(row.get("assembled_product_id"))
        or _text(row.get("set_id"))
    )


def _base_projection(row: pd.Series, scenario_id: str) -> dict[str, Any]:
    return {
        "product_id": _product_id(row),
        "product_family": _family(row),
        "selected_scenario_id": scenario_id,
        "flow_rate_lps": "",
        "flow_rate_resolution_status": "blocked",
        "flow_rate_resolution_source": "",
        "flow_rate_condition_type": "",
        "flow_rate_condition_value": "",
        "flow_rate_condition_unit": "",
        "flow_rate_condition_label": "",
        "customer_ready_for_selected_scenario": False,
        "customer_blocked_reason": "",
        "customer_presentation_note": "",
    }


def _blocked(
    row: pd.Series,
    scenario_id: str,
    reason: str,
    note: str,
    *,
    condition: dict[str, Any] | None = None,
    source: str = "",
) -> dict[str, Any]:
    projected = _base_projection(row, scenario_id)
    projected.update({
        "flow_rate_resolution_status": "blocked",
        "flow_rate_resolution_source": source,
        "customer_blocked_reason": reason,
        "customer_presentation_note": note,
    })
    if condition:
        projected.update({
            "flow_rate_condition_type": condition["condition_type"],
            "flow_rate_condition_value": int(condition["condition_value"]),
            "flow_rate_condition_unit": condition["condition_unit"],
            "flow_rate_condition_label": condition["condition_label"],
        })
    return projected


def _canonical_non_mplus_projection(row: pd.Series, scenario_id: str) -> dict[str, Any]:
    family = _family(row)
    if family == BLINE_FAMILY:
        return _blocked(
            row,
            scenario_id,
            "bline_customer_policy_not_approved",
            "B-line remains unavailable in customer output pending separate policy approval.",
        )

    ready = _truthy(row.get("ready_for_customer_view")) and _truthy(
        row.get("customer_view_enabled", row.get("ready_for_customer_view"))
    )
    projected = _base_projection(row, scenario_id)
    if not ready:
        projected.update({
            "customer_blocked_reason": _text(row.get("blocked_reason"))
            or _text(row.get("blocking_reason"))
            or "canonical_customer_view_not_enabled",
            "customer_presentation_note": "Product remains blocked by its canonical customer policy.",
        })
        return projected

    projected.update({
        "flow_rate_lps": row.get("flow_rate_lps", ""),
        "flow_rate_resolution_status": "canonical_unconditional",
        "flow_rate_resolution_source": "canonical",
        "flow_rate_condition_label": "unconditional",
        "customer_ready_for_selected_scenario": True,
        "customer_presentation_note": "Canonical customer-approved value; no M+ condition was applied.",
    })
    return projected


def _mplus_projection(
    row: pd.Series,
    conditional_values: pd.DataFrame,
    scenario_id: str,
) -> dict[str, Any]:
    if _has_value(row.get("flow_rate_lps")) or _has_value(row.get("selected_default_flow_rate_lps")):
        return _blocked(
            row,
            scenario_id,
            "unexpected_mplus_scalar_default_present",
            "M+ is blocked because an unconditional scalar/default flow was found.",
        )

    if scenario_id == NO_SCENARIO_SELECTED:
        return _blocked(
            row,
            scenario_id,
            SELECTION_REQUIRED,
            "Select 10 mm or 20 mm head water level to display the M+ flow rate.",
        )

    condition = _SCENARIO_CONDITIONS[scenario_id]
    if conditional_values.empty or "set_id" not in conditional_values.columns:
        matches = conditional_values.iloc[0:0]
    else:
        matches = conditional_values[
            conditional_values["set_id"].map(_text).eq(_product_id(row))
        ]
        if "parameter_name" in matches.columns:
            matches = matches[matches["parameter_name"].map(_text).eq("flow_rate_lps")]
        else:
            matches = matches.iloc[0:0]
        numeric_conditions = pd.to_numeric(
            matches.get("condition_value", pd.Series(index=matches.index, dtype=float)),
            errors="coerce",
        )
        matches = matches[numeric_conditions.eq(condition["condition_value"])]

    if len(matches) == 0:
        return _blocked(
            row,
            scenario_id,
            "missing_matching_conditional_flow_value",
            "No exact conditional M+ flow value exists for the selected head water level.",
            condition=condition,
            source=CONDITIONAL_SOURCE,
        )
    if len(matches) != 1:
        return _blocked(
            row,
            scenario_id,
            "duplicate_matching_conditional_flow_values",
            "Multiple conditional M+ flow values matched; customer presentation is blocked.",
            condition=condition,
            source=CONDITIONAL_SOURCE,
        )

    match = matches.iloc[0]
    metadata_valid = (
        _text(match.get("condition_type")) == condition["condition_type"]
        and _text(match.get("condition_unit")) == condition["condition_unit"]
        and _text(match.get("condition_label")) == condition["condition_label"]
        and _text(match.get("unit")) == "l/s"
    )
    value = pd.to_numeric(pd.Series([match.get("value")]), errors="coerce").iloc[0]
    value_valid = pd.notna(value) and abs(float(value) - condition["flow_rate_lps"]) < 1e-9
    if not metadata_valid or not value_valid:
        return _blocked(
            row,
            scenario_id,
            "invalid_conditional_flow_metadata_or_value",
            "Conditional M+ flow metadata/value did not exactly match the approved policy.",
            condition=condition,
            source=CONDITIONAL_SOURCE,
        )

    projected = _base_projection(row, scenario_id)
    projected.update({
        "flow_rate_lps": float(value),
        "flow_rate_resolution_status": "resolved_from_condition",
        "flow_rate_resolution_source": CONDITIONAL_SOURCE,
        "flow_rate_condition_type": condition["condition_type"],
        "flow_rate_condition_value": int(condition["condition_value"]),
        "flow_rate_condition_unit": condition["condition_unit"],
        "flow_rate_condition_label": condition["condition_label"],
        "customer_ready_for_selected_scenario": True,
        "customer_presentation_note": (
            f"{float(value):.2f} l/s at {condition['condition_label']}; "
            "conditional value, not an unconditional product property."
        ),
    })
    return projected


def build_customer_scenario_projection(
    canonical_frame: pd.DataFrame,
    conditional_values: pd.DataFrame,
    selected_scenario_id: str,
) -> pd.DataFrame:
    """Return a fail-closed customer projection without mutating either input."""
    canonical = pd.DataFrame() if canonical_frame is None else canonical_frame.copy(deep=True)
    conditions = pd.DataFrame() if conditional_values is None else conditional_values.copy(deep=True)
    scenario_id = _text(selected_scenario_id)
    known_scenario = scenario_id in SUPPORTED_CUSTOMER_SCENARIOS

    rows: list[dict[str, Any]] = []
    for _, row in canonical.iterrows():
        if not known_scenario:
            rows.append(_blocked(
                row,
                scenario_id,
                "unknown_customer_scenario_id",
                "Unknown customer scenario; no conditional value was applied.",
            ))
        elif _family(row) == MPLUS_FAMILY:
            rows.append(_mplus_projection(row, conditions, scenario_id))
        else:
            rows.append(_canonical_non_mplus_projection(row, scenario_id))
    return pd.DataFrame(rows, columns=CUSTOMER_PROJECTION_COLUMNS)


def reset_customer_scenario_for_run(
    session_state: MutableMapping[str, Any],
    run_id: str,
) -> bool:
    """Clear a stale selection whenever discovery/update creates a new run.

    Returns ``True`` when state was reset, which keeps the behavior easy to test
    without importing Streamlit.
    """
    normalized_run_id = _text(run_id)
    if _text(session_state.get(CUSTOMER_SCENARIO_RUN_KEY)) == normalized_run_id:
        return False
    session_state[CUSTOMER_SCENARIO_STATE_KEY] = NO_SCENARIO_SELECTED
    session_state[CUSTOMER_SCENARIO_RUN_KEY] = normalized_run_id
    return True
