"""Read-only customer projection for explicitly selected technical conditions.

The canonical product frames intentionally describe the no-scenario state.  This
module resolves approved M+ and B-line flow values only in a runtime customer projection;
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
APPROVED_BLINE_ARTICLES = frozenset({
    "9010.78.70",
    "9010.78.71",
    "9010.78.72",
    "9010.78.73",
    "3018172",
    "3018173",
    "3018174",
    "3018175",
})
APPROVED_BLINE_PRODUCT_IDS = frozenset(
    f"aco-showerdrain-b-finished-set-{article.replace('.', '-')}"
    for article in APPROVED_BLINE_ARTICLES
)
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


def _canonical_unconditional_projection(row: pd.Series, scenario_id: str) -> dict[str, Any]:
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
        "customer_presentation_note": "Canonical customer-approved value; no conditional flow was applied.",
    })
    return projected


def _bline_article(row: pd.Series) -> str:
    return _text(row.get("product_article_number")) or _text(row.get("article_number"))


def is_approved_bline_finished_set_candidate(row: pd.Series) -> bool:
    """Identify intended approved B-line rows without admitting family-level records.

    Exact identity validation remains separate so a malformed approved row is
    projected and blocked rather than silently disappearing from diagnostics.
    """
    product_id = _product_id(row)
    article = _bline_article(row)
    return product_id in APPROVED_BLINE_PRODUCT_IDS or (
        _text(row.get("product_family")) == BLINE_FAMILY
        and article in APPROVED_BLINE_ARTICLES
    )


def _conditional_identity_valid(row: pd.Series) -> bool:
    family = _family(row)
    product_id = _product_id(row)
    if not product_id:
        return False
    if family == MPLUS_FAMILY:
        return _text(row.get("assembly_model")) == "channel_body_x_drain_body_x_grate"
    article = _bline_article(row)
    expected_id = f"aco-showerdrain-b-finished-set-{article.replace('.', '-')}"
    return all((
        _text(row.get("product_family")) == BLINE_FAMILY,
        _text(row.get("assembly_model")) == "integral_all_in_one_set",
        article in APPROVED_BLINE_ARTICLES,
        product_id == expected_id,
        not _has_value(row.get("body_article_number")),
        not _has_value(row.get("grate_article_number")),
    ))


def _conditional_flow_projection(
    row: pd.Series,
    conditional_values: pd.DataFrame,
    scenario_id: str,
) -> dict[str, Any]:
    family = _family(row)
    family_label = "M+" if family == MPLUS_FAMILY else "B-line"
    if _has_value(row.get("flow_rate_lps")) or _has_value(row.get("selected_default_flow_rate_lps")):
        return _blocked(
            row,
            scenario_id,
            f"unexpected_{'mplus' if family == MPLUS_FAMILY else 'bline'}_scalar_default_present",
            f"{family_label} is blocked because an unconditional scalar/default flow was found.",
        )
    if not _conditional_identity_valid(row):
        return _blocked(
            row,
            scenario_id,
            "mismatched_conditional_product_identity",
            f"{family_label} product identity/model does not match its approved conditional policy.",
        )
    if scenario_id == NO_SCENARIO_SELECTED:
        return _blocked(
            row,
            scenario_id,
            SELECTION_REQUIRED,
            f"Select 10 mm or 20 mm head water level to display the {family_label} flow rate.",
        )

    condition = _SCENARIO_CONDITIONS[scenario_id]
    required_columns = {"set_id", "product_family", "assembly_model", "parameter_name", "condition_value"}
    if conditional_values.empty or not required_columns.issubset(conditional_values.columns):
        matches = conditional_values.iloc[0:0]
    else:
        matches = conditional_values[
            conditional_values["set_id"].map(_text).eq(_product_id(row))
            & conditional_values["product_family"].map(_text).eq(family)
            & conditional_values["assembly_model"].map(_text).eq(_text(row.get("assembly_model")))
            & conditional_values["parameter_name"].map(_text).eq("flow_rate_lps")
        ]
        numeric_conditions = pd.to_numeric(matches["condition_value"], errors="coerce")
        matches = matches[numeric_conditions.eq(condition["condition_value"])]

    if len(matches) == 0:
        return _blocked(
            row,
            scenario_id,
            "missing_matching_conditional_flow_value",
            f"No exact conditional {family_label} flow value exists for the selected head water level.",
            condition=condition,
            source=CONDITIONAL_SOURCE,
        )
    if len(matches) != 1:
        return _blocked(
            row,
            scenario_id,
            "duplicate_matching_conditional_flow_values",
            f"Multiple conditional {family_label} flow values matched; customer presentation is blocked.",
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
            f"Conditional {family_label} flow metadata/value did not exactly match the approved policy.",
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
        family = _family(row)
        bline_candidate = is_approved_bline_finished_set_candidate(row)
        if family == BLINE_FAMILY and not bline_candidate:
            # Family/discovery/catalog/evidence rows are not customer products.
            continue
        if not known_scenario:
            rows.append(_blocked(
                row,
                scenario_id,
                "unknown_customer_scenario_id",
                "Unknown customer scenario; no conditional value was applied.",
            ))
        elif family == MPLUS_FAMILY:
            rows.append(_conditional_flow_projection(row, conditions, scenario_id))
        elif bline_candidate:
            rows.append(_conditional_flow_projection(row, conditions, scenario_id))
        else:
            rows.append(_canonical_unconditional_projection(row, scenario_id))
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
