from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from src.customer_scenario_view import (
    CONDITIONAL_SOURCE,
    CUSTOMER_SCENARIO_RUN_KEY,
    CUSTOMER_SCENARIO_STATE_KEY,
    FLOW_HEAD_10MM,
    FLOW_HEAD_20MM,
    NO_SCENARIO_SELECTED,
    SELECTION_REQUIRED,
    build_customer_scenario_projection,
    reset_customer_scenario_for_run,
)


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    conditions: list[dict[str, object]] = []
    for index in range(4):
        product_id = f"aco-assembled-showerdrain-mplus-{index}"
        rows.append({
            "product_id": product_id,
            "product_family": "showerdrain_mplus",
            "flow_rate_lps": "",
            "selected_default_flow_rate_lps": "",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "blocked_reason": "blocked_pending_conditional_parameter_scoring",
        })
        for head, flow in ((10, 0.40), (20, 0.46)):
            conditions.append({
                "set_id": product_id,
                "product_family": "showerdrain_mplus",
                "parameter_name": "flow_rate_lps",
                "value": flow,
                "unit": "l/s",
                "condition_type": "head_water_level",
                "condition_value": head,
                "condition_unit": "mm",
                "condition_label": f"{head} mm head water level",
            })
    rows.extend([
        {
            "product_id": "aco-cplus",
            "product_family": "showerdrain_cplus",
            "flow_rate_lps": 0.5,
            "ready_for_benchmark": True,
            "ready_for_customer_view": True,
            "customer_view_enabled": True,
        },
        {
            "product_id": "aco-bline",
            "product_family": "showerdrain_b",
            "flow_rate_lps": "",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
        },
        {
            "product_id": "aco-easyflow",
            "product_family": "easyflow",
            "flow_rate_lps": "",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
        },
    ])
    return pd.DataFrame(rows), pd.DataFrame(conditions)


def _mplus(projection: pd.DataFrame) -> pd.DataFrame:
    return projection[projection["product_family"].eq("showerdrain_mplus")]


def test_no_selection_keeps_all_mplus_rows_blocked() -> None:
    products, conditions = _frames()
    result = _mplus(build_customer_scenario_projection(products, conditions, NO_SCENARIO_SELECTED))

    assert len(result) == 4
    assert result["flow_rate_lps"].fillna("").eq("").all()
    assert result["customer_ready_for_selected_scenario"].eq(False).all()
    assert result["customer_blocked_reason"].eq(SELECTION_REQUIRED).all()
    assert result["customer_presentation_note"].str.contains("Select 10 mm or 20 mm").all()


def test_explicit_head_scenarios_resolve_exact_approved_values_with_conditions() -> None:
    products, conditions = _frames()
    for scenario_id, expected_flow, expected_head in (
        (FLOW_HEAD_10MM, 0.40, 10),
        (FLOW_HEAD_20MM, 0.46, 20),
    ):
        result = _mplus(build_customer_scenario_projection(products, conditions, scenario_id))
        assert len(result) == 4
        assert result["flow_rate_lps"].eq(expected_flow).all()
        assert result["customer_ready_for_selected_scenario"].eq(True).all()
        assert result["flow_rate_resolution_status"].eq("resolved_from_condition").all()
        assert result["flow_rate_resolution_source"].eq(CONDITIONAL_SOURCE).all()
        assert result["flow_rate_condition_type"].eq("head_water_level").all()
        assert result["flow_rate_condition_value"].eq(expected_head).all()
        assert result["flow_rate_condition_unit"].eq("mm").all()
        assert result["flow_rate_condition_label"].eq(
            f"{expected_head} mm head water level"
        ).all()
        assert result["customer_presentation_note"].str.contains(
            f"at {expected_head} mm head water level", regex=False
        ).all()


def test_unknown_missing_duplicate_and_invalid_metadata_fail_closed() -> None:
    products, conditions = _frames()
    mplus_products = products[products["product_family"].eq("showerdrain_mplus")]

    unknown = _mplus(build_customer_scenario_projection(products, conditions, "flow_head_15mm"))
    assert unknown["customer_ready_for_selected_scenario"].eq(False).all()
    assert unknown["customer_blocked_reason"].eq("unknown_customer_scenario_id").all()

    missing_conditions = conditions[~conditions["set_id"].eq(mplus_products.iloc[0]["product_id"])]
    missing = _mplus(build_customer_scenario_projection(products, missing_conditions, FLOW_HEAD_10MM))
    assert not missing.iloc[0]["customer_ready_for_selected_scenario"]
    assert missing.iloc[0]["customer_blocked_reason"] == "missing_matching_conditional_flow_value"

    duplicate_conditions = pd.concat([conditions, conditions.iloc[[0]]], ignore_index=True)
    duplicate = _mplus(build_customer_scenario_projection(products, duplicate_conditions, FLOW_HEAD_10MM))
    assert not duplicate.iloc[0]["customer_ready_for_selected_scenario"]
    assert duplicate.iloc[0]["customer_blocked_reason"] == "duplicate_matching_conditional_flow_values"

    wrong_metadata = conditions.copy(deep=True)
    wrong_metadata.loc[wrong_metadata.index[0], "condition_unit"] = "cm"
    invalid = _mplus(build_customer_scenario_projection(products, wrong_metadata, FLOW_HEAD_10MM))
    assert not invalid.iloc[0]["customer_ready_for_selected_scenario"]
    assert invalid.iloc[0]["customer_blocked_reason"] == "invalid_conditional_flow_metadata_or_value"
    assert invalid.iloc[0]["flow_rate_lps"] == ""


def test_scalar_default_blocks_resolution_and_inputs_are_not_mutated() -> None:
    products, conditions = _frames()
    original_products = products.copy(deep=True)
    original_conditions = conditions.copy(deep=True)
    products.loc[0, "flow_rate_lps"] = 0.40
    scalar_input = products.copy(deep=True)

    result = _mplus(build_customer_scenario_projection(products, conditions, FLOW_HEAD_10MM))

    assert not result.iloc[0]["customer_ready_for_selected_scenario"]
    assert result.iloc[0]["customer_blocked_reason"] == "unexpected_mplus_scalar_default_present"
    assert result.iloc[0]["flow_rate_lps"] == ""
    assert_frame_equal(products, scalar_input)
    assert_frame_equal(conditions, original_conditions)
    assert original_products.loc[
        original_products["product_family"].eq("showerdrain_mplus"), "flow_rate_lps"
    ].fillna("").eq("").all()


def test_adjacent_customer_policies_are_preserved() -> None:
    products, conditions = _frames()
    result = build_customer_scenario_projection(products, conditions, FLOW_HEAD_10MM)

    cplus = result[result["product_family"].eq("showerdrain_cplus")]
    bline = result[result["product_family"].eq("showerdrain_b")]
    easyflow = result[result["product_family"].eq("easyflow")]
    assert cplus["customer_ready_for_selected_scenario"].eq(True).all()
    assert bline["customer_ready_for_selected_scenario"].eq(False).all()
    assert bline["customer_blocked_reason"].eq("bline_customer_policy_not_approved").all()
    assert easyflow["customer_ready_for_selected_scenario"].eq(False).all()


def test_new_run_clears_stale_session_selection() -> None:
    state = {
        CUSTOMER_SCENARIO_STATE_KEY: FLOW_HEAD_20MM,
        CUSTOMER_SCENARIO_RUN_KEY: "update-old",
    }
    assert reset_customer_scenario_for_run(state, "update-new")
    assert state[CUSTOMER_SCENARIO_STATE_KEY] == NO_SCENARIO_SELECTED
    assert state[CUSTOMER_SCENARIO_RUN_KEY] == "update-new"
    assert not reset_customer_scenario_for_run(state, "update-new")


def test_streamlit_control_has_a_non_condition_default_and_resets_on_new_runs() -> None:
    source = Path("app.py").read_text(encoding="utf-8")
    assert "options=list(CUSTOMER_SCENARIO_OPTIONS)" in source
    assert "st.session_state[CUSTOMER_SCENARIO_STATE_KEY] = NO_SCENARIO_SELECTED" in source
    assert source.count("reset_customer_scenario_for_run(st.session_state, run_id)") == 2
    assert "index=1" not in source
    assert "index=2" not in source
