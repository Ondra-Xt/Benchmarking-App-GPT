import pandas as pd
import pytest

from src.scenario_scoring import (
    build_scenario_comparison,
    resolve_flow_rate_for_scenario,
    scoring_scenarios_dataframe,
)


def _mplus_product():
    return {
        "product_id": "aco-assembled-showerdrain-mplus-example",
        "manufacturer": "aco",
        "product_name": "ACO ShowerDrain M+ assembled set",
        "product_family": "showerdrain_mplus",
        "flow_rate_lps": "",
        "water_seal_mm": 50,
        "outlet_dn": "DN50",
        "height_adj_min_mm": 25,
        "height_adj_max_mm": 128,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "blocked_reason": "blocked_pending_conditional_parameter_scoring",
    }


def _conditional_values():
    product_id = _mplus_product()["product_id"]
    return pd.DataFrame(
        [
            {
                "set_id": product_id,
                "parameter_name": "flow_rate_lps",
                "value": 0.40,
                "condition_type": "head_water_level",
                "condition_value": 10,
                "condition_unit": "mm",
                "condition_label": "10 mm head water level",
            },
            {
                "set_id": product_id,
                "parameter_name": "flow_rate_lps",
                "value": 0.46,
                "condition_type": "head_water_level",
                "condition_value": 20,
                "condition_unit": "mm",
                "condition_label": "20 mm head water level",
            },
        ]
    )


@pytest.mark.parametrize(
    ("scenario_id", "expected_flow", "expected_condition"),
    [("flow_head_10mm", 0.40, 10), ("flow_head_20mm", 0.46, 20)],
)
def test_resolver_uses_only_exact_explicit_scenario(scenario_id, expected_flow, expected_condition):
    resolved = resolve_flow_rate_for_scenario(_mplus_product(), _conditional_values(), scenario_id)

    assert resolved["flow_rate_lps"] == expected_flow
    assert resolved["flow_rate_resolution_status"] == "resolved_from_condition"
    assert resolved["flow_rate_resolution_source"] == "Conditional_Technical_Values"
    assert resolved["flow_rate_condition_value"] == expected_condition
    assert resolved["scenario_ready_for_benchmark"] is True
    assert resolved["scenario_blocked_reason"] == ""


def test_resolver_does_not_select_conditional_default_without_scenario():
    product = _mplus_product()
    resolved = resolve_flow_rate_for_scenario(product, _conditional_values(), "no_scenario_selected")

    assert product["flow_rate_lps"] == ""
    assert resolved["flow_rate_lps"] == ""
    assert resolved["flow_rate_resolution_status"] == "unresolved"
    assert resolved["scenario_ready_for_benchmark"] is False
    assert resolved["scenario_blocked_reason"] == "blocked_pending_conditional_parameter_scoring"


def test_scalar_unconditional_flow_is_reused_without_conditional_override():
    product = {
        **_mplus_product(),
        "product_id": "unconditional-product",
        "product_family": "other",
        "flow_rate_lps": 0.8,
        "ready_for_benchmark": True,
        "blocked_reason": "",
    }
    resolved = resolve_flow_rate_for_scenario(product, _conditional_values(), "flow_head_10mm")

    assert resolved["flow_rate_lps"] == 0.8
    assert resolved["flow_rate_resolution_source"] == "scalar_unconditional"
    assert resolved["scenario_ready_for_benchmark"] is True


def test_scenario_registry_and_comparison_have_stable_contract():
    scenarios = scoring_scenarios_dataframe()
    comparison = build_scenario_comparison(
        pd.DataFrame([_mplus_product()]), _conditional_values(), "flow_head_20mm"
    )

    assert scenarios["scenario_id"].tolist() == [
        "no_scenario_selected",
        "flow_head_10mm",
        "flow_head_20mm",
    ]
    assert scenarios.loc[scenarios["is_default"], "scenario_id"].tolist() == ["no_scenario_selected"]
    assert not scenarios["customer_view_enabled"].any()
    assert comparison.loc[0, "scenario_id"] == "flow_head_20mm"
    assert comparison.loc[0, "flow_rate_lps"] == 0.46
