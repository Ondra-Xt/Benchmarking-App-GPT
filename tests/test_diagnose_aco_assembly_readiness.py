import pandas as pd

import tools.diagnose_aco_assembly_readiness as mod


def test_compute_readiness_reports_ready_blocked_and_active():
    products = pd.DataFrame([
        {"product_id": "aco-showerdrain-splus-base-1", "flow_rate_lps": 1, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-assembled-showerdrain-splus-x__y", "flow_rate_lps": 1, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": 0.9, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-showerdrain-b-base", "flow_rate_lps": "", "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
    ])
    comparison = pd.DataFrame([{"product_id": "cmp-1"}])
    excluded = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-grate-a"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "component_id": "aco-showerdrain-cplus-grate-a", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
        {"product_id": "aco-showerdrain-b-base", "component_id": "aco-showerdrain-b-grate-missing", "option_type": "compatible_grate", "parent_family": "showerdrain_b", "option_family": "showerdrain_b"},
    ])

    reports, risk = mod.compute_readiness(products, comparison, excluded, bom)
    by_family = {r.family: r for r in reports}

    assert by_family["showerdrain_splus"].status == "ALREADY_ACTIVE / BASELINE_PROTECTED"
    assert by_family["showerdrain_splus"].current_assembled == 1

    assert by_family["showerdrain_cplus"].status == "READY"
    assert by_family["showerdrain_cplus"].proposed_n == 1

    assert by_family["showerdrain_b"].status == "BLOCKED"
    assert "no compatible_grate component_id rows present" in by_family["showerdrain_b"].blocking_reason

    assert risk.dangling_component_ids == 1
    assert risk.self_reference_rows == 0


def test_compute_readiness_risk_flags():
    products = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": "", "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-showerdrain-cplus-grate-a", "flow_rate_lps": 1, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
    ])
    comparison = pd.DataFrame([{"product_id": "aco-showerdrain-cplus-grate-a"}])
    excluded = pd.DataFrame([{"product_id": "aco-showerdrain-cplus-grate-a"}])
    bom = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-grate-a", "component_id": "aco-showerdrain-cplus-grate-a", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
    ])

    _, risk = mod.compute_readiness(products, comparison, excluded, bom)

    assert risk.grate_to_grate_links == 1
    assert risk.self_reference_rows == 1
    assert "aco-showerdrain-cplus-standard-h92" in risk.missing_base_scoring_fields
    assert risk.leaked_component_ids_in_products == ["aco-showerdrain-cplus-grate-a"]
    assert risk.leaked_component_ids_in_comparison == ["aco-showerdrain-cplus-grate-a"]
