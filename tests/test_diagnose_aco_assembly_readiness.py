import pandas as pd

import tools.diagnose_aco_assembly_readiness as mod


def _frames():
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain-c-base1__grate", "product_family": "showerdrain_c"},
        {"product_id": "aco-showerdrain-c-base1", "product_family": "showerdrain_c", "flow_rate_lps": 1.0, "water_seal_mm": 20, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-easyflow-base1", "product_family": "easyflow", "flow_rate_lps": 1.1, "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-easyflowplus-base1", "product_family": "easyflowplus", "flow_rate_lps": "", "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
            ])
    comparison = pd.DataFrame([
        {"product_id": "cmp-1"}, {"product_id": "cmp-1"}
    ])
    excluded = pd.DataFrame([
        {"product_id": "aco-easyflow-grate1", "option_family": "easyflow"},
        {"product_id": "aco-easyflowplus-grate1", "option_family": "easyflowplus"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-easyflow-base1", "component_id": "aco-easyflow-grate1", "option_type": "compatible_grate", "parent_family": "easyflow", "option_family": "easyflow"},
        {"product_id": "aco-easyflow-base1", "component_id": "aco-easyflowplus-grate1", "option_type": "compatible_grate", "parent_family": "easyflow", "option_family": "easyflow"},
        {"product_id": "aco-easyflowplus-base1", "component_id": "aco-easyflowplus-grate1", "option_type": "compatible_grate", "parent_family": "easyflowplus", "option_family": "easyflowplus"},
    ])
    return products, comparison, excluded, bom


def test_readiness_hardening_rules():
    products, comparison, excluded, bom = _frames()
    diag = mod.compute_readiness(products, comparison, excluded, bom)

    assert diag.families["showerdrain_c"].status.startswith("ALREADY_ACTIVE")
    assert diag.families["showerdrain_c"].proposed_n == 0

    easy = diag.families["easyflow"]
    assert easy.status == "BLOCKED"
    assert "cross-family mixing detected without explicit evidence" in easy.reasons
    assert any("easyflowplus" in x for x in easy.cross_family_mixing)
    assert all("aco-assembled-aco-assembled" not in p for p in easy.proposed_ids)

    eplus = diag.families["easyflowplus"]
    assert eplus.status == "BLOCKED"
    assert "proposed rows have missing base scoring fields" in eplus.reasons
    assert "aco-easyflowplus-base1" in eplus.missing_base_scoring_fields

    assert diag.duplicate_product_ids_in_products == []
    assert diag.duplicate_product_ids_in_comparison == ["cmp-1"]


def test_explicit_cross_family_evidence_allows_component():
    products, comparison, excluded, bom = _frames()
    bom.loc[1, "compatibility_evidence"] = "Explicit cross-family compatibility from vendor matrix"
    diag = mod.compute_readiness(products, comparison, excluded, bom)
    easy = diag.families["easyflow"]
    assert easy.proposed_n > 0
