import pandas as pd

import tools.diagnose_easyflow_assembled_fields as mod


ID1 = "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__aco-easyflow-aufsatzstuecke-fuer-designroste"
ID2 = "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__aco-easyflow-grate-design-roste-design-roste"
BASE = "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50"
COMP1 = "aco-easyflow-aufsatzstuecke-fuer-designroste"
COMP2 = "aco-easyflow-grate-design-roste-design-roste"


def test_parse_assembled_id_expected_easyflow_ids_no_extra_prefixes():
    b, c, ok = mod.parse_assembled_id(ID1)
    assert ok is True
    assert b == BASE
    assert c == COMP1
    assert not b.startswith("aco-aco-")
    assert not c.startswith("aco-aco-")

    b2, c2, ok2 = mod.parse_assembled_id(ID2)
    assert ok2 is True
    assert b2 == BASE
    assert c2 == COMP2


def test_parse_assembled_id_failure_when_separator_missing():
    b, c, ok = mod.parse_assembled_id("aco-assembled-easyflow-badformat")
    assert (b, c, ok) == ("", "", False)


def test_verdict_base_has_fields_but_not_copied():
    products = pd.DataFrame([
        {"product_id": BASE, "flow_rate_lps": "0.6", "water_seal_mm": "50", "outlet_dn": "DN50", "height_adj_min_mm": "70", "height_adj_max_mm": "110"},
        {"product_id": ID1, "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
    ])
    comparison = pd.DataFrame([])
    excluded = pd.DataFrame([{"product_id": COMP1}])
    registry = pd.DataFrame([])
    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert reports[0].verdict == "base row has fields but inheritance did not copy them"
    assert reports[0].base_source == "Products"
    assert reports[0].component_source == "Components"


def test_verdict_base_also_missing_fields():
    products = pd.DataFrame([
        {"product_id": BASE, "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
        {"product_id": ID1, "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
    ])
    comparison = pd.DataFrame([])
    excluded = pd.DataFrame([{"product_id": COMP1}])
    registry = pd.DataFrame([])
    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert reports[0].verdict == "base row also missing fields"


def test_verdict_base_missing_and_component_missing_with_fuzzy_candidates():
    products = pd.DataFrame([
        {"product_id": ID2, "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
        {"product_id": "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50-alt"},
    ])
    comparison = pd.DataFrame([{"product_id": "aco-easyflow-grate-design-roste"}])
    excluded = pd.DataFrame([])
    registry = pd.DataFrame([])
    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert reports[0].verdict == "base_id does not exist in final universe"
    assert "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50-alt" in reports[0].base_fuzzy_candidates


def test_verdict_component_missing_with_fuzzy_candidates():
    products = pd.DataFrame([
        {"product_id": BASE, "flow_rate_lps": "0.6", "water_seal_mm": "50", "outlet_dn": "DN50", "height_adj_min_mm": "70", "height_adj_max_mm": "110"},
        {"product_id": ID2, "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
    ])
    comparison = pd.DataFrame([{"product_id": "aco-easyflow-grate-design-roste"}])
    excluded = pd.DataFrame([])
    registry = pd.DataFrame([])
    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert reports[0].verdict == "component_id mismatch"
    assert "aco-easyflow-grate-design-roste" in reports[0].component_fuzzy_candidates
