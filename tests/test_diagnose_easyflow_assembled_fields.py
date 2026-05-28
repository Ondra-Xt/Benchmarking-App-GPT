import pandas as pd

import tools.diagnose_easyflow_assembled_fields as mod


def test_parse_assembled_id_success_and_failure():
    b, c, ok = mod.parse_assembled_id("aco-assembled-easyflow-foo__bar")
    assert ok is True
    assert b == "aco-easyflow-foo"
    assert c == "aco-bar"

    b2, c2, ok2 = mod.parse_assembled_id("aco-assembled-easyflow-foo")
    assert (b2, c2, ok2) == ("", "", False)


def test_diagnose_easyflow_rows_inheritance_not_copied():
    products = pd.DataFrame([
        {"product_id": "aco-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50", "flow_rate_lps": "0.6", "water_seal_mm": "50", "outlet_dn": "DN50", "height_adj_min_mm": "70", "height_adj_max_mm": "110"},
        {"product_id": "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__aco-easyflow-aufsatzstuecke-fuer-designroste", "flow_rate_lps": "", "water_seal_mm": "", "outlet_dn": "", "height_adj_min_mm": "", "height_adj_max_mm": ""},
    ])
    comparison = pd.DataFrame([])
    excluded = pd.DataFrame([
        {"product_id": "aco-aco-easyflow-aufsatzstuecke-fuer-designroste"}
    ])
    registry = pd.DataFrame([])

    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert len(reports) == 1
    assert reports[0].verdict == "base row has fields but inheritance did not copy them"
    assert reports[0].base_source == "Products"
    assert reports[0].component_source == "Components"


def test_diagnose_easyflow_rows_parse_fail_and_missing_base():
    products = pd.DataFrame([
        {"product_id": "aco-assembled-easyflow-badformat"},
        {"product_id": "aco-assembled-easyflow-easyflow-base__missing-component"},
    ])
    comparison = pd.DataFrame([])
    excluded = pd.DataFrame([])
    registry = pd.DataFrame([])

    reports = mod.diagnose_easyflow_rows(products, comparison, excluded, registry)
    assert reports[0].verdict == "assembled product_id parsing failed"
    assert reports[1].verdict == "base_id does not exist in final universe"
