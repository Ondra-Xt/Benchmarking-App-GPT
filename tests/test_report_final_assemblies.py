import pandas as pd

import tools.report_final_assemblies as mod


def test_summarize_final_assemblies_groups_and_missing_fields():
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain_splus-a__g1", "flow_rate_lps": 1.0, "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-assembled-showerdrain_c-a__g1", "flow_rate_lps": "", "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
        {"product_id": "aco-assembled-mycustom-x__y", "flow_rate_lps": 0.8, "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 8, "height_adj_max_mm": 18},
        {"product_id": "aco-normal-base", "flow_rate_lps": 1.1},
    ])

    summary = mod.summarize_final_assemblies(products)

    assert summary["total"] == 3
    assert summary["count_by_family"]["showerdrain_splus"] == 1
    assert summary["count_by_family"]["showerdrain_c"] == 1
    assert summary["count_by_family"]["mycustom"] == 1
    assert summary["missing_by_id"] == {
        "aco-assembled-showerdrain_c-a__g1": ["flow_rate_lps"]
    }


def test_main_prints_blocked_families(monkeypatch, capsys):
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain_splus-a__g1", "product_family": "showerdrain_splus", "flow_rate_lps": 1.0, "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 10, "height_adj_max_mm": 20},
    ])
    comparison = pd.DataFrame([{"product_id": "x"}])
    excluded = pd.DataFrame([])
    bom = pd.DataFrame([])

    monkeypatch.setattr(mod.aco, "discover_candidates", lambda target_length_mm, tolerance_mm: ([], {}))
    monkeypatch.setattr(mod.pipeline, "run_update", lambda registry, cfg: (products, comparison, excluded, pd.DataFrame([]), bom))

    rc = mod.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert "total assembled rows: 1" in out
    assert "showerdrain_cplus: BLOCKED" in out
    assert "showerdrain_mplus: BLOCKED" in out
    assert "showerdrain_eplus: BLOCKED" in out
