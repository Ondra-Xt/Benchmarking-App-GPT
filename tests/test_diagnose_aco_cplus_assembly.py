import pandas as pd

import tools.diagnose_aco_cplus_assembly as mod


def test_compute_cplus_diagnostic_counts_and_ids(monkeypatch):
    products = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"product_id": "aco-showerdrain-cplus-low-h69", "flow_rate_lps": 0.62, "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
        {"product_id": "x"},
    ])
    comparison = pd.DataFrame([{"product_id": "x"}])
    excluded = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-grate-a"},
        {"product_id": "aco-showerdrain-cplus-grate-b"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "component_id": "aco-showerdrain-cplus-grate-a", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
        {"product_id": "aco-showerdrain-cplus-low-h69", "component_id": "aco-showerdrain-cplus-grate-b", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
    ])
    coverage = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"product_id": "aco-showerdrain-cplus-low-h69", "flow_rate_lps": 0.62, "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
    ])

    monkeypatch.setattr(mod, "discover_cplus_sources", lambda: (["https://example.test/cplus"], ["https://example.test/design"], [], []))
    diag = mod.compute_cplus_diagnostic(products, comparison, excluded, bom, coverage)

    assert diag.base_count == 2
    assert diag.valid_grate_count == 2
    assert len(diag.proposed_ids) == 4
    assert diag.missing_grate_components == []
    assert diag.duplicate_ids == []
    assert diag.missing_base_scoring_fields == {}
    assert diag.base_field_values["aco-showerdrain-cplus-standard-h92"]["flow_rate_lps"] == "0.91"


def test_compute_cplus_diagnostic_flags_risks(monkeypatch):
    products = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"product_id": "aco-showerdrain-cplus-low-h69", "flow_rate_lps": "", "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
        {"product_id": "aco-assembled-showerdrain-cplus-standard-h92__showerdrain-cplus-grate-a"},
    ])
    comparison = pd.DataFrame([{"product_id": "x"}])
    excluded = pd.DataFrame([{"product_id": "aco-showerdrain-cplus-grate-a"}])
    bom = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "component_id": "aco-showerdrain-cplus-grate-a", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
        {"product_id": "aco-showerdrain-cplus-low-h69", "component_id": "aco-showerdrain-cplus-grate-missing", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
        {"product_id": "aco-showerdrain-cplus-grate-a", "component_id": "aco-showerdrain-cplus-grate-a", "option_type": "compatible_grate", "parent_family": "showerdrain_cplus", "option_family": "showerdrain_cplus"},
    ])
    coverage = pd.DataFrame([
        {"product_id": "aco-showerdrain-cplus-standard-h92", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"product_id": "aco-showerdrain-cplus-low-h69", "flow_rate_lps": "", "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
    ])

    monkeypatch.setattr(mod, "discover_cplus_sources", lambda: (["https://example.test/cplus"], ["https://example.test/design"], [], []))
    diag = mod.compute_cplus_diagnostic(products, comparison, excluded, bom, coverage)

    assert diag.missing_grate_components == ["aco-showerdrain-cplus-grate-missing"]
    assert diag.dangling_component_ids == 1
    assert diag.self_reference_rows == 1
    assert diag.grate_to_grate_links == 1
    assert diag.duplicate_ids == ["aco-assembled-showerdrain-cplus-standard-h92__showerdrain-cplus-grate-a"]
    assert diag.missing_base_scoring_fields == {
        "aco-showerdrain-cplus-low-h69": ["flow_rate_lps"],
    }


def test_discover_cplus_sources_identifies_design_and_evidence(monkeypatch):
    html_cplus = "<html><body><a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-roste-aus-geschliffenem-edelstahl/'>Design-Roste</a></body></html>"
    html_design = "<html><body>Rinnenkörper ACO ShowerDrain C, kompatibel zu ACO ShowerDrain C+.</body></html>"
    calls = []

    def fake_get(url, timeout=35):
        calls.append(url)
        if "cplus" in url or "c+" in url:
            return 200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/", html_cplus, None
        if "design-roste" in url:
            return 200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-roste-aus-geschliffenem-edelstahl/", html_design, None
        return 404, url, "", "not found"

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    sources, design, evidence, notes = mod.discover_cplus_sources()
    assert any("aco-showerdrain-c+" in u or "aco-showerdrain-cplus" in u for u in sources)
    assert any("design-roste" in u for u in design)
    assert any("design-roste" in u for u in evidence)
    assert any("implicit family-level" in n.lower() for n in notes)
