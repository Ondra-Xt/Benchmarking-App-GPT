import pandas as pd

import tools.diagnose_eplus_base_row_sources as mod


def test_collect_eplus_source_urls_includes_known_and_output_urls():
    products = pd.DataFrame([
        {"product_id": "aco-x", "product_family": "other", "product_url": "https://example.test/other"},
        {"product_id": "aco-e", "product_family": "showerdrain_eplus", "source_url": "https://example.test/eplus/base#tab"},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-g", "option_family": "showerdrain_eplus", "product_url": "https://example.test/eplus/grate?x=1"},
    ])

    urls = mod.collect_eplus_source_urls(products, components)

    assert "https://example.test/eplus/base/" in urls
    assert "https://example.test/eplus/grate/" in urls
    assert any("aco-showerdrain-eplus" in url for url in urls)
    assert "https://example.test/other/" not in urls


def test_parse_eplus_source_url_classifies_base_drain_grate_and_accessory(monkeypatch):
    html_by_url = {
        "https://example.test/eplus/rinnenkoerper/": """
            <main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1>
            <table><thead><tr><th>Artikel-Nr.</th><th>L1</th><th>Passend für</th></tr></thead>
            <tbody><tr><td>9010.90.11</td><td>L1 1185 mm</td><td>passend für ShowerDrain E+ Design-Roste</td></tr></tbody></table></main>
        """,
        "https://example.test/eplus/ablaufkoerper/": """
            <main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain E+</h1>
            <table><tr><th>Artikel-Nr.</th><th>DN</th><th>Ablaufleistung</th><th>Sperrwasserhöhe</th><th>Einbauhöhe</th></tr>
            <tr><td>9010.90.21</td><td>DN 50</td><td>0,8 l/s</td><td>Sperrwasserhöhe 50 mm</td><td>Einbauhöhe 25-128 mm</td></tr></table></main>
        """,
        "https://example.test/eplus/design-roste/": """
            <main><h1>Design-Roste aus elektropoliertem Edelstahl</h1>
            <table><tr><th>Artikel-Nr.</th><th>Länge</th><th>Beschreibung</th></tr>
            <tr><td>9010.90.31</td><td>Länge 1200 mm</td><td>Rost passend für ACO ShowerDrain E+</td></tr></table></main>
        """,
        "https://example.test/eplus/aco-showerstep-gefaellekeil/": """
            <main><h1>ACO ShowerStep Gefällekeil</h1>
            <table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.90.41</td><td>Zubehör passend für ShowerDrain E+</td></tr></table></main>
        """,
    }

    def fake_get(url, timeout=35):
        return 200, url, html_by_url[url], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, base = mod.parse_eplus_source_url("https://example.test/eplus/rinnenkoerper/")
    _url, drain = mod.parse_eplus_source_url("https://example.test/eplus/ablaufkoerper/")
    _url, grate = mod.parse_eplus_source_url("https://example.test/eplus/design-roste/")
    _url, accessory = mod.parse_eplus_source_url("https://example.test/eplus/aco-showerstep-gefaellekeil/")

    assert base[0].candidate_type == "eplus_base_body_candidate"
    assert base[0].system_role == "drain_unit"
    assert base[0].length_mm == "1185"
    assert base[0].compatible_with.startswith("passend für")
    assert drain[0].candidate_type == "eplus_drain_body_candidate"
    assert drain[0].flow_rate_lps == "0.8"
    assert drain[0].water_seal_mm == "50"
    assert drain[0].outlet_dn == "DN50"
    assert drain[0].height_adj_min_mm == "25"
    assert drain[0].height_adj_max_mm == "128"
    assert grate[0].candidate_type == "eplus_grate_component"
    assert accessory[0].candidate_type == "eplus_accessory"


def test_build_diagnostic_keeps_eplus_blocked_for_incomplete_base_model(monkeypatch):
    products = pd.DataFrame()
    comparison = pd.DataFrame()
    candidates_all = pd.DataFrame([
        {"product_id": "aco-base", "product_family": "showerdrain_eplus", "product_url": "https://example.test/eplus/rinnenkoerper/"},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-grate", "product_family": "showerdrain_eplus", "system_role": "grate", "product_url": "https://example.test/eplus/design-roste/"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-base", "parent_family": "showerdrain_eplus", "component_id": "aco-grate", "option_family": "showerdrain_eplus", "option_type": "compatible_grate"},
    ])
    html_by_url = {
        mod._canonical_url(url): "<main><h1>ACO ShowerDrain E+</h1>E+ family page</main>"
        for url in mod.KNOWN_EPLUS_SOURCE_URLS
    }
    html_by_url.update({
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[1]): "<main><h1>ACO ShowerStep Gefällekeil</h1><table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.90.41</td><td>Zubehör ShowerDrain E+</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[2]): "<main><h1>Design-Roste aus elektropoliertem Edelstahl</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.90.31</td><td>Länge 1200 mm</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[3]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1><table><tr><th>Artikel-Nr.</th><th>L1</th></tr><tr><td>9010.90.11</td><td>L1 1185 mm</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[4]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 57-128 mm</h1><table><tr><th>Artikel-Nr.</th><th>L1</th><th>DN</th></tr><tr><td>9010.90.12</td><td>L1 1185 mm</td><td>DN 50</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[5]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 80-128 mm DIN EN 1253-1</h1><table><tr><th>Artikel-Nr.</th><th>L1</th><th>Ablaufleistung</th></tr><tr><td>9010.90.13</td><td>L1 1185 mm</td><td>0,8 l/s</td></tr></table></main>",
        "https://example.test/eplus/rinnenkoerper/": "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1><table><tr><th>Artikel-Nr.</th><th>L1</th></tr><tr><td>9010.90.12</td><td>L1 1185 mm</td></tr></table></main>",
        "https://example.test/eplus/design-roste/": "<main><h1>Design-Roste aus elektropoliertem Edelstahl</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.90.32</td><td>Länge 1200 mm</td></tr></table></main>",
    })

    def fake_get(url, timeout=35):
        canonical = mod._canonical_url(url)
        return 200, canonical, html_by_url[canonical], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(products, comparison, candidates_all, components, bom)

    assert diag.readiness.eplus_products_present == 0
    assert diag.readiness.eplus_components_present == 1
    assert diag.readiness.eplus_base_body_candidates >= 1
    assert diag.readiness.eplus_grate_candidates >= 1
    assert diag.readiness.eplus_accessory_candidates >= 1
    assert diag.readiness.likely_assembly_model == "base_x_grate"
    assert diag.readiness.safe_to_generate_eplus_assemblies is False
    assert "missing fields" in diag.readiness.blocking_reason
    assert diag.production_behavior_changed is False
