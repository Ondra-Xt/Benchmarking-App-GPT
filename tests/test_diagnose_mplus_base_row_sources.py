import pandas as pd

import tools.diagnose_mplus_base_row_sources as mod


def test_collect_mplus_source_urls_includes_known_and_output_urls():
    products = pd.DataFrame([
        {"product_id": "aco-x", "product_family": "other", "product_url": "https://example.test/other"},
        {"product_id": "aco-m", "product_family": "showerdrain_mplus", "source_url": "https://example.test/mplus/base#tab"},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-g", "option_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/grate?x=1"},
    ])

    urls = mod.collect_mplus_source_urls(products, components)

    assert "https://example.test/mplus/base/" in urls
    assert "https://example.test/mplus/grate/" in urls
    assert any("aco-showerdrain-mplus" in url for url in urls)
    assert "https://example.test/other/" not in urls


def test_parse_mplus_source_url_classifies_channel_drain_grate_and_accessory(monkeypatch):
    html_by_url = {
        "https://example.test/mplus/rinnenkoerper/": """
            <main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1>
            <table><thead><tr><th>Artikel-Nr.</th><th>L1</th><th>Passend für</th></tr></thead>
            <tbody><tr><td>9010.90.11</td><td>L1 1185 mm</td><td>passend für ShowerDrain M+ Ablaufkörper</td></tr></tbody></table></main>
        """,
        "https://example.test/mplus/ablaufkoerper/": """
            <main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1>
            <table><tr><th>Artikel-Nr.</th><th>DN</th><th>Ablaufleistung</th><th>Sperrwasserhöhe</th><th>Einbauhöhe</th></tr>
            <tr><td>9010.90.21</td><td>DN 50</td><td>0,8 l/s</td><td>Sperrwasserhöhe 50 mm</td><td>Einbauhöhe 25-128 mm</td></tr></table></main>
        """,
        "https://example.test/mplus/design-roste/": """
            <main><h1>Design-Roste aus elektropoliertem Edelstahl</h1>
            <table><tr><th>Artikel-Nr.</th><th>Länge</th><th>Beschreibung</th></tr>
            <tr><td>9010.90.31</td><td>Länge 1200 mm</td><td>Rost passend für ACO ShowerDrain M+</td></tr></table></main>
        """,
        "https://example.test/mplus/zubehoer/": """
            <main><h1>Zubehör ACO ShowerDrain M+</h1>
            <table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.90.41</td><td>Adapter passend für ShowerDrain M+</td></tr></table></main>
        """,
    }

    def fake_get(url, timeout=35):
        return 200, url, html_by_url[url], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, channel = mod.parse_mplus_source_url("https://example.test/mplus/rinnenkoerper/")
    _url, drain = mod.parse_mplus_source_url("https://example.test/mplus/ablaufkoerper/")
    _url, grate = mod.parse_mplus_source_url("https://example.test/mplus/design-roste/")
    _url, accessory = mod.parse_mplus_source_url("https://example.test/mplus/zubehoer/")

    assert channel[0].candidate_type == "mplus_channel_body_candidate"
    assert channel[0].length_mm == "1185"
    assert channel[0].compatible_with.startswith("passend für")
    assert drain[0].candidate_type == "mplus_drain_body_candidate"
    assert drain[0].flow_rate_lps == "0.8"
    assert drain[0].water_seal_mm == "50"
    assert drain[0].outlet_dn == "DN50"
    assert drain[0].height_adj_min_mm == "25"
    assert drain[0].height_adj_max_mm == "128"
    assert grate[0].candidate_type == "mplus_grate_component"
    assert accessory[0].candidate_type == "mplus_accessory"


def test_build_diagnostic_keeps_mplus_blocked_for_compound_model(monkeypatch):
    products = pd.DataFrame([
        {"product_id": "aco-existing", "product_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/rinnenkoerper/"},
    ])
    comparison = pd.DataFrame()
    candidates_all = pd.DataFrame([
        {"product_id": "aco-candidate", "product_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/ablaufkoerper/"},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-grate", "product_family": "showerdrain_mplus", "system_role": "grate", "product_url": "https://example.test/mplus/design-roste/"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-existing", "parent_family": "showerdrain_mplus", "component_id": "aco-grate", "option_family": "showerdrain_mplus", "option_type": "informational"},
    ])
    html_by_url = {
        mod._canonical_url(mod.KNOWN_MPLUS_SOURCE_URLS[0]): "<main><h1>ACO ShowerDrain M+</h1>M+ family page</main>",
        mod._canonical_url(mod.KNOWN_MPLUS_SOURCE_URLS[1]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1><table><tr><th>Artikel-Nr.</th><th>L1</th></tr><tr><td>9010.90.11</td><td>L1 1185 mm</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_MPLUS_SOURCE_URLS[2]): "<main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1><table><tr><th>Artikel-Nr.</th><th>DN</th><th>Ablaufleistung</th><th>Sperrwasserhöhe</th><th>Einbauhöhe</th></tr><tr><td>9010.90.21</td><td>DN 50</td><td>0,8 l/s</td><td>Sperrwasserhöhe 50 mm</td><td>Einbauhöhe 25-128 mm</td></tr></table></main>",
        mod._canonical_url(mod.KNOWN_MPLUS_SOURCE_URLS[3]): "<main><h1>Design-Roste aus elektropoliertem Edelstahl</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.90.31</td><td>Länge 1200 mm</td></tr></table></main>",
        "https://example.test/mplus/rinnenkoerper/": "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1><table><tr><th>Artikel-Nr.</th><th>L1</th></tr><tr><td>9010.90.12</td><td>L1 1185 mm</td></tr></table></main>",
        "https://example.test/mplus/ablaufkoerper/": "<main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1><table><tr><th>Artikel-Nr.</th><th>DN</th></tr><tr><td>9010.90.22</td><td>DN 50</td></tr></table></main>",
        "https://example.test/mplus/design-roste/": "<main><h1>Design-Roste aus elektropoliertem Edelstahl</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.90.32</td><td>Länge 1200 mm</td></tr></table></main>",
    }

    def fake_get(url, timeout=35):
        canonical = mod._canonical_url(url)
        return 200, canonical, html_by_url[canonical], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(products, comparison, candidates_all, components, bom)

    assert diag.readiness.mplus_products_present == 1
    assert diag.readiness.mplus_components_present == 1
    assert diag.readiness.mplus_channel_body_candidates >= 1
    assert diag.readiness.mplus_drain_body_candidates >= 1
    assert diag.readiness.mplus_grate_candidates >= 1
    assert diag.readiness.likely_assembly_model == "channel_body_x_drain_body_x_grate"
    assert diag.readiness.safe_to_generate_mplus_assemblies is False
    assert diag.production_behavior_changed is False
