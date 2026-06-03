import pandas as pd

import tools.diagnose_eplus_base_row_sources as mod


FAMILY_URL = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/"
DESIGN_URL = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/"


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


def test_family_landing_body_text_is_not_classified_as_eplus_grate(monkeypatch):
    html = """
        <main><h1>ACO ShowerDrain E+</h1>
        <p>ACO ShowerDrain E+ Rinnenkörper, Einbauhöhe Oberkante Estrich 57-128 mm,
        Sperrwasserhöhe 50 mm, Ablaufstutzen DN 50.</p></main>
    """

    def fake_get(url, timeout=35):
        return 200, FAMILY_URL, html, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, rows = mod.parse_eplus_source_url(FAMILY_URL)

    assert len(rows) == 1
    assert rows[0].candidate_type == "eplus_page_level_body_row"
    assert rows[0].system_role == "drain_unit"
    assert rows[0].candidate_type != "eplus_grate_component"


def test_design_roste_page_remains_eplus_grate_component(monkeypatch):
    html = """
        <main><h1>Design-Roste aus elektropoliertem Edelstahl</h1>
        <table><tr><th>Artikel-Nr.</th><th>Länge</th><th>Beschreibung</th></tr>
        <tr><td>9010.80.31</td><td>Länge 1200 mm</td><td>Rost passend für ACO ShowerDrain E+</td></tr></table></main>
    """

    def fake_get(url, timeout=35):
        return 200, DESIGN_URL, html, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, rows = mod.parse_eplus_source_url(DESIGN_URL)

    assert len(rows) == 1
    assert rows[0].candidate_type == "eplus_grate_component"
    assert rows[0].system_role == "grate"
    assert rows[0].length_mm == "1200"


def test_support_and_cross_family_rows_are_excluded_from_eplus_body_candidates(monkeypatch):
    support_url = "https://example.test/eplus/montagewinkel/"
    c_only_url = "https://example.test/aco-showerdrain-c/rinnenkoerper/"
    html_by_url = {
        support_url: "<main><h1>Montagewinkel ACO ShowerDrain E+</h1><table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.80.41</td><td>Montagewinkel Zubehör passend für ShowerDrain E+</td></tr></table></main>",
        c_only_url: "<main><h1>ACO ShowerDrain C Rinnenkörper</h1><table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.85.20</td><td>Rinnenkörper ACO ShowerDrain C Einbauhöhe 80-128 mm</td></tr></table></main>",
    }

    def fake_get(url, timeout=35):
        return 200, url, html_by_url[url], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, support = mod.parse_eplus_source_url(support_url)
    _url, c_only = mod.parse_eplus_source_url(c_only_url)

    assert support[0].candidate_type == "eplus_excluded_support_accessory"
    assert c_only[0].candidate_type == "excluded_other"


def test_build_diagnostic_cleaned_counts_keep_eplus_blocked(monkeypatch):
    extra_body_url = "https://example.test/eplus/page-level-body/"
    products = pd.DataFrame([
        {"product_id": "aco-existing", "product_family": "showerdrain_eplus", "product_url": FAMILY_URL},
        {"product_id": "aco-body-extra", "product_family": "showerdrain_eplus", "product_url": extra_body_url},
    ])
    comparison = pd.DataFrame()
    candidates_all = pd.DataFrame()
    components = pd.DataFrame([
        {"product_id": "aco-grate", "product_family": "showerdrain_eplus", "system_role": "grate", "product_url": DESIGN_URL},
    ])
    bom = pd.DataFrame()
    body_text = "ACO ShowerDrain E+ Rinnenkörper, Einbauhöhe Oberkante Estrich 57-128 mm, Sperrwasserhöhe 50 mm, DN 50."
    html_by_url = {
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[0]): f"<main><h1>ACO ShowerDrain E+</h1>{body_text}</main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[1]): f"<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 57-128 mm</h1>{body_text}</main>",
        extra_body_url: f"<main><h1>ACO ShowerDrain E+ Ablaufdaten</h1>{body_text}</main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[2]): "<main><h1>Design-Roste aus elektropoliertem Edelstahl</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.80.31</td><td>Länge 1200 mm</td></tr></table></main>",
        "https://example.test/eplus/montagewinkel/": "<main><h1>Montagewinkel ACO ShowerDrain E+</h1><table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.80.41</td><td>Montagewinkel passend für ShowerDrain E+</td></tr></table></main>",
    }

    def fake_get(url, timeout=35):
        canonical = mod._canonical_url(url)
        return 200, canonical, html_by_url[canonical], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    products = pd.concat([
        products,
        pd.DataFrame([{"product_id": "aco-support", "product_family": "showerdrain_eplus", "product_url": "https://example.test/eplus/montagewinkel/"}]),
    ], ignore_index=True)

    diag = mod.build_diagnostic(products, comparison, candidates_all, components, bom)

    assert diag.readiness.eplus_page_level_body_rows == 3
    assert diag.readiness.eplus_grate_candidates == 1
    assert diag.readiness.eplus_article_level_body_candidates == 0
    assert diag.readiness.hydraulic_complete_candidates == 0
    assert diag.readiness.likely_assembly_model == "base_x_grate"
    assert diag.readiness.safe_to_generate_eplus_assemblies is False
    assert diag.production_behavior_changed is False
