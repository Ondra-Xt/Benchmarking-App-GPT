from __future__ import annotations

import pandas as pd

import tools.diagnose_eplus_base_row_sources as mod


def test_parse_eplus_source_url_classifies_clean_groups_and_exclusions(monkeypatch):
    html_by_url = {
        "https://example.test/eplus/rinnenkoerper/": """
            <main><h1>ACO ShowerDrain E+ Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm</h1>
            <table><tr><th>Artikel-Nr.</th><th>L1</th><th>Passend für</th><th>Beschreibung</th></tr>
            <tr><td>9010.77.11</td><td>L1 1185 mm</td><td>Passend für ACO ShowerDrain E+</td><td>Rinnenkörper ohne hydraulische Felder</td></tr>
            <tr><td>9010.79.23</td><td>L1 1185 mm</td><td>Passend für Duschrinne ACO ShowerDrain C</td><td>Abdeckung</td></tr>
            <tr><td>9010.79.24</td><td>L1 1185 mm</td><td>Passend für Duschrinne ACO ShowerDrain C</td><td>Abdeckung</td></tr>
            <tr><td>9010.88.58</td><td></td><td>Passend für Duschrinne ACO ShowerDrain C</td><td>Montagewinkel</td></tr>
            <tr><td>9010.88.58</td><td></td><td>Passend für Duschrinne ACO ShowerDrain C</td><td>Montagewinkel</td></tr>
            </table></main>
        """,
        "https://example.test/eplus/design-roste/": """
            <main><h1>Design-Roste aus elektropoliertem Edelstahl ACO ShowerDrain E+</h1>
            <table><tr><th>Artikel-Nr.</th><th>Länge</th><th>Beschreibung</th></tr>
            <tr><td>9010.80.31</td><td>Länge 1200 mm</td><td>Rost passend für ACO ShowerDrain E+</td></tr></table></main>
        """,
    }

    def fake_get(url, timeout=35):
        final = mod._canonical_url(url)
        return 200, final, html_by_url[final], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _url, body_rows = mod.parse_eplus_source_url("https://example.test/eplus/rinnenkoerper/")
    _url, grate_rows = mod.parse_eplus_source_url("https://example.test/eplus/design-roste/")

    by_article = {row.article_number: row for row in body_rows}
    assert any(row.candidate_type == "eplus_page_level_body_evidence" for row in body_rows)
    assert by_article["9010.77.11"].candidate_type == "eplus_article_level_body_incomplete"
    assert by_article["9010.79.23"].candidate_type == "excluded_cross_family"
    assert by_article["9010.79.24"].candidate_type == "excluded_cross_family"
    assert [row.article_number for row in body_rows if row.article_number == "9010.88.58"] == ["9010.88.58"]
    assert by_article["9010.88.58"].candidate_type == "excluded_support_or_mounting_part"
    assert grate_rows[0].candidate_type == "eplus_grate_component"


def test_build_diagnostic_keeps_eplus_blocked_and_page_level_ids_visible(monkeypatch):
    products = pd.DataFrame()
    comparison = pd.DataFrame()
    candidates_all = pd.DataFrame([
        {"product_id": "aco-eplus-candidate", "product_family": "showerdrain_eplus", "product_url": "https://example.test/eplus/rinnenkoerper/"},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-eplus-grate", "product_family": "showerdrain_eplus", "system_role": "grate", "product_url": "https://example.test/eplus/design-roste/"},
    ])
    bom = pd.DataFrame()
    html_by_url = {
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[0]): "<main><h1>ACO ShowerDrain E+</h1></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[1]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm ACO ShowerDrain E+</h1></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[2]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 57-128 mm ACO ShowerDrain E+</h1></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[3]): "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 80-128 mm DIN EN 1253-1 ACO ShowerDrain E+</h1></main>",
        mod._canonical_url(mod.KNOWN_EPLUS_SOURCE_URLS[4]): "<main><h1>Design-Roste aus elektropoliertem Edelstahl ACO ShowerDrain E+</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.80.31</td><td>Länge 1200 mm</td></tr></table></main>",
        "https://example.test/eplus/rinnenkoerper/": "<main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 25-128 mm ACO ShowerDrain E+</h1><table><tr><th>Artikel-Nr.</th><th>Text</th></tr><tr><td>9010.77.11</td><td>Passend für ACO ShowerDrain E+ ohne Hydraulikfelder</td></tr><tr><td>9010.79.23</td><td>Passend für Duschrinne ACO ShowerDrain C</td></tr><tr><td>9010.88.58</td><td>Montagewinkel passend für ACO ShowerDrain E+</td></tr></table></main>",
        "https://example.test/eplus/design-roste/": "<main><h1>Design-Roste aus elektropoliertem Edelstahl ACO ShowerDrain E+</h1><table><tr><th>Artikel-Nr.</th><th>Länge</th></tr><tr><td>9010.80.32</td><td>Länge 1200 mm</td></tr></table></main>",
    }

    def fake_get(url, timeout=35):
        final = mod._canonical_url(url)
        return 200, final, html_by_url[final], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(products, comparison, candidates_all, components, bom)
    page_ids = {row.proposed_product_id for row in diag.page_level_body_evidence}

    assert {
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
    }.issubset(page_ids)
    assert {row.article_number for row in diag.excluded_cross_family} == {"9010.79.23"}
    assert {row.article_number for row in diag.excluded_support_or_mounting_part} == {"9010.88.58"}
    assert diag.readiness.hydraulic_complete_candidates == 0
    assert diag.readiness.likely_assembly_model == "base_x_grate"
    assert diag.readiness.safe_to_generate_eplus_assemblies is False
    assert diag.readiness.blocking_reason == "E+ source-backed page-level body/grate evidence exists, but article-level hydraulic fields are incomplete and cross-family/support rows are excluded."
    assert diag.production_behavior_changed is False
