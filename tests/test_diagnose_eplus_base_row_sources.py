import tools.diagnose_eplus_base_row_sources as mod


BODY_HTML = """
<main>
  <h1>ACO ShowerDrain E+ Rinnenkörper Einbauhöhe Oberkante Estrich 57-128 mm</h1>
  <p>Abflussleistung 0,70 l/s. Sperrwasserhöhe 50 mm. Ablaufstutzen DN 50.</p>
  <table>
    <tr><th>Artikel-Nr.</th><th>Text</th></tr>
    <tr><td>9010.88.58</td><td>Montagewinkel Zubehör für ShowerDrain E+</td></tr>
    <tr><td>9010.79.23</td><td>ShowerDrain C Rinnenkörper</td></tr>
  </table>
</main>
"""

FAMILY_HTML = """
<main>
  <h1>ACO ShowerDrain E+</h1>
  <p>Navigation zu Rinnenkörpern, Design-Rosten und Zubehör.</p>
</main>
"""

GRATE_HTML = """
<main>
  <h1>Design-Roste aus elektropoliertem Edelstahl</h1>
  <p>Design-Roste für ACO ShowerDrain E+.</p>
</main>
"""


def test_eplus_known_rinnenkoerper_pages_are_page_level_body_evidence(monkeypatch):
    urls = [
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1/",
    ]

    def fake_get(url, timeout=35):
        return 200, url, BODY_HTML, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    diag = mod.build_diagnostic(source_urls=urls)

    assert len(diag.page_level_body_evidence) == 3
    assert [row.height_adj_min_mm for row in diag.page_level_body_evidence] == ["25", "57", "80"]
    assert all(row.flow_rate_lps == "0.70" for row in diag.page_level_body_evidence)
    assert all(row.water_seal_mm == "50" for row in diag.page_level_body_evidence)
    assert all(row.outlet_dn == "DN50" for row in diag.page_level_body_evidence)
    assert diag.article_level_body_candidates == ()
    assert all(row.article_number != "9010.88.58" for row in diag.article_level_body_candidates)
    assert diag.production_behavior_changed is False


def test_family_landing_excluded_and_design_roste_only_grate(monkeypatch):
    family = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/"
    grate = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/"

    def fake_get(url, timeout=35):
        html = GRATE_HTML if "design-roste" in url else FAMILY_HTML
        return 200, url, html, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    diag = mod.build_diagnostic(source_urls=[family, grate])

    assert diag.page_level_body_evidence == ()
    assert len(diag.grate_candidates) == 1
    assert diag.grate_candidates[0].source_url == grate
    assert len(diag.excluded_other_candidates) == 1
    assert diag.excluded_other_candidates[0].evidence_type == "family_landing_page_excluded"


def test_support_and_incomplete_rows_do_not_print_as_article_level_candidates(monkeypatch, capsys):
    body = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/"
    grate = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/"

    def fake_get(url, timeout=35):
        return 200, url, GRATE_HTML if "design-roste" in url else BODY_HTML, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    diag = mod.build_diagnostic(source_urls=[body, body, grate])
    mod.print_report(diag)
    out = capsys.readouterr().out

    assert len(diag.page_level_body_evidence) == 1
    assert diag.article_level_body_candidates == ()
    assert [row.article_number for row in diag.excluded_support_or_mounting_part] == ["9010.88.58"]
    assert "Article-level body candidates: 0" in out
    candidate_section = out.split("Article-level body candidates: 0", 1)[1].split("Article-level body incomplete", 1)[0]
    assert "9010.88.58" not in candidate_section
    assert diag.readiness.eplus_page_level_body_rows == 1
    assert diag.readiness.eplus_article_level_body_candidates == 0
    assert diag.readiness.eplus_article_level_body_incomplete == 0
    assert diag.readiness.eplus_grate_candidates == 1
    assert diag.readiness.hydraulic_complete_candidates == 0
    assert diag.readiness.likely_assembly_model == "base_x_grate"
    assert diag.readiness.safe_to_generate_eplus_assemblies is False
