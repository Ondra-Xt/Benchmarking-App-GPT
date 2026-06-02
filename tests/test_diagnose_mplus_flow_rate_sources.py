import pandas as pd

import tools.diagnose_mplus_flow_rate_sources as mod
from tools.report_mplus_compound_assembly_mapping import ProposedCompoundMappingRow


def _mapping(article, flow=""):
    digits = article.replace(".", "")
    return ProposedCompoundMappingRow(
        channel_body_id="channel-body-25-128",
        channel_body_article_number="",
        drain_body_id=f"aco-{digits}",
        drain_body_article_number=article,
        grate_id="mplus-design-roste-elektropoliert",
        grate_article_number="",
        proposed_compound_set_id=f"diagnostic-mplus-channel-body-25-128__aco-{digits}__mplus-design-roste-elektropoliert",
        proposed_product_name=f"Diagnostic {article}",
        product_family="showerdrain_mplus",
        assembly_model="channel_body_x_drain_body_x_grate",
        source_url_channel_body="https://example.test/mplus/channel/",
        source_url_drain_body="https://example.test/mplus/drain/",
        source_url_grate="https://example.test/mplus/grate/",
        water_seal_mm="50",
        outlet_dn="DN50",
        flow_rate_lps=flow,
        height_adj_min_mm="25",
        height_adj_max_mm="128",
        missing_technical_fields=() if flow else ("flow_rate_lps",),
        mapping_confidence="blocked_source_backed_parts_incomplete_technical_fields",
        blocking_reason="missing source-backed complete hydraulic fields for compound generation",
        safe_to_generate=False,
    )


class FakeMappingReport:
    sheet_counts = {
        "Products": 46,
        "Comparison": 46,
        "Candidates_All": 118,
        "Components": 100,
        "BOM_Options": 221,
        "Final_Assemblies": 28,
        "Final_Set_Details": 28,
    }
    proposed_mappings = tuple(_mapping(article) for article in mod.TARGET_MPLUS_ARTICLES)
    generic_drain_body_pages = ()


def _patch_build_report(monkeypatch):
    monkeypatch.setattr(mod.mplus_mapping, "build_report", lambda *args, **kwargs: FakeMappingReport())


def test_inspect_source_url_finds_flow_terms_article_rows_and_downloads(monkeypatch):
    html = """
        <main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1>
        <table><tr><th>Artikel-Nr.</th><th>Ablaufleistung</th><th>DN</th></tr>
        <tr><td>9010.81.20</td><td>0,8 l/s nach DIN EN 1253</td><td>DN 50</td></tr></table>
        <a href="/media/aco-showerdrain-mplus-hydraulik.pdf">Technische Information M+ Hydraulik</a>
        </main>
    """
    monkeypatch.setattr(mod.aco, "_safe_get_text", lambda url, timeout=35: (200, url, html, ""))

    final_url, snippets, links, flat_text = mod.inspect_source_url("https://example.test/mplus/drain/")

    assert final_url == "https://example.test/mplus/drain/"
    assert any(snippet.evidence_type == "explicit_article_table" for snippet in snippets)
    assert any(snippet.flow_values == ("0.8",) for snippet in snippets)
    assert any("9010.81.20" in snippet.article_numbers for snippet in snippets)
    assert links[0].href == "https://example.test/media/aco-showerdrain-mplus-hydraulik.pdf"
    assert "9010.81.20" in flat_text


def test_build_diagnostic_keeps_generic_family_flow_ambiguous_and_blocked(monkeypatch):
    _patch_build_report(monkeypatch)
    html_by_url = {
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/":
            "<main><p>ACO ShowerDrain M+ Ablaufleistung 0,8 l/s nach DIN EN 1253.</p></main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/":
            "<main><table><tr><th>Artikel-Nr.</th></tr><tr><td>9010.81.20</td></tr><tr><td>9010.81.21</td></tr></table></main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/":
            "<main><table><tr><th>Artikel-Nr.</th><th>DN</th></tr><tr><td>9010.81.20</td><td>DN50</td></tr><tr><td>9010.81.21</td><td>DN50</td></tr><tr><td>9010.81.22</td><td>DN50</td></tr><tr><td>9010.81.23</td><td>DN50</td></tr></table></main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/":
            "<main><p>Design-Roste ACO ShowerDrain M+</p></main>",
        "https://example.test/mplus/channel/": "<main>channel</main>",
        "https://example.test/mplus/drain/": "<main>9010.81.20 9010.81.21 9010.81.22 9010.81.23</main>",
        "https://example.test/mplus/grate/": "<main>grate</main>",
    }

    def fake_get(url, timeout=35):
        canonical = mod.mplus_sources._canonical_url(url.replace("/produktee/", "/produkte/"))
        return 200, canonical, html_by_url[canonical], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    assert diag.safe_to_fill_mplus_flow_rate_lps is False
    assert all(row.evidence_type == "ambiguous" for row in diag.per_article_evidence)
    assert all(row.confidence == "low" for row in diag.per_article_evidence)
    assert all(row.discovered_flow_rate_lps_candidates == ("0.8",) for row in diag.per_article_evidence)
    assert diag.risk_checks.family_level_value_treated_as_article_level == ()
    assert diag.production_behavior_changed is False


def test_build_diagnostic_allows_safe_fill_only_for_explicit_article_flow(monkeypatch):
    _patch_build_report(monkeypatch)
    rows = "".join(
        f"<tr><td>{article}</td><td>0,8 l/s</td><td>DN50</td></tr>"
        for article in mod.TARGET_MPLUS_ARTICLES
    )
    html_by_url = {
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/": "<main>M+</main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/": "<main>channel</main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/": f"<main><table><tr><th>Artikel-Nr.</th><th>Ablaufleistung</th><th>DN</th></tr>{rows}</table></main>",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/": "<main>grate</main>",
        "https://example.test/mplus/channel/": "<main>channel</main>",
        "https://example.test/mplus/drain/": "<main>drain</main>",
        "https://example.test/mplus/grate/": "<main>grate</main>",
    }

    def fake_get(url, timeout=35):
        canonical = mod.mplus_sources._canonical_url(url)
        return 200, canonical, html_by_url[canonical], ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    assert diag.safe_to_fill_mplus_flow_rate_lps is True
    assert all(row.evidence_type == "explicit_article_table" for row in diag.per_article_evidence)
    assert all(row.confidence == "high" for row in diag.per_article_evidence)
    assert all(row.source_url for row in diag.per_article_evidence)
    assert all(row.evidence_snippet for row in diag.per_article_evidence)


def test_print_report_includes_required_sections(capsys):
    diag = mod.MPlusFlowRateDiagnostic(
        sheet_counts=FakeMappingReport.sheet_counts,
        target_articles=mod.TARGET_MPLUS_ARTICLES,
        source_urls_inspected=("https://example.test/mplus/drain/",),
        flow_snippets=(mod.FlowSnippet("https://example.test/mplus/drain/", "explicit_article_table", "9010.81.20 Ablaufleistung 0,8 l/s", ("0.8",), ("9010.81.20",)),),
        per_article_evidence=(
            mod.ArticleFlowEvidence("9010.81.20", "aco-90108120", "50", "DN50", "", ("0.8",), "https://example.test/mplus/drain/", "9010.81.20 Ablaufleistung 0,8 l/s", "explicit_article_table", "high", "review", True),
        ),
        download_links=(),
        safe_to_fill_mplus_flow_rate_lps=False,
        blocking_reason="keep blocked",
        recommended_next_action="parse PDFs",
        risk_checks=mod.FlowRiskChecks(),
    )

    mod.print_report(diag)
    output = capsys.readouterr().out

    assert "Input sheet/frame counts" in output
    assert "Target M+ articles" in output
    assert "Source URLs inspected" in output
    assert "Flow/discharge snippets found" in output
    assert "Per-article flow evidence table" in output
    assert "PDF/download links related to M+ hydraulics" in output
    assert "safe_to_fill_mplus_flow_rate_lps: False" in output
    assert "Recommended" not in output  # exact output uses snake-case recommended_next_action
    assert "recommended_next_action" in output
    assert "production behavior changed: no" in output
