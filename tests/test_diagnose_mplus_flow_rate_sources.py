import tools.diagnose_mplus_flow_rate_sources as mod


HTML = """
<main>
  <h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1>
  <p>Abflussleistung: 0,4 l/s mit 10 mm Aufstau</p>
  <p>0,46 l/s mit 20 mm Aufstau</p>
  <table>
    <tr><th>Artikel-Nr.</th><th>Beschreibung</th></tr>
    <tr><td>9010.81.20</td><td>Ablaufkörper waagerecht DN 50</td></tr>
    <tr><td>9010.81.21</td><td>Ablaufkörper senkrecht DN 50</td></tr>
    <tr><td>9010.81.22</td><td>Ablaufkörper waagerecht DN 40</td></tr>
    <tr><td>9010.81.23</td><td>Ablaufkörper senkrecht DN 40</td></tr>
  </table>
  <p>Zubehör: Reduziert die Abflussleistung um 0,1 l/s</p>
</main>
"""


def test_inspect_source_html_separates_drain_flow_from_accessory_reduction():
    family, reductions, article_table, flat_text = mod.inspect_source_html(HTML, "https://example.test/mplus/drain/")

    assert [(row.flow_rate_lps, row.head_mm) for row in family] == [("0.4", "10"), ("0.46", "20")]
    assert [row.evidence_type for row in family] == ["explicit_drain_body_family_level", "explicit_drain_body_family_level"]
    assert all(row.confidence == "medium" for row in family)
    assert all(row.article_specific is False for row in family)
    assert [row.accessory_flow_reduction_lps for row in reductions] == ["0.1"]
    assert article_table == ()
    assert "9010.81.20" in flat_text
    assert "0.1" not in {row.flow_rate_lps for row in family}


def test_build_diagnostic_keeps_target_articles_conservative_and_blocked(monkeypatch):
    def fake_get(url, timeout=35):
        return 200, "https://example.test/mplus/drain/", HTML, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    diag = mod.build_diagnostic(["https://example.test/mplus/drain/"])

    assert [(row.flow_rate_lps, row.head_mm) for row in diag.drain_body_flow_candidates] == [("0.4", "10"), ("0.46", "20")]
    assert [row.accessory_flow_reduction_lps for row in diag.accessory_flow_reduction_lps] == ["0.1"]
    assert diag.safe_to_fill_mplus_flow_rate_lps is False
    assert diag.production_behavior_changed is False
    assert diag.risk_checks.accessory_reduction_treated_as_flow_candidate == ()
    assert diag.risk_checks.multiple_head_condition_flow_values_found is True
    assert diag.risk_checks.article_level_flow_table_values_found is False
    assert len(diag.per_article) == 4
    assert all(row.article_found_in_source_text is True for row in diag.per_article)
    assert all(row.evidence_type == "drain_body_family_level" for row in diag.per_article)
    assert all(row.evidence_type != "explicit_article_table" for row in diag.per_article)
    assert all(row.flow_rate_lps_10mm_head == "0.4" for row in diag.per_article)
    assert all(row.flow_rate_lps_20mm_head == "0.46" for row in diag.per_article)
    assert all(row.accessory_flow_reduction_lps == ("0.1",) for row in diag.per_article)
    assert all(row.article_specific is False for row in diag.per_article)
    assert all(row.safe_to_fill_mplus_flow_rate_lps is False for row in diag.per_article)


def test_same_article_table_row_is_required_for_explicit_article_table():
    html = """
    <main>
      <p>Abflussleistung: 0,4 l/s mit 10 mm Aufstau</p>
      <table>
        <tr><th>Artikel-Nr.</th><th>Text</th></tr>
        <tr><td>9010.81.20</td><td>0,46 l/s mit 20 mm Aufstau Ablaufleistung</td></tr>
        <tr><td>9010.81.21</td><td>Ablaufkörper ohne Flusswert</td></tr>
      </table>
    </main>
    """

    family, reductions, article_table, _flat_text = mod.inspect_source_html(
        html,
        "https://example.test/mplus/drain/",
        target_articles=("9010.81.20", "9010.81.21"),
    )

    assert reductions == ()
    assert [(row.article_number, row.flow_rate_lps, row.evidence_type) for row in article_table] == [
        ("9010.81.20", "0.46", "explicit_article_table")
    ]
    assert all(row.article_number != "9010.81.21" for row in article_table)
    assert [(row.flow_rate_lps, row.head_mm) for row in family] == [("0.4", "10"), ("0.46", "20")]
