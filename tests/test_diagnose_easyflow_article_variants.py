import pandas as pd

import tools.diagnose_easyflow_article_variants as mod


HTML = """
<html><body><main>
  <h1>ACO Easyflow Komplettabläufe ACO Easyflow DN 50</h1>
  <table>
    <thead><tr>
      <th>Artikel-Nr.</th><th>Bezeichnung</th><th>Geruchsverschluss</th><th>DN</th>
      <th>Ablaufleistung</th><th>Bauhöhe</th><th>Abmessung</th>
    </tr></thead>
    <tbody>
      <tr><td>1234.56.70</td><td>Standard WS50</td><td>50 mm</td><td>DN50</td><td>1,5 l/s</td><td>15-96 mm</td><td>150 x 150 mm</td></tr>
      <tr><td>1234.56.71</td><td>Flatline WS50</td><td>50 mm</td><td>DN50</td><td>1,0 l/s</td><td>7-75 mm</td><td>Aussparung 160 x 160 mm</td></tr>
      <tr><td>1234.56.72</td><td>Standard WS30</td><td>30 mm</td><td>DN50</td><td>0,8 l/s</td><td>7-75 mm</td><td>Recess 160 x 160 mm</td></tr>
    </tbody>
  </table>
</main></body></html>
"""


def test_parse_article_variant_candidates_extracts_article_level_facts():
    candidates = mod.parse_article_variant_candidates(HTML, "https://example.test/aco-easyflow-komplettablaeufe-aco-easyflow-dn-50/?x=1")

    assert len(candidates) == 3
    first = candidates[0]
    assert first.article_number == "1234.56.70"
    assert first.article_digits == "12345670"
    assert first.source_url == "https://example.test/aco-easyflow-komplettablaeufe-aco-easyflow-dn-50/"
    assert first.water_seal_mm == 50
    assert first.outlet_dn == "DN50"
    assert first.flow_rate_lps == 1.5
    assert first.height_adj_min_mm == 15
    assert first.height_adj_max_mm == 96
    assert "Standard" in first.variant_markers
    assert "WS50" in first.variant_markers
    assert "150 x 150 mm" in first.dimensions_recess_cutout_descriptors


def test_classify_attribution_blocks_multiple_ws50_dn50_candidates():
    candidates = mod.parse_article_variant_candidates(HTML, "https://example.test/easyflow/")

    result = mod.classify_attribution(candidates)

    assert result.status == "multiple_candidate_articles"
    assert result.unique_match_exists is False
    assert [c.article_number for c in result.matching_candidates] == ["1234.56.70", "1234.56.71"]
    assert "Multiple Easyflow WS50/DN50 article rows" in result.blocking_reason
    assert "Do not infer one article" in result.blocking_reason


def test_classify_attribution_allows_single_exact_article_match():
    candidates = [
        mod.ArticleVariantCandidate("1234.56.70", "https://example.test/easyflow/", "Standard WS50 DN50", 50, "DN50", 1.5, 15, 96, "", ("Standard", "WS50")),
        mod.ArticleVariantCandidate("1234.56.72", "https://example.test/easyflow/", "Standard WS30 DN50", 30, "DN50", 0.8, 7, 75, "", ("Standard", "WS30")),
    ]

    result = mod.classify_attribution(candidates)

    assert result.status == "exact_article_match"
    assert result.unique_match_exists is True
    assert [c.article_number for c in result.matching_candidates] == ["1234.56.70"]


def test_collect_easyflow_source_urls_excludes_easyflowplus(monkeypatch):
    registry = pd.DataFrame([
        {
            "product_id": "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50",
            "product_family": "easyflow",
            "product_name": "ACO Easyflow DN50",
            "product_url": "https://example.test/aco-easyflow-dn50/",
            "sources": "https://example.test/aco-easyflow-table/?x=1",
        },
        {
            "product_id": "aco-easyflowplus-komplettablauf",
            "product_family": "easyflowplus",
            "product_name": "ACO Easyflow+ DN50",
            "product_url": "https://example.test/aco-easyflow-plus-dn50/",
            "sources": "https://example.test/aco-easyflowplus-table/",
        },
    ])
    debug = [
        {"final_url": "https://example.test/aco-easyflow-debug/#x", "seed_url": "", "error": ""},
        {"final_url": "https://example.test/aco-easyflow-plus-debug/", "seed_url": "", "error": ""},
    ]

    urls = mod.collect_easyflow_source_urls(registry, debug, [pd.Series({"product_url": "https://example.test/aco-easyflow-base/#a"})])

    assert urls == [
        "https://example.test/aco-easyflow-base/",
        "https://example.test/aco-easyflow-dn50/",
        "https://example.test/aco-easyflow-table/",
        "https://example.test/aco-easyflow-debug/",
    ]
    assert not any("plus" in url for url in urls)


def test_build_report_runs_discovery_pipeline_and_fetches_diagnostic_candidates(monkeypatch):
    registry_rows = [
        {
            "manufacturer": "aco",
            "product_id": mod.BASE_ID,
            "product_family": "easyflow",
            "product_name": "ACO Easyflow DN50",
            "product_url": "https://example.test/aco-easyflow-dn50/",
            "sources": "https://example.test/aco-easyflow-dn50/",
        }
    ]
    products = pd.DataFrame([
        {"product_id": mod.BASE_ID, "product_family": "easyflow", "water_seal_mm": 50, "outlet_dn": "DN50", "flow_rate_lps": "", "height_adj_min_mm": "", "height_adj_max_mm": "", "product_url": "https://example.test/aco-easyflow-dn50/"},
        {"product_id": mod.ASSEMBLED_IDS[0], "water_seal_mm": 50, "outlet_dn": "DN50"},
        {"product_id": mod.ASSEMBLED_IDS[1], "water_seal_mm": 50, "outlet_dn": "DN50"},
    ])
    evidence = pd.DataFrame([{"product_id": mod.BASE_ID, "field": "water_seal_mm", "extracted_value": "50"}])

    monkeypatch.setattr(mod.aco, "discover_candidates", lambda target_length_mm, tolerance_mm: (registry_rows, []))
    monkeypatch.setattr(mod.pipeline, "run_update", lambda registry, cfg: (products, products.copy(), pd.DataFrame(), evidence, pd.DataFrame()))
    monkeypatch.setattr(mod.aco, "_safe_get_text", lambda url, timeout=35: (200, url, HTML, ""))

    report = mod.build_diagnostic_report()

    assert report.counts["Products"] == 3
    assert report.base_location == "Products"
    assert report.assembled_locations == {mod.ASSEMBLED_IDS[0]: "Products", mod.ASSEMBLED_IDS[1]: "Products"}
    assert report.evidence_rows == [{"product_id": mod.BASE_ID, "field": "water_seal_mm", "extracted_value": "50"}]
    assert report.attribution.status == "multiple_candidate_articles"
    assert len(report.candidates) == 3
