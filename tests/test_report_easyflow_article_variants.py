from __future__ import annotations

import io

import pandas as pd

from tools import report_easyflow_article_variants as mod

BODY_HTML = """
<html><body><main><h1>Komplettabläufe ACO Easyflow DN 50</h1>
<table>
<thead><tr><th>Artikel-Nr.</th><th>Bezeichnung</th><th>Geruchsverschluss</th><th>DN</th><th>Ablaufleistung</th><th>Bauhöhe</th><th>Abmessung</th></tr></thead>
<tbody>
<tr><td>1234.56.70</td><td>Standard WS50 ohne Seitenzulauf</td><td>50 mm</td><td>DN50</td><td>1,5 l/s</td><td>15-96 mm</td><td>Aussparung 150 x 150 mm</td></tr>
<tr><td>1234.56.71</td><td>Flatline WS50 mit Seitenzulauf</td><td>50 mm</td><td>DN50</td><td>1,0 l/s</td><td>7-75 mm</td><td>Recess 160 x 160 mm</td></tr>
<tr><td>1234.56.72</td><td>Standard WS30</td><td>30 mm</td><td>DN50</td><td>0,8 l/s</td><td>7-75 mm</td><td>160 x 160 mm</td></tr>
</tbody>
</table>
</main></body></html>
"""

GRATE_HTML = """
<html><body><h1>ACO Easyflow Design-Roste</h1>
<table><tr><th>Artikel</th><th>Bezeichnung</th></tr>
<tr><td>8765.43.21</td><td>Design-Rost 150 x 150 mm</td></tr></table>
</body></html>
"""

ACCESSORY_HTML = """
<html><body><h1>ACO Easyflow Aufsatzstuecke</h1>
<table><tr><th>Artikel</th><th>Bezeichnung</th></tr>
<tr><td>8765.43.22</td><td>Aufsatzstueck fuer Designroste 150 x 150 mm</td></tr></table>
</body></html>
"""


def test_normalize_candidate_classifies_body_and_extracts_normalized_fields():
    candidate = mod.diag.parse_article_variant_candidates(
        BODY_HTML,
        "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/",
    )[0]

    row = mod.normalize_candidate(candidate)

    assert row.as_dict() == {
        "base_product_id": mod.BASE_PRODUCT_ID,
        "article_number": "1234.56.70",
        "source_url": "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/",
        "variant_type": "candidate_body_variant",
        "water_seal_mm": 50,
        "outlet_dn": "DN50",
        "flow_rate_lps": 1.5,
        "height_adj_min_mm": 15,
        "height_adj_max_mm": 96,
        "cutout_mm": "150 x 150 mm",
        "side_inlet": "false",
        "row_text": "1234.56.70 Standard WS50 ohne Seitenzulauf 50 mm DN50 1,5 l/s 15-96 mm Aussparung 150 x 150 mm",
        "attribution_status": "matches_current_ws50_dn50_base_facts",
    }


def test_classify_variant_row_separates_excluded_variant_types():
    assert mod.classify_variant_row("https://example.test/aco-easyflow/design-roste/", "Design-Rost") == "excluded_grate_variant"
    assert mod.classify_variant_row("https://example.test/aco-easyflow/aufsatzstuecke/", "Aufsatzstueck") == "excluded_accessory_variant"
    assert mod.classify_variant_row("https://example.test/aco-easyflow-plus/komplettablauf/", "Easyflow+") == "excluded_easyflowplus"


def test_build_variant_report_runs_discovery_pipeline_and_reports_matching_candidates(monkeypatch):
    registry_rows = [
        {
            "manufacturer": "aco",
            "product_id": mod.BASE_PRODUCT_ID,
            "product_family": "easyflow",
            "product_name": "ACO Easyflow DN50",
            "product_url": "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/",
            "sources": "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/design-roste/ https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/aufsatzstuecke/ https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus/komplettablauf/",
        }
    ]
    products = pd.DataFrame([
        {"product_id": mod.BASE_PRODUCT_ID, "product_family": "easyflow", "water_seal_mm": 50, "outlet_dn": "DN50", "flow_rate_lps": "", "height_adj_min_mm": "", "height_adj_max_mm": "", "product_url": registry_rows[0]["product_url"]},
    ])

    calls = {"discover": None, "run_update_registry_rows": None}

    def fake_discover(target_length_mm, tolerance_mm):
        calls["discover"] = (target_length_mm, tolerance_mm)
        return registry_rows, []

    def fake_run_update(registry, cfg):
        calls["run_update_registry_rows"] = len(registry)
        return products, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    monkeypatch.setattr(mod.aco, "discover_candidates", fake_discover)
    monkeypatch.setattr(mod.pipeline, "run_update", fake_run_update)

    pages = {
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/": BODY_HTML,
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow/einzelablaeufe-aco-easyflow-dn-50/": BODY_HTML,
        "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/": BODY_HTML,
        "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/design-roste/": GRATE_HTML,
        "https://example.test/produkte/badentwaesserung/badablaeufe/aco-easyflow/aufsatzstuecke/": ACCESSORY_HTML,
    }

    def fake_get(url, timeout=35):
        assert "easyflow-plus" not in url
        html = pages.get(mod.diag.canonical_url(url), "")
        return (200 if html else 404), mod.diag.canonical_url(url), html, ""

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    report = mod.build_variant_report()

    assert calls == {"discover": (1200, 100), "run_update_registry_rows": 1}
    assert report.matching_ws50_dn50_count == 6
    assert report.matching_article_numbers == ["1234.56.70", "1234.56.71", "1234.56.70", "1234.56.71", "1234.56.70", "1234.56.71"]
    assert report.distinct_flow_values == [1.0, 1.5]
    assert report.distinct_height_ranges == ["15-96", "7-75"]
    assert report.unique_attribution_possible is False
    assert report.attribution_status == "multiple_candidate_articles"
    assert report.counts["candidate_body_variant"] == 9
    assert report.counts["excluded_grate_variant"] == 1
    assert report.counts["excluded_accessory_variant"] == 1
    assert report.counts["excluded_easyflowplus"] == 0


def test_write_variant_csv_uses_required_columns():
    row = mod.NormalizedVariantRow(
        base_product_id=mod.BASE_PRODUCT_ID,
        article_number="1234.56.70",
        source_url="https://example.test/",
        variant_type="candidate_body_variant",
        water_seal_mm=50,
        outlet_dn="DN50",
        flow_rate_lps=1.5,
        height_adj_min_mm=15,
        height_adj_max_mm=96,
        cutout_mm="150 x 150 mm",
        side_inlet="false",
        row_text="row text",
        attribution_status="matches_current_ws50_dn50_base_facts",
    )
    stream = io.StringIO()

    mod.write_variant_csv([row], stream)

    lines = stream.getvalue().splitlines()
    assert lines[0] == ",".join(mod.VARIANT_COLUMNS)
    assert "candidate_body_variant" in lines[1]
