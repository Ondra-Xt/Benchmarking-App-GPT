from __future__ import annotations

import pandas as pd

from src.canonical_aco_export import CanonicalAcoFrames
from tools import report_tece_source_inventory as report_mod


def _sample_candidates():
    return [
        {
            "manufacturer": "tece",
            "product_id": "tece-600100",
            "product_family": "TECEdrain",
            "product_name": "TECEdrainline 1200 mm 600100",
            "product_url": "https://produktdaten.tece.de/web/tece_DE/de_DE/tece/PR/600100/index.xhtml",
            "sources": "https://produktdaten.tece.de/web/tece_DE/de_DE/tece/PR/600100/index.xhtml",
            "length_delta_mm": 0,
        }
    ]


def _sample_params(_url):
    return {
        "flow_rate_lps": 0.8,
        "outlet_dn": "DN50",
        "height_adj_min_mm": 95,
        "height_adj_max_mm": 150,
        "water_seal_mm": None,
        "evidence": [
            ("HTML fetch", "status=200", "https://produktdaten.tece.de/web/tece_DE/de_DE/tece/PR/600100/index.xhtml"),
            ("PDF fallback", "https://documents.pdod.de/tece/tcdb_600100.pdf", "https://produktdaten.tece.de/web/tece_DE/de_DE/tece/PR/600100/index.xhtml"),
            ("PDF status", "ok", "https://documents.pdod.de/tece/tcdb_600100.pdf"),
        ],
    }


def test_tece_inventory_report_is_diagnostic_only_and_blocks_production(monkeypatch):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: (_sample_candidates(), [{"method": "fixture"}]))
    monkeypatch.setattr(report_mod.tece, "extract_parameters", _sample_params)

    report = report_mod.build_report()

    assert report.candidate_count == 1
    assert report.article_numbers == ["600100"]
    assert report.technical_field_coverage["flow_rate_lps"] == 1
    assert report.technical_field_coverage["water_seal_mm"] == 0
    assert report.evidence_source_counts["HTML"] == 1
    assert report.evidence_source_counts["guessed tcdb PDF"] == 1
    assert report.production_promotion_blocked is True
    assert report.article_level_compatibility_evidence_exists is False
    row = report.rows[0]
    assert row.ready_for_benchmark is False
    assert row.ready_for_customer_view is False
    assert row.production_promotion_blocked is True
    assert "Products/Comparison" in row.production_status_note
    assert "water_seal_mm" in row.missing_fields


def test_tece_inventory_does_not_mutate_aco_baseline(monkeypatch):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: (_sample_candidates(), []))
    monkeypatch.setattr(report_mod.tece, "extract_parameters", _sample_params)

    products = pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"], "ready_for_benchmark": [True]})
    comparison = pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"]})
    baseline = CanonicalAcoFrames(
        registry=pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"]}),
        products=products.copy(deep=True),
        comparison=comparison.copy(deep=True),
        excluded=pd.DataFrame(),
        evidence=pd.DataFrame(),
        bom_options=pd.DataFrame(),
    )

    before_products = baseline.products.copy(deep=True)
    before_comparison = baseline.comparison.copy(deep=True)
    report = report_mod.build_report()

    pd.testing.assert_frame_equal(baseline.products, before_products)
    pd.testing.assert_frame_equal(baseline.comparison, before_comparison)
    assert all(not row.product_id.startswith("aco-") for row in report.rows)
    assert all(row.ready_for_benchmark is False and row.ready_for_customer_view is False for row in report.rows)


def test_tece_inventory_cli_json(monkeypatch, capsys):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: (_sample_candidates(), []))
    monkeypatch.setattr(report_mod.tece, "extract_parameters", _sample_params)

    assert report_mod.main(["--json"]) == 0
    out = capsys.readouterr().out
    assert '"candidate_count": 1' in out
    assert '"production_promotion_blocked": true' in out
    assert '"ready_for_benchmark": false' in out
