from __future__ import annotations

import json

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


def test_http_202_seed_responses_are_classified_blocked_async(monkeypatch):
    debug = [
        {
            "method": "produktdaten_seed",
            "seed_url": "https://produktdaten.tece.de/seed.xhtml",
            "status_code": 202,
            "final_url": "https://produktdaten.tece.de/seed.xhtml",
            "candidates_found": 0,
            "blocked_live_seed": True,
            "async_or_placeholder": True,
            "classification": "async_placeholder",
        },
        {"method": "final", "candidates_found": 0, "after_length_filter": 0, "sample_dropped_by_length": "[]", "sample_index_only_urls": "[]"},
    ]
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], debug))

    report = report_mod.build_report()

    assert report.candidate_count == 0
    assert report.blocked_live_seed_count == 1
    assert report.async_or_placeholder_seed_count == 1
    assert report.seed_status_summary[0]["classification"] == "async_placeholder"


def test_zero_candidates_with_blocked_seeds_has_clear_overall_status(monkeypatch, capsys):
    debug = [
        {"method": "produktdaten_seed", "seed_url": "seed-a", "status_code": 202, "final_url": "seed-a", "candidates_found": 0},
        {"method": "final", "candidates_found": 0, "after_length_filter": 0},
    ]
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], debug))

    report = report_mod.build_report()
    assert report.overall_status == "TECE_SOURCE_INVENTORY_BLOCKED_LIVE_SOURCE"
    assert "browser/session-capable" in report.recommended_next_action

    assert report_mod.main([]) == 0
    assert "OVERALL: TECE_SOURCE_INVENTORY_BLOCKED_LIVE_SOURCE" in capsys.readouterr().out


def test_tece_candidates_still_keep_production_promotion_blocked(monkeypatch):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: (_sample_candidates(), [{"method": "final", "candidates_found": 1, "after_length_filter": 1}]))
    monkeypatch.setattr(report_mod.tece, "extract_parameters", _sample_params)

    report = report_mod.build_report()

    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False
    assert all(row.production_promotion_blocked for row in report.rows)
    assert all(row.ready_for_benchmark is False for row in report.rows)
    assert all(row.ready_for_customer_view is False for row in report.rows)


def test_tece_source_pack_loader_reads_html_and_txt_fixtures():
    report = report_mod.load_source_pack("tests/fixtures/tece/source_pack")

    assert report.source_pack_file_count >= 2
    assert report.source_pack_candidate_count >= 2
    assert "600100" in report.article_numbers
    assert "650001" in report.article_numbers
    assert report.technical_field_coverage["nominal_length_mm"] >= 1
    assert report.technical_field_coverage["flow_rate_lps"] >= 1
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False
    assert report.article_level_compatibility_evidence_exists is False
    assert report.compatibility_evidence_status == "missing_article_level_compatibility_matrix"
    assert all(row.production_promotion_blocked for row in report.rows)
    assert all(row.ready_for_benchmark is False for row in report.rows)


def test_tece_report_supports_source_pack_and_stays_incomplete(monkeypatch, capsys):
    debug = [
        {"method": "produktdaten_seed", "seed_url": "seed-a", "status_code": 202, "final_url": "seed-a", "candidates_found": 0},
        {"method": "final", "candidates_found": 0, "after_length_filter": 0},
    ]
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], debug))

    report = report_mod.build_report(source_pack="tests/fixtures/tece/source_pack")

    assert report.candidate_count == 0
    assert report.source_pack is not None
    assert report.source_pack.source_pack_candidate_count >= 2
    assert report.source_pack.production_promotion_blocked is True
    assert report.overall_status == "TECE_SOURCE_PACK_INVENTORY_INCOMPLETE"
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False

    assert report_mod.main(["--source-pack", "tests/fixtures/tece/source_pack"]) == 0
    out = capsys.readouterr().out
    assert "source_pack_candidate_count" in out
    assert "source_pack_compatibility_evidence_status: missing_article_level_compatibility_matrix" in out
    assert "OVERALL: TECE_SOURCE_PACK_INVENTORY_INCOMPLETE" in out


def test_tece_source_pack_does_not_add_rows_to_aco_canonical_frames(monkeypatch):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], []))

    baseline = CanonicalAcoFrames(
        registry=pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"]}),
        products=pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"], "ready_for_benchmark": [True]}),
        comparison=pd.DataFrame({"product_id": ["aco-1"], "manufacturer": ["aco"]}),
        excluded=pd.DataFrame(),
        evidence=pd.DataFrame(),
        bom_options=pd.DataFrame(),
    )
    before = {name: frame.copy(deep=True) for name, frame in baseline.__dict__.items()}

    report = report_mod.build_report(source_pack="tests/fixtures/tece/source_pack")

    for name, frame in baseline.__dict__.items():
        pd.testing.assert_frame_equal(frame, before[name])
    assert report.source_pack is not None
    assert all(row.article_number for row in report.source_pack.rows)


def test_tece_source_pack_report_includes_manifest_metadata_and_scope_counts(monkeypatch):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], []))

    report = report_mod.build_report(source_pack="tests/fixtures/tece/source_pack")

    assert report.source_pack is not None
    assert report.source_pack.manifest is not None
    assert report.source_pack.manifest["sources"][0]["approved_for_benchmark_evidence"] is False
    assert report.source_pack.evidence_scope_counts["article_data"] >= 1
    assert report.source_pack.evidence_scope_counts["technical_datasheet"] == 1
    assert report.source_pack.cover_grate_matrix_evidence_exists is True
    assert report.source_pack.assembly_matrix_evidence_exists is False
    assert report.source_pack.production_promotion_blocked is True
    assert report.production_promotion_blocked is True


def test_tece_source_pack_classifies_channel_body_and_synthetic_blocks():
    report = report_mod.load_source_pack("tests/fixtures/tece/source_pack")
    row = next(row for row in report.rows if row.article_number == "600100")
    assert row.tece_article_role_candidate == "channel_body"
    assert row.tece_family_candidate == "TECEdrainline"
    assert row.classification_confidence == "high"
    assert "synthetic_test_fixture_only" in row.production_blocking_reason
    assert row.production_promotion_blocked is True


def test_tece_source_pack_classifies_datasheet_cover_and_matrix_without_readiness():
    report = report_mod.load_source_pack("tests/fixtures/tece/source_pack")
    datasheet = next(row for row in report.rows if row.article_number == "650001")
    cover = next(row for row in report.rows if row.article_number == "601200")
    matrix = next(row for row in report.rows if row.article_number == "601201")

    assert datasheet.tece_article_role_candidate in {"technical_datasheet_only", "cover_or_grate"}
    assert cover.tece_article_role_candidate == "cover_or_grate"
    assert matrix.tece_article_role_candidate == "compatibility_matrix"
    assert matrix.classification_reason == "evidence_scope=cover_grate_matrix"
    assert all(row.ready_for_benchmark is False for row in report.rows)
    assert all(row.ready_for_customer_view is False for row in report.rows)
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_source_pack_classification_counts_are_in_json_report(monkeypatch, capsys):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: ([], []))
    assert report_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json"]) == 0
    payload = capsys.readouterr().out
    assert '"source_pack_classification_summary"' in payload
    assert '"role_counts"' in payload
    assert '"family_counts"' in payload
    assert '"classification_confidence_counts"' in payload
    assert '"production_blocking_reason_counts"' in payload


def test_tece_evidence_gap_synthetic_fixture_pack_status_and_json(capsys):
    from tools import report_tece_evidence_gap as gap_mod

    report = gap_mod.build_evidence_gap_report("tests/fixtures/tece/source_pack")

    assert report.overall_status == "OVERALL: TECE_EVIDENCE_GAP_SYNTHETIC_ONLY"
    assert report.gap_summary["synthetic_fixture_only"] is True
    assert report.gap_summary["approved_benchmark_evidence_missing"] is True
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False

    assert gap_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json"]) == 0
    payload = capsys.readouterr().out
    assert '"gap_summary"' in payload
    assert '"recommended_next_actions"' in payload
    assert '"OVERALL: TECE_EVIDENCE_GAP_SYNTHETIC_ONLY"' in payload


def test_tece_evidence_gap_missing_cover_grate_matrix_is_blocker(tmp_path):
    from tools import report_tece_evidence_gap as gap_mod

    (tmp_path / "tece_real_product.txt").write_text(
        "Product name: TECEdrainline channel body approved sample\n"
        "Article number: 700100\nProduct family: TECEdrainline\n"
        "Nominal length: 1200 mm\nFlow rate: 0.8 l/s\nOutlet: DN50\n"
        "Height adjustment: 95-150 mm\nWater seal: 50 mm\nInstallation height: 95 mm\n",
        encoding="utf-8",
    )
    (tmp_path / "tece_cover.txt").write_text(
        "Product name: TECEdrainline cover grate approved sample\n"
        "Article number: 700200\nProduct family: TECEdrainline\n",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(
        json.dumps({"sources": [
            {"source_file": "tece_real_product.txt", "source_type": "txt", "source_origin": "public", "evidence_scope": "article_data", "approved_for_benchmark_evidence": True},
            {"source_file": "tece_cover.txt", "source_type": "txt", "source_origin": "public", "evidence_scope": "article_data", "approved_for_benchmark_evidence": True},
        ]}),
        encoding="utf-8",
    )

    report = gap_mod.build_evidence_gap_report(tmp_path)

    assert report.overall_status == "OVERALL: TECE_EVIDENCE_GAP_COMPATIBILITY_BLOCKED"
    assert report.cover_grate_matrix_evidence_exists is False
    assert report.gap_summary["missing_explicit_cover_grate_compatibility_matrix"] is True
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_evidence_gap_reports_missing_technical_fields(tmp_path):
    from tools import report_tece_evidence_gap as gap_mod

    (tmp_path / "tece_real_product.txt").write_text(
        "Product name: TECEdrainline channel body approved sample\n"
        "Article number: 700100\nProduct family: TECEdrainline\n"
        "Nominal length: 1200 mm\n",
        encoding="utf-8",
    )
    (tmp_path / "tece_matrix.txt").write_text(
        "TECEdrainline compatibility matrix compatible with cover article number 700300 and channel 700100. Article number: 700300",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(
        json.dumps({"sources": [
            {"source_file": "tece_real_product.txt", "source_type": "txt", "source_origin": "public", "evidence_scope": "article_data", "approved_for_benchmark_evidence": True},
            {"source_file": "tece_matrix.txt", "source_type": "txt", "source_origin": "public", "evidence_scope": "cover_grate_matrix", "approved_for_benchmark_evidence": True},
        ]}),
        encoding="utf-8",
    )

    report = gap_mod.build_evidence_gap_report(tmp_path)

    assert report.overall_status == "OVERALL: TECE_EVIDENCE_GAP_TECHNICAL_DATA_INCOMPLETE"
    assert report.missing_field_counts["flow_rate_lps"] >= 1
    assert report.gap_summary["missing_complete_technical_datasheets"] is True
