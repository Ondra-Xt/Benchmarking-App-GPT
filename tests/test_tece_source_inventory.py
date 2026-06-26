from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from src.canonical_aco_export import CanonicalAcoFrames
from tools import report_tece_source_inventory as report_mod
from tools import report_tece_compatibility_diagnostics as compat_mod
from tools import report_tece_source_pack_classification as classification_mod
from tools import report_tece_evidence_gap as gap_mod
from tools.tece_report_output import write_json_output
from tools.report_tece_source_inventory import TeceSourcePackRow


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


def test_tece_json_output_preserves_unicode_in_utf8_file(tmp_path):
    out = tmp_path / "unicode_report.json"

    write_json_output({"flow_text": "≥ 0.72 l/s"}, out)

    assert out.read_bytes().decode("utf-8")
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["flow_text"] == "≥ 0.72 l/s"
    assert "\\u2265" not in out.read_text(encoding="utf-8")


def test_tece_inventory_json_out_writes_valid_utf8_file(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(report_mod.tece, "discover_candidates", lambda **_kwargs: (_sample_candidates(), []))
    monkeypatch.setattr(report_mod.tece, "extract_parameters", _sample_params)
    out = tmp_path / "inventory_report.json"

    assert report_mod.main(["--json", "--out", str(out)]) == 0

    assert capsys.readouterr().out == ""
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["candidate_count"] == 1
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False


def test_tece_classification_json_out_writes_valid_utf8_file(tmp_path, capsys):
    out = tmp_path / "classification_report.json"

    assert classification_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out)]) == 0

    assert capsys.readouterr().out == ""
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["source_pack_candidate_count"] >= 2
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False


def test_tece_evidence_gap_json_out_writes_valid_utf8_file(tmp_path, capsys):
    out = tmp_path / "evidence_gap_report.json"

    assert gap_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out)]) == 0

    assert capsys.readouterr().out == ""
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["overall_status"] == "OVERALL: TECE_EVIDENCE_GAP_SYNTHETIC_ONLY"
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False


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


def test_tece_pdf_page_range_excludes_unrelated_pages_and_reports_metadata(tmp_path, monkeypatch):
    pdf = tmp_path / "catalog.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% synthetic placeholder")
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "catalog.pdf",
        "source_type": "pdf",
        "source_origin": "manual_download",
        "evidence_scope": "article_data",
        "page_start": 2,
        "page_end": 2,
        "page_range_label": "TECEdrainline drainage section",
        "approved_for_benchmark_evidence": False,
        "notes": "Synthetic test fixture."
    }]}), encoding="utf-8")

    class Page:
        def __init__(self, text):
            self.text = text
        def extract_text(self):
            return self.text
    class Reader:
        def __init__(self, _path):
            self.pages = [
                Page("Unrelated WC module Article number: 999999 Product family: TECEprofil"),
                Page("Product name: TECEdrainline channel body Article number: 600100 Nominal length: 1200 mm Flow rate: 0.8 l/s Outlet: DN50 Height adjustment: 95-150 mm Water seal: 50 mm Installation height: 95 mm"),
                Page("Unrelated module Article number: 888888"),
            ]
    import pypdf
    monkeypatch.setattr(pypdf, "PdfReader", Reader)

    report = report_mod.load_source_pack(tmp_path)

    assert report.article_numbers == ["600100"]
    assert "999999" not in report.article_numbers
    assert report.page_range_label_counts == {"TECEdrainline drainage section": 1}
    row = report.rows[0]
    assert row.source_page_start == 2
    assert row.source_page_end == 2
    assert row.page_range_label == "TECEdrainline drainage section"
    assert row.production_promotion_blocked is True
    assert row.ready_for_benchmark is False
    assert row.ready_for_customer_view is False


def test_source_pack_report_json_outputs_are_not_ingested(tmp_path):
    (tmp_path / "tece_product.txt").write_text(
        "Product name: TECEdrainline channel body Article number: 700100 Nominal length: 1200 mm",
        encoding="utf-8",
    )
    for name in ("inventory_report.json", "classification_report.json", "evidence_gap_report.json"):
        (tmp_path / name).write_text('{"article_number":"999999","product_family":"TECEdrainline"}', encoding="utf-8")
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "tece_product.txt",
        "source_type": "txt",
        "source_origin": "unknown",
        "evidence_scope": "article_data",
        "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)

    assert report.article_numbers == ["700100"]
    assert all(row.article_number != "999999" for row in report.rows)
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_same_pdf_manifest_entries_extract_independent_page_ranges(tmp_path, monkeypatch):
    pdf = tmp_path / "catalog.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% synthetic placeholder")
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [
        {
            "source_file": "catalog.pdf",
            "source_type": "pdf",
            "source_origin": "manual_download",
            "evidence_scope": "article_data",
            "page_start": 1,
            "page_end": 1,
            "page_range_label": "TECEdrainline 271-294",
            "product_family_hint": "TECEdrainline",
            "approved_for_benchmark_evidence": False,
        },
        {
            "source_file": "catalog.pdf",
            "source_type": "pdf",
            "source_origin": "manual_download",
            "evidence_scope": "technical_datasheet",
            "page_start": 2,
            "page_end": 2,
            "page_range_label": "TECEdrainprofile 257-270",
            "product_family_hint": "TECEdrainprofile",
            "approved_for_benchmark_evidence": False,
        },
    ]}), encoding="utf-8")

    class Page:
        def __init__(self, text):
            self.text = text
        def extract_text(self):
            return self.text
    class Reader:
        def __init__(self, _path):
            self.pages = [
                Page("Product name: TECEdrainline channel body Article number: 700101 Nominal length: 1200 mm Flow rate: 0.8 l/s Outlet: DN50"),
                Page("Product name: TECEdrainprofile datasheet Article number: 700202 Nominal length: 900 mm Flow rate: 0.7 l/s Outlet: DN50"),
            ]
    import pypdf
    monkeypatch.setattr(pypdf, "PdfReader", Reader)

    report = report_mod.load_source_pack(tmp_path)

    assert report.article_numbers == ["700101", "700202"]
    assert report.page_range_label_counts == {"TECEdrainline 271-294": 1, "TECEdrainprofile 257-270": 1}
    assert {row.page_range_label for row in report.rows} == {"TECEdrainline 271-294", "TECEdrainprofile 257-270"}
    assert {row.source_page_start for row in report.rows} == {1, 2}
    assert all(row.source_file == "catalog.pdf" for row in report.rows)
    assert all(row.production_promotion_blocked for row in report.rows)
    assert report.production_promotion_blocked is True


def test_tece_catalog_text_extracts_multiple_table_rows_and_family_fallback(tmp_path):
    (tmp_path / "catalog.txt").write_text(
        "TECEdrainprofile Designrost Tabelle Nennlänge Oberfläche Best.-Nr. "
        "800 mm Edelstahl 600710 900 mm schwarz 600711. "
        "Nebenhinweis TECEdrainline darf Familie nicht überschreiben.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "catalog.txt",
        "source_type": "txt",
        "source_origin": "manual_download",
        "evidence_scope": "article_data",
        "page_range_label": "TECEdrainprofile 257-270",
        "product_family_hint": "TECEdrainprofile",
        "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)

    assert report.source_pack_candidate_count == 2
    assert report.article_numbers == ["600710", "600711"]
    assert report.page_range_label_counts == {"TECEdrainprofile 257-270": 2}
    assert all(row.tece_family_candidate == "TECEdrainprofile" for row in report.rows)
    assert all(row.product_family == "TECEdrainprofile" for row in report.rows)
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False


def test_tece_drain_block_extracts_technical_and_conditional_flows(tmp_path):
    (tmp_path / "drain.txt").write_text(
        "TECEdrainline Ablauf DN 40 Aufbauhöhe 68,5 mm reduzierte Sperrwasserhöhe 30 mm "
        "Ablaufleistung >=0,52/>=0,60 l/s bei 10/20 mm Aufstau Best.-Nr. 650004",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "drain.txt",
        "source_type": "txt",
        "source_origin": "manual_download",
        "evidence_scope": "technical_datasheet",
        "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline",
        "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)
    row = report.rows[0]

    assert row.article_number == "650004"
    assert row.outlet_dn == "DN40"
    assert row.water_seal_mm == 30
    assert row.installation_height_mm == 68
    assert row.flow_rate_lps == ""
    assert row.conditional_technical_values == [
        {"parameter_name": "flow_rate_lps", "value": 0.52, "condition_type": "head_water_level", "condition_value": 10, "condition_unit": "mm", "condition_label": "10 mm Aufstau"},
        {"parameter_name": "flow_rate_lps", "value": 0.6, "condition_type": "head_water_level", "condition_value": 20, "condition_unit": "mm", "condition_label": "20 mm Aufstau"},
    ]
    assert report.conditional_technical_value_count == 2
    assert report.production_promotion_blocked is True


def test_real_catalog_source_pack_is_not_synthetic_only_and_is_compatibility_blocked(tmp_path):
    (tmp_path / "Sortimentsliste_TECE_DE_2026_web.pdf").write_bytes(b"%PDF-1.4\n% placeholder")
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "Sortimentsliste_TECE_DE_2026_web.pdf",
        "source_type": "pdf",
        "source_origin": "manual_download",
        "source_url": "https://www.tece.com/example/Sortimentsliste_TECE_DE_2026_web.pdf",
        "document_title": "TECE Sortimentsliste 2026 Deutschland",
        "notes": "Real TECE catalogue source candidate; diagnostic-only.",
        "evidence_scope": "article_data",
        "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    from tools import report_tece_evidence_gap as gap_mod

    report = gap_mod.build_evidence_gap_report(tmp_path)

    assert report.gap_summary["synthetic_fixture_only"] is False
    assert report.gap_summary["approved_benchmark_evidence_missing"] is True
    assert report.overall_status == "OVERALL: TECE_EVIDENCE_GAP_COMPATIBILITY_BLOCKED"
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False


def test_tece_compatibility_same_length_candidate_is_diagnostic_only(tmp_path):
    from tools import report_tece_compatibility_diagnostics as compat_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainline channel body Product name: body Article number: 650000 Nominal length: 1200 mm. "
        "TECEdrainline grate Abdeckung Rost Product name: cover Article number: 601200 Nominal length: 1200 mm.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "pack.txt", "source_type": "txt", "source_origin": "manual_download",
        "evidence_scope": "article_data", "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = compat_mod.build_compatibility_diagnostics_report(tmp_path)
    pairings = report.families[0].possible_pairings

    assert report.compatibility_candidate_count == 1
    assert pairings[0].evidence_type == "same_length_same_family_candidate"
    assert pairings[0].diagnostic_only is True
    assert pairings[0].production_safe is False
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_compatibility_explicit_text_pairing_detected_separately(tmp_path):
    from tools import report_tece_compatibility_diagnostics as compat_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainline channel body Product name: body Article number: 650000 Nominal length: 1200 mm. "
        "TECEdrainline grate Product name: cover Article number: 601200 Nominal length: 1200 mm. "
        "Compatibility note: Article 650000 is compatible with cover 601200.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "pack.txt", "source_type": "txt", "source_origin": "manual_download",
        "evidence_scope": "article_data", "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = compat_mod.build_compatibility_diagnostics_report(tmp_path)
    pairing = report.families[0].possible_pairings[0]

    assert pairing.evidence_type == "explicit_text_pairing"
    assert pairing.evidence_confidence == "high"
    assert pairing.production_safe is False
    assert report.explicit_article_level_compatibility_evidence_exists is True
    assert report.production_promotion_blocked is True


def test_tece_compatibility_json_out_writes_utf8(tmp_path):
    from tools import report_tece_compatibility_diagnostics as compat_mod
    out = tmp_path / "tece_compatibility.json"
    assert compat_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out)]) == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert "families" in payload


def test_tece_evidence_gap_integrates_compatibility_diagnostic_counts_but_blocks(tmp_path):
    from tools import report_tece_evidence_gap as gap_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainline channel body Article number: 650000 Nominal length: 1200 mm. "
        "TECEdrainline grate Abdeckung Article number: 601200 Nominal length: 1200 mm.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "pack.txt", "source_type": "txt", "source_origin": "manual_download",
        "evidence_scope": "article_data", "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = gap_mod.build_evidence_gap_report(tmp_path)

    assert report.compatibility_diagnostic_available is True
    assert report.compatibility_candidate_count == 1
    assert report.article_level_compatibility_evidence_exists is False
    assert report.overall_status == "OVERALL: TECE_EVIDENCE_GAP_COMPATIBILITY_BLOCKED"
    assert report.production_promotion_blocked is True


def _compat_row(article: str, role: str, length: int, family: str = "TECEdrainline", evidence: str = "") -> TeceSourcePackRow:
    return TeceSourcePackRow(
        source_file=f"{article}.txt",
        source_type="txt",
        product_family=family,
        product_name=f"{family} {role} {length} mm {article}",
        article_number=article,
        source_url="",
        nominal_length_mm=length,
        flow_rate_lps="",
        water_seal_mm="",
        outlet_dn="",
        height_adj_min_mm="",
        height_adj_max_mm="",
        installation_height_mm="",
        evidence_text=evidence,
        missing_fields=[],
        confidence=0.5,
        compatibility_evidence_type="missing",
        article_level_compatibility_evidence_exists=False,
        production_promotion_blocked=True,
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        recommended_next_action="diagnostic only",
        tece_article_role_candidate=role,
        tece_family_candidate=family,
        classification_confidence="high",
        classification_reason="test",
        production_blocking_reason="missing_article_level_compatibility_matrix",
    )


def test_tece_compatibility_summary_counts_same_length_diagnostic_only(monkeypatch):
    rows = [
        _compat_row("650001", "channel_body", 900),
        _compat_row("601900", "cover_or_grate", 900),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")

    assert report.compatibility_candidate_count == 1
    assert report.evidence_level_counts == {"same_length_same_family_candidate": 1}
    assert report.evidence_type_counts == {"same_length_same_family_candidate": 1}
    assert report.evidence_confidence_counts == {"low": 1}
    assert report.diagnostic_only_candidate_count == 1
    assert report.production_safe_candidate_count == 0
    assert report.family_candidate_counts == {"TECEdrainline": 1}
    assert report.family_evidence_level_counts == {"TECEdrainline": {"same_length_same_family_candidate": 1}}
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_compatibility_json_has_top_level_summary_counts(tmp_path):
    out = tmp_path / "compatibility_report.json"

    assert compat_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out)]) == 0

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert "evidence_level_counts" in payload
    assert "evidence_confidence_counts" in payload
    assert "production_safe_candidate_count" in payload
    assert payload["production_safe_candidate_count"] == 0
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False


def test_tece_compatibility_explicit_evidence_counted_but_not_promoted(monkeypatch):
    evidence = "Compatibility matrix: Article 650001 compatible with 601900."
    body = _compat_row("650001", "channel_body", 900, evidence=evidence)
    cover = _compat_row("601900", "cover_or_grate", 900, evidence=evidence)
    object.__setattr__(body, "compatibility_evidence_type", "explicit_matrix")
    rows = [body, cover]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")

    assert report.evidence_level_counts == {"explicit_article_level_matrix": 1}
    assert report.evidence_confidence_counts == {"high": 1}
    assert report.production_safe_candidate_count == 0
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_compatibility_evidence_audit_detects_explicit_text_pairing(tmp_path):
    from tools import report_tece_compatibility_evidence_audit as audit_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainline channel body Product name: body Article number: 650000 Nominal length: 1200 mm. "
        "TECEdrainline grate Product name: cover Article number: 601200 Nominal length: 1200 mm. "
        "Passend zu Rinne 650000 ist Abdeckung 601200.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "pack.txt", "source_type": "txt", "source_origin": "manual_download",
        "evidence_scope": "article_data", "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = audit_mod.build_audit_report(tmp_path)
    sample = report["examples_by_family_and_evidence_level"]["TECEdrainline"]["explicit_text_pairing"][0]

    assert report["explicit_text_pairing_candidate_count"] == 1
    assert sample["requires_manual_review"] is True
    assert "blocked" in sample["why_not_production_safe"]
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_tece_compatibility_evidence_audit_same_length_and_section_remain_diagnostic(monkeypatch):
    from tools import report_tece_compatibility_evidence_audit as audit_mod
    rows = [
        _compat_row("650001", "channel_body", 900, evidence="section body"),
        _compat_row("601900", "cover_or_grate", 900, evidence="section cover"),
        _compat_row("650002", "channel_body", 1000, evidence="section body"),
        _compat_row("601100", "cover_or_grate", 1100, evidence="section cover"),
    ]
    for row in rows:
        object.__setattr__(row, "page_range_label", "TECEdrainline 271-294")
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = audit_mod.build_audit_report("synthetic")

    assert report["evidence_level_counts"]["same_length_same_family_candidate"] >= 1
    assert report["evidence_level_counts"]["explicit_section_pairing"] >= 1
    assert report["diagnostic_only_candidate_count"] == report["compatibility_candidate_count"]
    for samples in report["examples_by_family_and_evidence_level"]["TECEdrainline"].values():
        assert all(sample["why_not_production_safe"] for sample in samples)


def test_tece_compatibility_evidence_audit_json_out_writes_utf8(tmp_path):
    from tools import report_tece_compatibility_evidence_audit as audit_mod
    out = tmp_path / "audit.json"

    assert audit_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out)]) == 0

    text = out.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False
    assert "examples_by_family_and_evidence_level" in payload


def test_tece_compatibility_audit_does_not_change_aco_canonical_export():
    from src.canonical_aco_export import build_canonical_aco_frames

    frames = build_canonical_aco_frames()

    assert "tece" not in set(frames.products.get("manufacturer", []))
    assert "tece" not in set(frames.comparison.get("manufacturer", []))
    assert "tece" not in set(frames.bom_options.get("manufacturer", []))


def test_tece_refined_roles_and_exclusions(monkeypatch):
    rows = [
        _compat_row("650000", "cover_or_grate", 900),
        _compat_row("650001", "cover_or_grate", 900),
        _compat_row("650002", "cover_or_grate", 900),
        _compat_row("650003", "cover_or_grate", 900),
        _compat_row("650004", "cover_or_grate", 900),
        _compat_row("601900", "cover_or_grate", 900),
        _compat_row("999999", "unknown", 900),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")
    family = report.families[0]

    drain_articles = {row["article_number"] for row in family.candidate_body_channel_drain_articles if row["classified_role"] == "drain_body"}
    assert {"650000", "650001", "650002", "650003", "650004"}.issubset(drain_articles)
    assert report.excluded_reason_counts["same_role_pairing_not_actionable"] >= 1
    assert report.excluded_reason_counts["unknown_role_without_explicit_text_pairing"] >= 1
    assert report.actionable_candidate_count < report.compatibility_candidate_count


def test_tece_complete_sets_summarized_and_drainway_zero_actionable(monkeypatch):
    rows = [
        _compat_row("800001", "complete_set", 1200, family="TECEdrainline", evidence="Komplettset bestehend aus Rinne und Rost"),
        _compat_row("DW001", "unknown", 0, family="TECEdrainway", evidence="TECEdrainway accessory row"),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")

    assert report.complete_set_article_count == 1
    assert report.family_source_row_counts["TECEdrainway"] == 1
    assert report.family_actionable_candidate_counts["TECEdrainway"] == 0
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tece_gap_exposes_actionable_count_and_stays_blocked(tmp_path):
    from tools import report_tece_evidence_gap as gap_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainline Ablauf Article number: 650000 Nominal length: 1200 mm. "
        "TECEdrainline Abdeckung Article number: 601200 Nominal length: 1200 mm.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "pack.txt", "source_type": "txt", "source_origin": "manual_download",
        "evidence_scope": "article_data", "page_range_label": "TECEdrainline 271-294",
        "product_family_hint": "TECEdrainline", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = gap_mod.build_evidence_gap_report(tmp_path)

    assert report.actionable_compatibility_candidate_count == 1
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False

def test_tecedrainprofile_article_ranges_classify_profile_and_drain_roles(monkeypatch):
    rows = [
        _compat_row("670900", "unknown", 900, family="TECEdrainprofile", evidence="TECEdrainprofile profile body article"),
        _compat_row("671900", "unknown", 900, family="TECEdrainprofile", evidence="TECEdrainprofile Profilrinne channel article"),
        _compat_row("673001", "unknown", 0, family="TECEdrainprofile", evidence="TECEdrainprofile drain body"),
        _compat_row("673002", "drain_component", 0, family="TECEdrainprofile", evidence="TECEdrainprofile drain component"),
        _compat_row("674001", "unknown", 0, family="TECEdrainprofile", evidence="TECEdrainprofile accessory"),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")
    family = report.families[0]
    roles = {row["article_number"]: row["classified_role"] for row in family.candidate_body_channel_drain_articles + family.candidate_cover_grate_plate_articles + family.unknown_role_articles}

    assert roles["670900"] == "profile_cover"
    assert roles["671900"] == "profile_cover"
    assert roles["673001"] == "drain_body"
    assert roles["673002"] == "drain_component"
    assert roles["674001"] == "unknown"
    assert report.unknown_role_row_count == 1


def test_tecedrainprofile_profile_to_drain_actionable_but_same_and_unknown_excluded(monkeypatch):
    rows = [
        _compat_row("670900", "unknown", 900, family="TECEdrainprofile", evidence="profile body"),
        _compat_row("671900", "unknown", 900, family="TECEdrainprofile", evidence="profile channel"),
        _compat_row("670901", "unknown", 1000, family="TECEdrainprofile", evidence="profile body"),
        _compat_row("673001", "unknown", 0, family="TECEdrainprofile", evidence="drain body"),
        _compat_row("679999", "unknown", 0, family="TECEdrainprofile", evidence="ambiguous accessory"),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")
    family = report.families[0]
    role_pairs = {pairing.role_pair for pairing in family.possible_pairings}
    excluded_reasons = {entry["role_pair"]: entry["excluded_reason"] for entry in family.excluded_pairings}

    assert "drain_body_to_profile_cover" in role_pairs
    assert excluded_reasons["profile_cover_to_profile_cover"] == "same_role_pairing_not_actionable"
    assert excluded_reasons["profile_cover_to_unknown"] == "unknown_role_without_explicit_text_pairing"
    assert report.family_actionable_candidate_counts["TECEdrainprofile"] >= 2
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tecedrainprofile_unknown_pair_allowed_only_with_explicit_text(monkeypatch):
    evidence = "Passend zu Profil 670900 ist Sonderteil 679999."
    rows = [
        _compat_row("670900", "unknown", 900, family="TECEdrainprofile", evidence=evidence),
        _compat_row("679999", "unknown", 0, family="TECEdrainprofile", evidence=evidence),
    ]
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = compat_mod.build_compatibility_diagnostics_report("synthetic")

    assert report.actionable_candidate_count == 1
    assert report.families[0].possible_pairings[0].evidence_type == "explicit_text_pairing"
    assert report.production_promotion_blocked is True
    assert report.ready_for_benchmark is False
    assert report.ready_for_customer_view is False


def test_tecedrainprofile_source_pack_classifies_article_ranges():
    profile = report_mod.classify_source_pack_row(
        "Product name: TECEdrainprofile profile Article number: 670900 Nominal length: 900 mm",
        {"product_family_hint": "TECEdrainprofile", "evidence_scope": "article_data"},
    )
    channel = report_mod.classify_source_pack_row(
        "Product name: TECEdrainprofile Profilrinne Article number: 671900 Nominal length: 900 mm",
        {"product_family_hint": "TECEdrainprofile", "evidence_scope": "article_data"},
    )
    drain = report_mod.classify_source_pack_row(
        "Product name: TECEdrainprofile Ablauf Article number: 673003",
        {"product_family_hint": "TECEdrainprofile", "evidence_scope": "article_data"},
    )

    assert profile["tece_article_role_candidate"] == "profile_cover"
    assert channel["tece_article_role_candidate"] == "profile_cover"
    assert drain["tece_article_role_candidate"] == "drain_body"



def test_tecedrainprofile_table_rows_keep_same_row_values(tmp_path):
    pack = tmp_path / "pack"
    pack.mkdir()
    (pack / "profile.txt").write_text(
        "TECEdrainprofile Tabelle Länge Breite Farbe Best.-Nr. LE 1 "
        "800 mm 55 mm Edelstahl gebürstet 670800 "
        "800 mm 55 mm Edelstahl poliert 670810 "
        "900 mm 55 mm Edelstahl gebürstet 670900 "
        "900 mm 55 mm Edelstahl poliert 670910 "
        "1000 mm 55 mm Edelstahl gebürstet 671000 "
        "1000 mm 55 mm Edelstahl poliert 671010 "
        "1200 mm 55 mm Edelstahl gebürstet 671200 "
        "1200 mm 55 mm Edelstahl poliert 671210 "
        "1600 mm 55 mm Edelstahl gebürstet 671600",
        encoding="utf-8",
    )
    (pack / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "profile.txt", "source_type": "txt", "source_origin": "manual",
        "product_family_hint": "TECEdrainprofile", "evidence_scope": "article_data",
        "page_start": 257, "page_end": 270, "page_range_label": "TECEdrainprofile 257-270",
        "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(pack)
    by_article = {row.article_number: row for row in report.rows}

    expected = {
        "670800": (800, 55, "Edelstahl gebürstet"),
        "670810": (800, 55, "Edelstahl poliert"),
        "670900": (900, 55, "Edelstahl gebürstet"),
        "670910": (900, 55, "Edelstahl poliert"),
        "671000": (1000, 55, "Edelstahl gebürstet"),
        "671010": (1000, 55, "Edelstahl poliert"),
        "671200": (1200, 55, "Edelstahl gebürstet"),
        "671210": (1200, 55, "Edelstahl poliert"),
        "671600": (1600, 55, "Edelstahl gebürstet"),
    }
    for article, (length, width, finish) in expected.items():
        assert by_article[article].tece_article_role_candidate == "profile_cover"
        assert by_article[article].nominal_length_mm == length
        assert by_article[article].width_mm == width
        assert by_article[article].finish_or_color == finish
    assert by_article["670910"].nominal_length_mm == 900

def test_tece_actionable_review_shortlist_bounded_and_prioritized(monkeypatch):
    from tools import report_tece_actionable_review_shortlist as shortlist_mod
    rows = [
        _compat_row("650001", "drain_body", 900, evidence="section body"),
        _compat_row("601901", "cover_or_grate", 901, evidence="section cover"),
        _compat_row("650002", "drain_body", 1000, evidence="same body"),
        _compat_row("601000", "cover_or_grate", 1000, evidence="same cover"),
        _compat_row("650003", "drain_body", 1100, evidence="same body"),
        _compat_row("601100", "cover_or_grate", 1100, evidence="same cover"),
    ]
    for row in rows[:2]:
        object.__setattr__(row, "page_range_label", "TECEdrainline section")
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = shortlist_mod.build_shortlist_report("synthetic", max_per_family=2)
    levels = [row["evidence_level"] for row in report["shortlisted_candidates"]]

    assert report["summary"]["total_actionable_candidate_count"] > report["summary"]["shortlisted_candidate_count"]
    assert report["summary"]["shortlisted_candidate_count"] == 2
    assert report["total_actionable_candidate_count"] == report["summary"]["total_actionable_candidate_count"]
    assert report["shortlisted_candidate_count"] == report["summary"]["shortlisted_candidate_count"]
    assert levels[0] == "explicit_section_pairing"
    assert "same_length_same_family_candidate" in levels


def test_tece_actionable_review_shortlist_profile_and_blocking_flags(monkeypatch):
    from tools import report_tece_actionable_review_shortlist as shortlist_mod
    rows = [
        _compat_row("670900", "unknown", 900, family="TECEdrainprofile", evidence="profile body"),
        _compat_row("673001", "unknown", 0, family="TECEdrainprofile", evidence="drain body"),
        _compat_row("650001", "drain_body", 900, family="TECEdrainline", evidence="line body"),
        _compat_row("601901", "cover_or_grate", 901, family="TECEdrainline", evidence="line cover"),
    ]
    for row in rows:
        object.__setattr__(row, "page_range_label", f"{row.tece_family_candidate} section")
    monkeypatch.setattr(compat_mod, "load_source_pack", lambda source_pack: SimpleNamespace(source_pack_path=str(source_pack), rows=rows))

    report = shortlist_mod.build_shortlist_report("synthetic", max_per_family=10)

    assert report["summary"]["family_shortlist_counts"]["TECEdrainprofile"] >= 1
    assert report["family_shortlist_counts"] == report["summary"]["family_shortlist_counts"]
    assert report["evidence_level_counts"] == report["summary"]["evidence_level_counts"]
    assert report["role_pair_counts"] == report["summary"]["role_pair_counts"]
    profile_rows = [row for row in report["shortlisted_candidates"] if row["family"] == "TECEdrainprofile"]
    assert profile_rows
    assert profile_rows[0]["role_pair"] == "drain_body_to_profile_cover"
    assert profile_rows[0]["drain_article_number"] == "673001"
    assert profile_rows[0]["visible_article_number"] == "670900"
    assert profile_rows[0]["primary_article_role"] == "drain_body"
    assert profile_rows[0]["secondary_article_role"] == "profile_cover"
    assert report["summary"]["production_safe_candidate_count"] == 0
    assert report["production_safe_candidate_count"] == 0
    assert report["diagnostic_only_candidate_count"] == report["summary"]["diagnostic_only_candidate_count"]
    assert report["summary"]["production_promotion_blocked"] is True
    assert report["production_promotion_blocked"] is True
    assert report["summary"]["ready_for_benchmark"] is False
    assert report["ready_for_benchmark"] is False
    assert report["summary"]["ready_for_customer_view"] is False
    assert report["ready_for_customer_view"] is False
    assert all(row["manual_review_status"] == "pending_review" for row in report["shortlisted_candidates"])


def test_tece_actionable_review_shortlist_json_out_writes_utf8(tmp_path):
    from tools import report_tece_actionable_review_shortlist as shortlist_mod
    out = tmp_path / "tece_review_shortlist.json"

    assert shortlist_mod.main(["--source-pack", "tests/fixtures/tece/source_pack", "--json", "--out", str(out), "--max-per-family", "5"]) == 0

    payload = json.loads(out.read_text(encoding="utf-8"))
    required_top_level_fields = {
        "total_actionable_candidate_count",
        "shortlisted_candidate_count",
        "family_shortlist_counts",
        "evidence_level_counts",
        "role_pair_counts",
        "production_safe_candidate_count",
        "diagnostic_only_candidate_count",
        "production_promotion_blocked",
        "ready_for_benchmark",
        "ready_for_customer_view",
        "shortlisted_candidates",
    }
    assert required_top_level_fields.issubset(payload)
    assert "summary" in payload
    assert payload["summary"]["production_safe_candidate_count"] == 0
    assert payload["production_safe_candidate_count"] == 0
    assert payload["summary"]["production_promotion_blocked"] is True
    assert payload["production_promotion_blocked"] is True
    assert payload["summary"]["ready_for_benchmark"] is False
    assert payload["ready_for_benchmark"] is False
    assert payload["summary"]["ready_for_customer_view"] is False
    assert payload["ready_for_customer_view"] is False
    for field in required_top_level_fields - {"shortlisted_candidates"}:
        assert payload[field] == payload["summary"][field]
    assert "shortlisted_candidates" in payload


def test_tece_actionable_review_shortlist_does_not_change_aco_canonical_export():
    from tools import report_tece_actionable_review_shortlist as shortlist_mod
    from src.canonical_aco_export import build_canonical_aco_frames

    shortlist_mod.build_shortlist_report("tests/fixtures/tece/source_pack", max_per_family=5)
    frames = build_canonical_aco_frames()

    assert "tece" not in set(frames.products.get("manufacturer", []))
    assert "tece" not in set(frames.comparison.get("manufacturer", []))
    assert "tece" not in set(frames.bom_options.get("manufacturer", []))


def test_full_inventory_csv_exports_all_source_pack_rows(tmp_path):
    from tools import export_tece_inventory_review_csv as csv_mod
    out = tmp_path / "inventory.csv"

    csv_mod.export_inventory_csv("tests/fixtures/tece/source_pack", out)

    raw = out.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
    text = raw.decode("utf-8-sig")
    assert "600100" in text
    assert "601200" in text
    assert "650001" in text
    assert "3601050" in text
    assert "production_blocking_reason" in text


def test_exported_inventory_csv_includes_tecedrainpoint_s_sentinel_and_stays_blocked(tmp_path):
    import csv
    import json
    from tools import export_tece_inventory_review_csv as csv_mod
    from tools import report_tece_source_pack_coverage as coverage_mod

    out = tmp_path / "inventory_review.csv"
    csv_mod.export_inventory_csv("tests/fixtures/tece/source_pack", out)

    with out.open(encoding="utf-8-sig", newline="") as fh:
        rows = {row["article_number"]: row for row in csv.DictReader(fh)}

    row = rows["3601050"]
    conditional_values = json.loads(row["conditional_technical_values"])
    assert row["tece_family_candidate"] == "TECEdrainpoint"
    assert row["article_role"] == "drain_body"
    assert row["flow_rate_lps"] == ""
    assert conditional_values == [
        {"condition_label": "10 mm Aufstau", "condition_type": "head_water_level", "condition_unit": "mm", "condition_value": 10, "parameter_name": "flow_rate_lps", "value": 0.52},
        {"condition_label": "20 mm Aufstau", "condition_type": "head_water_level", "condition_unit": "mm", "condition_value": 20, "parameter_name": "flow_rate_lps", "value": 0.6},
    ]
    assert row["production_promotion_blocked"] == "True"
    assert row["ready_for_benchmark"] == "False"
    assert row["ready_for_customer_view"] == "False"

    coverage = coverage_mod.build_coverage_report("tests/fixtures/tece/source_pack")
    point = coverage["families"]["TECEdrainpoint S"]
    assert "3601050" not in point["missing_sentinel_article_numbers"]
    assert "missing_sentinel_articles" not in point["extraction_warnings"]
    assert point["extracted_row_count"] >= 1
    assert coverage["production_safe_candidate_count"] == 0
    assert coverage["production_promotion_blocked"] is True
    assert coverage["ready_for_benchmark"] is False
    assert coverage["ready_for_customer_view"] is False


def test_tece_source_pack_coverage_includes_all_four_families(tmp_path):
    from tools import report_tece_source_pack_coverage as coverage_mod
    (tmp_path / "pack.txt").write_text(
        "TECEdrainway Zubehör Article number: 800001. "
        "TECEdrainprofile Ablauf Article number: 673001. "
        "TECEdrainline Ablauf Article number: 600906 Länge: 900 mm. "
        "TECEdrainpoint S Ablaufset Article number: 3601050 Ablaufleistung 0,52/0,60 l/s bei 10/20 mm Aufstau.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [
        {"source_file": "pack.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainway", "page_start": 245, "page_end": 256, "page_range_label": "TECEdrainway 245-256", "approved_for_benchmark_evidence": False},
        {"source_file": "pack.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainprofile", "page_start": 257, "page_end": 270, "page_range_label": "TECEdrainprofile 257-270", "approved_for_benchmark_evidence": False},
        {"source_file": "pack.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294, "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False},
        {"source_file": "pack.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainpoint S", "page_start": 295, "page_end": 326, "page_range_label": "TECEdrainpoint S 295-326", "approved_for_benchmark_evidence": False},
    ]}), encoding="utf-8")

    payload = coverage_mod.build_coverage_report(tmp_path)

    assert set(payload["families"]) == {"TECEdrainway", "TECEdrainprofile", "TECEdrainline", "TECEdrainpoint S"}
    assert payload["production_safe_candidate_count"] == 0
    assert payload["production_promotion_blocked"] is True
    assert payload["ready_for_benchmark"] is False
    assert payload["ready_for_customer_view"] is False


def test_tecedrainline_sentinel_lengths_ignore_le_index_columns(tmp_path):
    (tmp_path / "line.txt").write_text(
        "Artikel-Schnellsuche Best.-Nr. LE 1 LE 2 LE 3 Seite 600906 40 80 120 279 601006 40 80 120 279 601206 40 80 120 279 601506 40 80 120 279. "
        "TECEdrainline product table 900 mm Edelstahl gebürstet 600906 1000 mm Edelstahl gebürstet 601006 1200 mm Edelstahl gebürstet 601206 1500 mm Edelstahl gebürstet 601506",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "line.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data",
        "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294, "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)
    by_article = {row.article_number: row for row in report.rows}

    assert by_article["600906"].nominal_length_mm == 900
    assert by_article["601006"].nominal_length_mm == 1000
    assert by_article["601206"].nominal_length_mm == 1200
    assert by_article["601506"].nominal_length_mm == 1500
    assert all(by_article[a].nominal_length_mm != 40 for a in ["600906", "601006", "601206", "601506"])


def test_tecedrainpoint_s_sentinel_and_conditional_flows_preserved(tmp_path):
    (tmp_path / "point.txt").write_text(
        "TECEdrainpoint S Ablaufset DN 50 Aufbauhöhe 95 mm Sperrwasserhöhe 50 mm "
        "Ablaufleistung >=0,52/>=0,60 l/s bei 10/20 mm Aufstau Best.-Nr. 3601050",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "point.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data",
        "product_family_hint": "TECEdrainpoint S", "page_start": 295, "page_end": 326, "page_range_label": "TECEdrainpoint S 295-326", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)
    row = next(row for row in report.rows if row.article_number == "3601050")

    assert row.tece_family_candidate == "TECEdrainpoint"
    assert row.tece_article_role_candidate == "drain_body"
    assert row.outlet_dn == "DN50"
    assert row.conditional_technical_values == [
        {"parameter_name": "flow_rate_lps", "value": 0.52, "condition_type": "head_water_level", "condition_value": 10, "condition_unit": "mm", "condition_label": "10 mm Aufstau"},
        {"parameter_name": "flow_rate_lps", "value": 0.6, "condition_type": "head_water_level", "condition_value": 20, "condition_unit": "mm", "condition_label": "20 mm Aufstau"},
    ]


def test_tece_review_shortlist_note_distinguishes_inventory(tmp_path):
    from tools import report_tece_actionable_review_shortlist as shortlist_mod
    report = shortlist_mod.build_shortlist_report("tests/fixtures/tece/source_pack")
    assert any("not the complete TECE inventory" in note for note in report["report_notes"])
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True

def test_tecedrainline_designrost_column_table_keeps_same_row_lengths(tmp_path):
    (tmp_path / "designrost.txt").write_text(
        'TECEdrainline Designrost "quadratum" aus Edelstahl für Duschrinne '
        'Nennlänge 700 mm 800 mm 900 mm 1000 mm 1200 mm 1500 mm '
        'Oberfläche gebürstet gebürstet gebürstet gebürstet gebürstet gebürstet '
        'Best.-Nr. 600751 600851 600951 601051 601251 601551 LE 1 1 1 1 1 1 1',
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "designrost.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data",
        "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294,
        "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)
    by_article = {row.article_number: row for row in report.rows}

    expected = {
        "600751": 700,
        "600851": 800,
        "600951": 900,
        "601051": 1000,
        "601251": 1200,
        "601551": 1500,
    }
    for article, length in expected.items():
        assert by_article[article].nominal_length_mm == length
        assert by_article[article].finish_or_color == "gebürstet"
        assert by_article[article].tece_article_role_candidate == "cover_or_grate"
    assert by_article["601251"].nominal_length_mm != 700
    assert all(by_article[article].nominal_length_mm != 1 for article in expected)


def test_tece_coverage_warns_for_high_tecedrainline_unknown_role_count(tmp_path):
    from tools import report_tece_source_pack_coverage as coverage_mod
    for name, text in {
        "unknown1.txt": "TECEdrainline Article number: 700001.",
        "unknown2.txt": "TECEdrainline Article number: 700002.",
        "cover.txt": "TECEdrainline Rost Article number: 700003.",
    }.items():
        (tmp_path / name).write_text(text, encoding="utf-8")
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [
        {"source_file": "unknown1.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294, "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False},
        {"source_file": "unknown2.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294, "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False},
        {"source_file": "cover.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data", "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294, "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False},
    ]}), encoding="utf-8")

    payload = coverage_mod.build_coverage_report(tmp_path)
    line = payload["families"]["TECEdrainline"]

    assert line["unknown_role_count"] >= 2
    assert "high_unknown_role_count" in line["extraction_warnings"]


def test_exported_inventory_csv_prefers_structured_tecedrainline_rows(tmp_path):
    import csv
    from tools import export_tece_inventory_review_csv as csv_mod

    pack = tmp_path / "pack"
    pack.mkdir()
    (pack / "line_tables.txt").write_text(
        'TECEdrainline Designrost "quadratum" aus Edelstahl für Duschrinne '
        'Nennlänge Oberfläche Best.-Nr. LE 1 '
        '700 mm gebürstet 600751 1 St. 800 mm gebürstet 600851 1 St. '
        '900 mm gebürstet 600951 1 St. 1000 mm gebürstet 601051 1 St. '
        '1200 mm gebürstet 601251 1 St. 1500 mm gebürstet 601551 1 St. '
        'Generic bad context Article number: 601251 Nennlänge 700 mm complete set should not win. '
        'TECEdrainline Designabdeckung steel II aus Edelstahl Best.-Nr. LE 1 '
        '600800 600900 601000 601200 601500 Nennlängen 800 900 1000 1200 1500 '
        'TECEdrainline Fliesenmulde plate Nennlänge Farbe Best.-Nr. LE 1 '
        '800 mm Edelstahl 600810 1 St. 800 mm schwarz 600811 1 St. 900 mm Edelstahl 600910 1 St. '
        '900 mm schwarz 600911 1 St. 1000 mm Edelstahl 601010 1 St. 1000 mm schwarz 601011 1 St. '
        '1200 mm Edelstahl 601210 1 St. 1200 mm schwarz 601211 1 St. 1500 mm Edelstahl 601510 1 St. 1500 mm schwarz 601511 1 St.',
        encoding="utf-8",
    )
    (pack / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "line_tables.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data",
        "product_family_hint": "TECEdrainline", "page_start": 271, "page_end": 294,
        "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")
    out = tmp_path / "inventory_review.csv"

    csv_mod.export_inventory_csv(pack, out)
    with out.open(encoding="utf-8-sig", newline="") as fh:
        rows = {row["article_number"]: row for row in csv.DictReader(fh)}

    expected_lengths = {
        "600851": "800", "600951": "900", "601051": "1000", "601251": "1200", "601551": "1500",
        "600800": "800", "600900": "900", "601000": "1000", "601200": "1200", "601500": "1500",
        "600810": "800", "600811": "800", "600910": "900", "600911": "900", "601010": "1000",
        "601011": "1000", "601210": "1200", "601211": "1200", "601510": "1500", "601511": "1500",
    }
    for article, length in expected_lengths.items():
        assert rows[article]["nominal_length_mm"] == length
        assert rows[article]["finish_or_color"]
        assert rows[article]["article_role"] == "cover_or_grate"
        assert rows[article]["article_role"] not in {"unknown", "complete_set"}
        assert rows[article]["extraction_method"] in {"structured_column_table", "structured_designrost_column_table", "structured_known_cover_table"}
        assert rows[article]["production_promotion_blocked"] == "True"
        assert rows[article]["ready_for_benchmark"] == "False"
        assert rows[article]["ready_for_customer_view"] == "False"
    assert rows["601251"]["nominal_length_mm"] == "1200"
    assert rows["601251"]["nominal_length_mm"] != "700"


def test_tecedrainline_known_roles_and_accessories_do_not_inherit_technical_fields(tmp_path):
    (tmp_path / "roles.txt").write_text(
        "TECEdrainline Ablauf DN 50 Aufbauhöhe 95 mm Sperrwasserhöhe 50 mm Best.-Nr. 650000. "
        "TECEdrainline Zubehör Ersatzteil nearby DN 50 Aufbauhöhe 95 mm Best.-Nr. 660004. "
        "TECEdrainline Ersatzteil nearby DN 50 Sperrwasserhöhe 50 mm Best.-Nr. 668010.",
        encoding="utf-8",
    )
    (tmp_path / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": [{
        "source_file": "roles.txt", "source_type": "txt", "source_origin": "manual", "evidence_scope": "article_data",
        "product_family_hint": "TECEdrainline", "page_range_label": "TECEdrainline 271-294", "approved_for_benchmark_evidence": False,
    }]}), encoding="utf-8")

    report = report_mod.load_source_pack(tmp_path)
    rows = {row.article_number: row for row in report.rows}

    assert rows["650000"].tece_article_role_candidate == "drain_body"
    for article in ("660004", "668010"):
        assert rows[article].tece_article_role_candidate == "accessory"
        assert rows[article].outlet_dn == ""
        assert rows[article].water_seal_mm == ""
        assert rows[article].installation_height_mm == ""
