from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pandas as pd

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_compatibility_pair_audit_ranking as mod
from tools import report_tece_overlay_compatibility_diagnostic as diag_mod


def _assembly_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    specs = (
        ("easyflow", 2, False, "partial"),
        ("easyflowplus", 6, True, "complete"),
        ("showerdrain_splus", 16, True, "complete"),
        ("showerdrain_c", 4, True, "complete"),
        ("showerdrain_cplus", 30, True, "explicit_source_ready_production_assembly"),
        ("showerdrain_mplus", 4, False, "conditional_parameter_available_production_blocked"),
    )
    for family, count, ready, status in specs:
        for index in range(count):
            row: dict[str, object] = {
                "product_id": f"aco-assembled-{family}-{index}",
                "assembled_family": family,
                "assembled_from_bom": "TRUE",
                "data_quality_status": status,
                "is_complete_technical_data": "TRUE" if ready else "FALSE",
                "ready_for_benchmark": "TRUE" if ready else "FALSE",
                "ready_for_customer_view": "TRUE" if ready and family != "showerdrain_mplus" else "FALSE",
                "customer_view_enabled": "TRUE" if ready and family != "showerdrain_mplus" else "FALSE",
                "flow_rate_lps": 0.7 if ready else "",
                "height_adj_min_mm": 10 if ready else "",
                "height_adj_max_mm": 100 if ready else "",
            }
            if family == "showerdrain_cplus":
                base_id = (
                    "aco-showerdrain-cplus-standard-h92"
                    if index < 15 else "aco-showerdrain-cplus-low-h69"
                )
                hydraulics = aco_mod.CPLUS_BASE_HYDRAULICS[base_id]
                row.update({
                    "product_id": f"aco-assembled-showerdrain-cplus-{index}",
                    "product_family": "showerdrain_cplus",
                    "family": "showerdrain_cplus",
                    "assembly_model": "base_x_grate",
                    "base_id": base_id,
                    "base_article_number": "9010.85.10" if index < 15 else "9010.85.20",
                    "grate_id": f"aco-cplus-design-grate-{index % 15}",
                    "grate_article_number": f"9010.88.{index % 15:02d}",
                    "product_name": f"C+ approved grate {index}",
                    **hydraulics,
                })
            if family == "easyflow":
                row.update({
                    "flow_rate_lps": "", "height_adj_min_mm": "", "height_adj_max_mm": "",
                    "is_complete_technical_data": "FALSE",
                    "missing_technical_fields": "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
                })
            rows.append(row)
    return rows


def _products(assemblies: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, assembly in assemblies.iterrows():
        family = assembly["assembled_family"]
        rows.append({
            "product_id": assembly["product_id"],
            "product_family": family,
            "assembly_model": "channel_body_x_drain_body_x_grate" if family == "showerdrain_mplus" else "base_x_grate",
            "flow_rate_lps": assembly["flow_rate_lps"],
            "selected_default_flow_rate_lps": "",
            "ready_for_benchmark": assembly["ready_for_benchmark"],
            "ready_for_customer_view": assembly["ready_for_customer_view"],
            "customer_view_enabled": assembly["customer_view_enabled"],
            "blocked_reason": "blocked_pending_conditional_parameter_scoring" if family == "showerdrain_mplus" else "",
        })
    for article in sorted(aco_mod.BLINE_ARTICLES):
        rows.append({
            "product_id": f"aco-showerdrain-b-finished-set-{article.replace('.', '-')}",
            "product_family": "showerdrain_b",
            "product_article_number": article,
            "article_number": article,
            "assembly_model": "integral_all_in_one_set",
            "flow_rate_lps": "",
            "selected_default_flow_rate_lps": "",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "body_article_number": "",
            "grate_article_number": "",
            "blocked_reason": "blocked_pending_conditional_parameter_scoring",
        })
    rows.append({
        "product_id": "aco-showerdrain-b-family-discovery",
        "product_family": "showerdrain_b",
        "family": "showerdrain_b",
        "candidate_type": "family_navigation",
        "assembly_model": "",
        "flow_rate_lps": "0.90",
        "selected_default_flow_rate_lps": "",
        "ready_for_benchmark": "FALSE",
        "ready_for_customer_view": "FALSE",
        "customer_view_enabled": "FALSE",
        "product_article_number": "",
        "body_article_number": "",
        "grate_article_number": "",
    })
    while len(rows) < 88:
        index = len(rows)
        rows.append({
            "product_id": f"aco-canonical-filler-{index}",
            "product_family": "other",
            "flow_rate_lps": 0.5,
            "ready_for_benchmark": True,
            "ready_for_customer_view": True,
            "customer_view_enabled": True,
        })
    return pd.DataFrame(rows)


def _details(assemblies: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, assembly in assemblies.iterrows():
        family = assembly["assembled_family"]
        ready = family not in {"easyflow", "showerdrain_mplus"}
        rows.append({
            "set_id": assembly["product_id"],
            "assembled_product_id": assembly["product_id"],
            "assembled_family": family,
            "ready_for_benchmark": "TRUE" if ready else "FALSE",
            "ready_for_customer_view": "TRUE" if ready else "FALSE",
            "base_product_id": "aco-easyflow-complete-dn50-ws50" if family == "easyflow" else "",
            "base_article_number": "",
            "article_number": "",
            "selected_article_number": "",
            "data_quality_status": assembly["data_quality_status"],
        })
    return pd.DataFrame(rows)


def _conditions(products: pd.DataFrame) -> pd.DataFrame:
    rows = []
    conditional = products[
        products["product_family"].eq("showerdrain_mplus")
        | products.apply(aco_mod.is_approved_bline_finished_set_candidate, axis=1)
    ]
    for _, product in conditional.iterrows():
        for head, flow in ((10, 0.40), (20, 0.46)):
            rows.append({
                "set_id": product["product_id"],
                "product_family": product["product_family"],
                "assembly_model": product["assembly_model"],
                "parameter_name": "flow_rate_lps",
                "value": flow,
                "unit": "l/s",
                "condition_type": "head_water_level",
                "condition_value": head,
                "condition_unit": "mm",
                "condition_label": f"{head} mm head water level",
            })
    return pd.DataFrame(rows)


def _canonical_sheets() -> dict[str, pd.DataFrame]:
    assemblies = pd.DataFrame(_assembly_rows())
    products = _products(assemblies)
    details = _details(assemblies)
    cplus_evidence = []
    for base_id, values in aco_mod.CPLUS_BASE_HYDRAULICS.items():
        for index in range(15):
            cplus_evidence.append({
                "set_id": f"{base_id}-{index}",
                "product_family": "showerdrain_cplus",
                "assembly_model": "base_x_grate",
                "base_id": base_id,
                "grate_id": f"aco-cplus-design-grate-{index}",
                "grate_article_number": f"9010.88.{index:02d}",
                "flow_rate_lps": values["flow_rate_lps"],
                "water_seal_mm": values["water_seal_mm"],
                "outlet_dn": values["outlet_dn"],
                "height_adj_min_mm": values["height_adj_min_mm"],
                "height_adj_max_mm": values["height_adj_max_mm"],
                "article_level_compatibility_found": True,
                "safe_to_generate": True,
                "ready_for_benchmark": True,
                "ready_for_customer_view": False,
            })
    bom = pd.DataFrame([
        {
            "product_id": row["set_id"],
            "component_id": row["grate_id"],
            "product_family": "showerdrain_cplus",
            "option_type": "compatible_grate",
        }
        for row in cplus_evidence
    ] + [
        {"product_id": f"other-{i}", "component_id": f"accessory-{i}",
         "product_family": "other", "option_type": "optional_accessory"}
        for i in range(221)
    ])
    raw_easyflow_articles = (
        "2500.00.00", "2500.00.77", "2500.05.00", "2500.05.77", "2500.55.00",
        "2500.55.77", "2505.00.00", "2505.00.77", "2505.05.00", "2505.05.77",
    )
    variants = pd.DataFrame([
        {
            "product_family": "easyflow",
            "variant_type": "candidate_body_variant",
            "article_number": article,
            "base_product_id": (
                "aco-easyflow-complete-dn50-ws50"
                if article in aco_mod.EASYFLOW_ARTICLES else "aco-easyflow-other-variant-scope"
            ),
            "source_url": f"https://example.test/easyflow/{article}",
            "attribution_status": "candidate_variant",
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "flow_rate_lps": 1.0 if article == "2500.05.00" else 1.5,
            "height_adj_min_mm": 7 if article == "2500.05.00" else 15,
            "height_adj_max_mm": 75 if article == "2500.05.00" else 96,
        }
        for article in raw_easyflow_articles
    ] + [
        {"product_family": "other", "variant_type": "other", "article_number": f"9999.{i:02d}.00"}
        for i in range(66)
    ])
    eplus_evidence = pd.DataFrame([
        {
            "evidence_id": f"eplus-{i}",
            "product_family": "showerdrain_eplus",
            "article_level_compatibility_found": False,
            "ready_for_customer_view": False,
            "production_status_note": "diagnostic-only evidence; no production generation change.",
        }
        for i in range(3)
    ])
    sheets: dict[str, pd.DataFrame] = {
        "Products": products,
        "Comparison": products.copy(deep=True),
        "Scoring_Field_Coverage": pd.DataFrame({"product_id": products["product_id"]}),
        "Candidates_All": pd.DataFrame({"candidate_id": range(118)}),
        "Components": pd.DataFrame({"component_id": [f"component-{i}" for i in range(100)]}),
        "BOM_Options": bom,
        "Final_Assemblies": assemblies,
        "Final_Set_Details": details,
        "Article_Variants": variants,
        "Mplus_Compound_Mappings": pd.DataFrame({"set_id": range(4)}),
        "Eplus_Proposal_Mappings": pd.DataFrame({"set_id": range(3)}),
        "Eplus_Compatible_Grate_Evidence": eplus_evidence,
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame(cplus_evidence),
        "Bline_Source_Evidence": pd.DataFrame({"evidence_id": range(8)}),
        "Conditional_Technical_Values": _conditions(products),
        "Scoring_Scenarios": pd.DataFrame({"scenario_id": ["no_scenario_selected", "flow_head_10mm", "flow_head_20mm"]}),
        "Comparison_flow_head_10mm": products.copy(deep=True),
        "Comparison_flow_head_20mm": products.copy(deep=True),
    }
    return sheets



def _write_fixture(tmp_path: Path, *, missing_blocked: str | None = None, report_updates: dict[str, object] | None = None) -> tuple[Path, Path]:
    csv_path = tmp_path / "preview.csv"
    report_path = tmp_path / "preview_report.json"
    rows: list[dict[str, str]] = []

    def add(article: str, original: str, applied: str, status: str, source: str = "tece_catalog.csv") -> None:
        rows.append({
            "article_number": article,
            "product_family": "TECEdrainline",
            "original_article_role": original,
            "applied_article_role": applied,
            "review_apply_status": status,
            "source_file": source,
        })

    # Existing non-unknown inventory roles.
    for role, count in {"accessory": 21, "complete_set": 4, "cover_or_grate": 58, "drain_body": 7}.items():
        for i in range(count):
            add(f"{role}-{i}", role, role, "not_reviewed")

    # Safe reviewed unknown rows: +19 accessory, +65 cover/grate, +49 drain body = 133.
    n = 0
    for role, count in {"accessory": 19, "cover_or_grate": 65, "drain_body": 49}.items():
        for i in range(count):
            add(f"applied-{n:03d}", "unknown", role, "applied", source=f"evidence_{role}.csv")
            n += 1

    blocked_articles = ["650700", "650800", "651500"]
    for article in blocked_articles:
        if article != missing_blocked:
            add(article, "unknown", "unknown", "skipped_blocked", source="natural_stone.csv")
    if missing_blocked:
        add("650900", "unknown", "unknown", "skipped_blocked", source="natural_stone.csv")

    for i in range(436 - len(rows)):
        rows.append({
            "article_number": f"other-{i}",
            "product_family": "OtherFamily",
            "original_article_role": "unknown",
            "applied_article_role": "unknown",
            "review_apply_status": "not_reviewed",
            "source_file": "other.csv",
        })

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    report = {
        "valid": True,
        "total_review_rows": 136,
        "safe_to_apply_true_count": 133,
        "blocked_count": 3,
        "applied_count": 133,
        "skipped_blocked_count": 3,
        "skipped_duplicate_match_count": 0,
        "unknown_reduction": 133,
        "current_inventory_role_counts": {
            "accessory": 21,
            "complete_set": 4,
            "cover_or_grate": 58,
            "drain_body": 7,
            "unknown": 136,
        },
        "preview_role_counts_after_safe_apply": {
            "accessory": 40,
            "complete_set": 4,
            "cover_or_grate": 123,
            "drain_body": 56,
            "unknown": 3,
        },
    }
    if report_updates:
        report.update(report_updates)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return csv_path, report_path



def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _set_fixture_lengths(csv_path: Path) -> None:
    with csv_path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
        fieldnames = list(rows[0])
    for col in ["nominal_length_mm", "evidence_text_snippet"]:
        if col not in fieldnames:
            fieldnames.append(col)
    for row in rows:
        row["nominal_length_mm"] = ""
        row["evidence_text_snippet"] = ""
    body_done = cover_done = accessory_done = False
    for row in rows:
        if row.get("product_family") != "TECEdrainline":
            continue
        role = row.get("applied_article_role")
        if role == "drain_body" and not body_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "body evidence 900 mm"
            body_done = True
        elif role == "cover_or_grate" and not cover_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "cover evidence 900 mm"
            cover_done = True
        elif role == "accessory" and not accessory_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "accessory evidence 900 mm"
            accessory_done = True
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _compat_path(tmp_path: Path, csv_path: Path, qa_path: Path, updates: dict[str, object] | None = None) -> Path:
    report = diag_mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)
    if updates:
        report.update(updates)
    path = tmp_path / "compat.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def test_valid_real_style_pair_audit_fixture(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is True
    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["recomputed_pair_counts_by_type"] == report["input_candidate_pair_counts_by_type"]
    assert report["recomputed_pair_counts_by_status"] == report["input_candidate_pair_counts_by_status"]
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_invalid_qa_report_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path, report_updates={"applied_count": 132})
    compat_path = tmp_path / "compat.json"
    compat_path.write_text(json.dumps({"valid": True}), encoding="utf-8")

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("QA report is invalid" in error for error in report["errors"])


def test_invalid_compatibility_report_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = tmp_path / "compat.json"
    compat_path.write_text(json.dumps({"valid": False, "errors": ["bad"]}), encoding="utf-8")

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("compatibility report is invalid" in error for error in report["errors"])


def test_recomputed_counts_mismatch_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path, {"candidate_pair_counts_by_type": {"drain_body_to_cover_or_grate": 1}})

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("counts by type differ" in error for error in report["errors"])


def test_blocked_unknown_articles_preserved(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["blocked_review_count"] > 0


def test_no_production_safe_pairs(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    pairs = report["top_high_priority_pairs_sample"] + report["blocked_review_sample"]
    assert pairs
    assert all(pair["production_safe"] is False for pair in pairs)
    assert all(pair["ready_for_benchmark"] is False for pair in pairs)
    assert all(pair["ready_for_customer_view"] is False for pair in pairs)


def test_exact_length_cover_pairs_high_and_accessory_pairs_medium(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["high_review_priority_count"] > 0
    assert report["medium_review_priority_count"] > 0
    assert report["top_high_priority_pairs_sample"][0]["pair_type"] == "drain_body_to_cover_or_grate"


def test_unresolved_length_missing_bucketed_and_complete_sets_sampled_not_paired(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["unresolved_length_missing_count"] > 0
    assert report["complete_set_rows_sample"]
    assert all(pair["pair_type"] != "complete_set" for pair in report["unresolved_length_missing_sample"])


def test_optional_csv_output_works_with_required_columns(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    csv_out = tmp_path / "audit.csv"
    out = tmp_path / "audit.json"

    exit_code = mod.main(["--preview-csv", str(csv_path), "--qa-report", str(qa_path), "--compatibility-report", str(compat_path), "--out", str(out), "--csv-out", str(csv_out), "--json"])

    assert exit_code == 0
    with csv_out.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == mod.CSV_COLUMNS
        assert next(reader)["diagnostic_only"] == "True"


def test_source_pack_immutability(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    before = {_path: _hash(_path) for _path in (csv_path, qa_path, compat_path)}
    mod.main(["--preview-csv", str(csv_path), "--qa-report", str(qa_path), "--compatibility-report", str(compat_path), "--out", str(tmp_path / "audit.json"), "--json"])
    assert {_path: _hash(_path) for _path in (csv_path, qa_path, compat_path)} == before


def test_aco_canonical_baseline_remains_pass_stable() -> None:
    report = aco_mod.audit_frames(_canonical_sheets())
    assert all(check.passed for check in report.checks)
    assert report.overall == aco_mod.STABLE
    assert "OVERALL: ACO_BASELINE_STABLE" in aco_mod.format_report(Path("canonical.xlsx"), report)
