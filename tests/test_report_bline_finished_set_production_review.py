from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from tools.report_bline_finished_set_production_review import (
    BLOCKED,
    ELIGIBLE,
    EXPECTED_ARTICLES,
    EXPECTED_SHEET_COUNTS,
    APPROVED,
    build_bline_finished_set_production_review,
    workbook_diagnostics,
)
from tools.report_bline_source_evidence import build_export_evidence_dataframe
from src.excel_export import _append_bline_direct_finished_set_rows


def _rows(count: int, **values: object) -> pd.DataFrame:
    return pd.DataFrame([{**values, "row_id": index} for index in range(count)])


def _stable_sheets() -> dict[str, pd.DataFrame]:
    sheets = {
        name: _rows(count, product_family="other")
        for name, count in EXPECTED_SHEET_COUNTS.items()
    }
    sheets["Bline_Source_Evidence"] = build_export_evidence_dataframe()
    sheets["Products"] = _rows(80, product_family="other", ready_for_customer_view=False)
    sheets["Comparison"] = _rows(80, product_family="other")
    # Canonical discovery may retain one family-level B catalog row in these sheets.
    # It is not a promoted direct finished-set product and must not be counted as one.
    sheets["Products"].loc[0, ["product_family", "product_id", "article_number"]] = [
        "showerdrain_b", "aco-showerdrain-b-family", "9010.78.70"
    ]
    sheets["Comparison"].loc[0, ["product_family", "product_id", "article_number"]] = [
        "showerdrain_b", "aco-showerdrain-b-family", "9010.78.70"
    ]
    sheets["Products"], sheets["Comparison"] = _append_bline_direct_finished_set_rows(
        sheets["Products"], sheets["Comparison"], sheets["Bline_Source_Evidence"]
    )
    sheets["BOM_Options"] = _rows(251, product_family="other", option_family="other")

    final_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []
    family_counts = {
        "easyflowplus": 6,
        "showerdrain_c": 4,
        "showerdrain_splus": 16,
        "showerdrain_cplus": 30,
        "showerdrain_mplus": 4,
        "easyflow": 2,
    }
    for family, count in family_counts.items():
        for index in range(count):
            product_id = f"aco-assembled-{family.replace('_', '-')}-{index}"
            customer_ready = family not in {"showerdrain_mplus", "easyflow"}
            status = {
                "showerdrain_cplus": "explicit_source_ready_production_assembly",
                "showerdrain_mplus": "conditional_parameter_available_production_blocked",
                "easyflow": "partial",
            }.get(family, "complete")
            row = {
                "product_id": product_id,
                "assembled_product_id": product_id,
                "set_id": product_id,
                "assembled_family": family,
                "product_family": family,
                "ready_for_benchmark": family != "showerdrain_mplus" and family != "easyflow",
                "ready_for_customer_view": customer_ready,
                "data_quality_status": status,
                "is_complete_technical_data": family != "easyflow",
                "missing_technical_fields": (
                    "flow_rate_lps,height_adj_min_mm,height_adj_max_mm"
                    if family == "easyflow" else ""
                ),
            }
            final_rows.append(row.copy())
            detail_rows.append(row.copy())
    sheets["Final_Assemblies"] = pd.DataFrame(final_rows)
    sheets["Final_Set_Details"] = pd.DataFrame(detail_rows)
    sheets["Eplus_Compatible_Grate_Evidence"] = _rows(
        3,
        product_family="showerdrain_eplus",
        ready_for_benchmark=False,
        ready_for_customer_view=False,
    )
    sheets["Cplus_Compatible_Grate_Evidence"] = _rows(
        30, product_family="showerdrain_cplus"
    )
    return sheets


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_all_eight_integral_articles_are_approved_direct_products() -> None:
    sheets = _stable_sheets()
    originals = {name: frame.copy(deep=True) for name, frame in sheets.items()}

    review = build_bline_finished_set_production_review(sheets)
    diagnostics = workbook_diagnostics(sheets, review)

    assert len(review) == 8
    assert set(review["product_article_number"]) == EXPECTED_ARTICLES
    assert review["classification"].eq(ELIGIBLE).all()
    assert review["production_gate"].eq(APPROVED).all()
    assert review["blocking_reasons"].eq("").all()
    assert review["body_article_number_empty"].all()
    assert review["grate_article_number_empty"].all()
    assert review["integral_finished_set_model"].all()
    assert review["unconditional_flow_empty"].all()
    assert review["flow_rate_10mm_is_0_40"].all()
    assert review["flow_rate_20mm_is_0_46"].all()
    assert review["height_adjustment_min_empty"].all()
    assert review["height_adjustment_max_empty"].all()
    assert not review.columns.str.contains("no_bline_production_rows").any()

    assert {name: diagnostics[name] for name in EXPECTED_SHEET_COUNTS} == EXPECTED_SHEET_COUNTS
    assert diagnostics["Bline_Products"] == 8
    assert diagnostics["Bline_Comparison"] == 8
    assert diagnostics["Bline_BOM_Options"] == 0
    assert diagnostics["Bline_Final_Assemblies"] == 0
    assert diagnostics["Bline_Final_Set_Details"] == 0
    assert diagnostics["Final_Set_Details_ready_for_customer_view_true"] == 56
    assert diagnostics["Cplus_customer_approved_state_unchanged"] is True
    assert diagnostics["Mplus_blocked"] is True
    assert diagnostics["Easyflow_blocked"] is True
    assert diagnostics["Eplus_diagnostic_only"] is True
    assert diagnostics["review_rows"] == 8
    assert diagnostics["eligible_review_rows"] == 8
    assert diagnostics["blocked_review_rows"] == 0

    for name, original in originals.items():
        assert_frame_equal(sheets[name], original)


def test_invalid_or_base_x_grate_evidence_is_blocked() -> None:
    sheets = _stable_sheets()
    evidence = sheets["Bline_Source_Evidence"].copy()
    evidence.loc[0, "assembly_model"] = "base_x_grate"
    evidence.loc[0, "body_article_number"] = "body-article"
    evidence.loc[0, "grate_article_number"] = "grate-article"
    evidence.loc[0, "flow_rate_lps"] = "0.46"
    sheets["Bline_Source_Evidence"] = evidence

    review = build_bline_finished_set_production_review(sheets)
    row = review.loc[review["product_article_number"].eq("9010.78.70")].iloc[0]

    assert row["classification"] == BLOCKED
    assert row["production_gate"] == BLOCKED
    assert "integral_finished_set_model" in row["blocking_reasons"]
    assert "body_article_number_empty" in row["blocking_reasons"]
    assert "grate_article_number_empty" in row["blocking_reasons"]
    assert "unconditional_flow_empty" in row["blocking_reasons"]


def test_existing_xlsx_is_not_mutated_and_cli_reports_approved_gate(tmp_path: Path) -> None:
    workbook = tmp_path / "benchmark_output.xlsx"
    _write_workbook(workbook, _stable_sheets())
    before = _sha256(workbook)

    result = subprocess.run(
        [
            sys.executable,
            "tools/report_bline_finished_set_production_review.py",
            "--xlsx",
            str(workbook),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _sha256(workbook) == before
    assert "Mode: read-only diagnostic" in result.stdout
    assert "base_x_grate is prohibited" in result.stdout
    assert f"OVERALL: {APPROVED}" in result.stdout
    assert f"Production gate: {APPROVED}" in result.stdout
    assert "Retain direct finished-set product modelling" in result.stdout
    assert "- Bline_Products: 8" in result.stdout
    assert "- Bline_Comparison: 8" in result.stdout
    assert "- Bline_BOM_Options: 0" in result.stdout
    assert "- Bline_Final_Assemblies: 0" in result.stdout
    assert "- Bline_Final_Set_Details: 0" in result.stdout
    assert "- Easyflow_blocked: True" in result.stdout
    assert "- review_rows: 8" in result.stdout
    assert "- eligible_review_rows: 8" in result.stdout
    assert "- blocked_review_rows: 0" in result.stdout
    assert result.stdout.count(f"production_gate={APPROVED}") == 8
