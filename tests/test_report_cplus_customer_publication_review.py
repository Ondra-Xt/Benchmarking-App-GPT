from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from tools.report_cplus_customer_publication_review import (
    APPROVED_GRATE_ARTICLES,
    APPROVED,
    APPROVED_GATE,
    BLOCKED,
    PROTECTED_BASES,
    REQUIRED_SHEETS,
    build_cplus_customer_publication_review,
    workbook_diagnostics,
)


def _stable_sheets() -> dict[str, pd.DataFrame]:
    products: list[dict] = []
    comparison: list[dict] = []
    bom: list[dict] = []
    final: list[dict] = []
    details: list[dict] = []
    evidence: list[dict] = []

    for base_id, protected in PROTECTED_BASES.items():
        base_article = "9010.85.20,9010.85.30" if base_id.endswith("h92") else "9010.85.40,9010.85.50"
        for article in sorted(APPROVED_GRATE_ARTICLES):
            grate_id = f"aco-{article.replace('.', '')}"
            assembly_id = f"aco-assembled-showerdrain-cplus-{base_id}__{grate_id}"
            common = {
                "product_id": assembly_id,
                "assembled_family": "showerdrain_cplus",
                "product_family": "showerdrain_cplus",
                "product_name": "ACO ShowerDrain C+ stainless design grate",
                "base_id": base_id,
                "base_product_id": base_id,
                "base_article_number": base_article,
                "grate_id": grate_id,
                "grate_component_id": grate_id,
                "grate_article_number": article,
                **protected,
                "compatibility_evidence_type": "explicit_catalog_matrix",
                "compatibility_confidence": "high",
                "article_level_compatibility_found": True,
                "data_quality_status": "explicit_source_ready_production_assembly",
                "ready_for_benchmark": True,
                "ready_for_customer_view": True,
                "customer_view_enabled": True,
            }
            products.append(common.copy())
            comparison.append(common.copy())
            final.append(common.copy())
            details.append({
                **common,
                "set_id": assembly_id,
                "assembled_product_id": assembly_id,
                "component_id": grate_id,
                "component_role": "grate",
                "component_family": "showerdrain_c_article_grate",
            })
            bom.append({
                "product_id": base_id,
                "base_id": base_id,
                "component_id": grate_id,
                "product_family": "showerdrain_cplus",
                "option_type": "compatible_grate",
                "option_role": "grate",
                "component_family": "showerdrain_c_article_grate",
                "base_article_number": base_article,
                "grate_article_number": article,
                "compatibility_evidence_type": "explicit_catalog_matrix",
                "compatibility_confidence": "high",
                "article_level_compatibility_found": True,
            })
            evidence.append({
                "set_id": f"diag-cplus-{base_id}__{grate_id}",
                "product_family": "showerdrain_cplus",
                "base_id": base_id,
                "grate_id": grate_id,
                "base_article_number": base_article,
                "grate_article_number": article,
                **protected,
                "compatibility_evidence_type": "explicit_catalog_matrix",
                "compatibility_confidence": "high",
                "article_level_compatibility_found": True,
                "data_quality_status": "explicit_source_ready_diagnostic_only",
                "ready_for_benchmark": True,
                "ready_for_customer_view": False,
            })

    for index in range(26):
        row = {
            "product_id": f"aco-assembled-stable-{index}",
            "assembled_family": "showerdrain_splus",
            "ready_for_benchmark": True,
            "ready_for_customer_view": True,
            "customer_view_enabled": True,
            "data_quality_status": "complete",
        }
        final.append(row)
        details.append({**row, "set_id": row["product_id"], "assembled_product_id": row["product_id"]})
    for index in range(4):
        row = {
            "product_id": f"aco-assembled-showerdrain-mplus-{index}",
            "assembled_family": "showerdrain_mplus",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "data_quality_status": "conditional_parameter_available_production_blocked",
        }
        final.append(row)
        details.append({**row, "set_id": row["product_id"], "assembled_product_id": row["product_id"]})
    for index in range(2):
        row = {
            "product_id": f"aco-assembled-easyflow-{index}",
            "assembled_family": "easyflow",
            "is_complete_technical_data": False,
            "missing_technical_fields": "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
            "data_quality_status": "partial",
        }
        final.append(row)
        details.append({
            **row,
            "set_id": row["product_id"],
            "assembled_product_id": row["product_id"],
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
        })

    products.extend({
        "product_id": f"aco-non-cplus-{index}",
        "product_family": "other",
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "customer_view_enabled": False,
    } for index in range(50))
    comparison.extend({
        "product_id": f"aco-non-cplus-{index}",
        "product_family": "other",
    } for index in range(50))
    bom.extend({
        "product_id": f"aco-other-base-{index}",
        "product_family": "other",
        "option_type": "accessory",
    } for index in range(221))

    return {
        "Products": pd.DataFrame(products),
        "Comparison": pd.DataFrame(comparison),
        "BOM_Options": pd.DataFrame(bom),
        "Final_Assemblies": pd.DataFrame(final),
        "Final_Set_Details": pd.DataFrame(details),
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame(evidence),
    }


def test_all_30_cplus_rows_are_approved_for_publication_without_mutation():
    sheets = _stable_sheets()
    before = {name: frame.copy(deep=True) for name, frame in sheets.items()}

    review = build_cplus_customer_publication_review(sheets)
    diagnostics = workbook_diagnostics(sheets, review)

    assert diagnostics == {
        "Products": 80,
        "Comparison": 80,
        "BOM_Options": 251,
        "Final_Assemblies": 62,
        "Final_Set_Details": 62,
        "Cplus_assemblies": 30,
        "Cplus_evidence": 30,
        "Eplus_assemblies": 0,
        "Bline_assemblies": 0,
        "Mplus_blocked": True,
        "Easyflow_blocked": True,
        "review_rows": 30,
        "approved_customer_view_rows": 30,
        "blocked_rows": 0,
        "customer_flags_approved_enabled": True,
    }
    assert len(review) == 30
    assert review["classification"].eq(APPROVED).all()
    assert review["publication_gate"].eq(APPROVED_GATE).all()
    cplus_products = sheets["Products"][
        sheets["Products"]["product_id"].str.startswith("aco-assembled-showerdrain-cplus-")
    ]
    assert cplus_products["ready_for_customer_view"].eq(True).all()
    assert cplus_products["customer_view_enabled"].eq(True).all()
    assert set(review["grate_article_number"]) == APPROVED_GRATE_ARTICLES
    assert not review.astype(str).apply(lambda column: column.str.contains("Tile", case=False)).any().any()
    assert review.filter(regex=r"^check_").all(axis=None)

    for name, frame in sheets.items():
        assert_frame_equal(frame, before[name])


def test_disabled_cplus_row_is_blocked_not_auto_corrected():
    sheets = _stable_sheets()
    assembly_id = sheets["Products"].iloc[0]["product_id"]
    original = sheets["Products"].copy(deep=True)
    sheets["Products"].loc[sheets["Products"]["product_id"].eq(assembly_id), "customer_view_enabled"] = False

    review = build_cplus_customer_publication_review(sheets)
    row = review[review["assembly_id"].eq(assembly_id)].iloc[0]

    assert row["classification"] == BLOCKED
    assert row["publication_gate"] == BLOCKED
    assert "production_customer_view_enabled" in row["blocking_reasons"]
    assert sheets["Products"].loc[
        sheets["Products"]["product_id"].eq(assembly_id), "customer_view_enabled"
    ].eq(False).all()
    assert original.loc[original["product_id"].eq(assembly_id), "customer_view_enabled"].eq(True).all()


def test_cli_reads_existing_workbook_without_changing_it(tmp_path: Path):
    path = tmp_path / "benchmark_output.xlsx"
    sheets = _stable_sheets()
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name in REQUIRED_SHEETS:
            sheets[name].to_excel(writer, sheet_name=name, index=False)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    result = subprocess.run(
        [sys.executable, "tools/report_cplus_customer_publication_review.py", "--xlsx", str(path)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OVERALL: approved_for_customer_publication" in result.stdout
    assert "approved_customer_view_rows: 30" in result.stdout
    assert "blocked_rows: 0" in result.stdout
    assert "publication_gate=approved" in result.stdout
    assert "Easyflow_blocked: True" in result.stdout
    assert "customer_flags_approved_enabled: True" in result.stdout
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
