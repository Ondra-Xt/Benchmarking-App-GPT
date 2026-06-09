from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from tools.report_customer_view_readiness import (
    ASSEMBLY_SHEETS,
    build_customer_view_readiness_report,
    load_report_sheets,
)


def _stable_sheets() -> dict[str, pd.DataFrame]:
    family_counts = {
        "easyflow": 2,
        "easyflowplus": 6,
        "showerdrain_c": 4,
        "showerdrain_splus": 16,
        "showerdrain_mplus": 4,
        "showerdrain_cplus": 30,
    }
    rows = []
    for family, count in family_counts.items():
        for index in range(count):
            product_id = f"aco-assembled-{family.replace('_', '-')}-{index}__component-{index}"
            customer_ready = family in {
                "easyflowplus", "showerdrain_c", "showerdrain_splus"
            }
            rows.append({
                "product_id": product_id,
                "assembled_product_id": product_id,
                "set_id": product_id,
                "assembled_family": family,
                "ready_for_benchmark": family != "easyflow" and family != "showerdrain_mplus",
                "ready_for_customer_view": customer_ready,
                "customer_view_enabled": customer_ready,
                "is_complete_technical_data": family != "easyflow" and family != "showerdrain_mplus",
                "missing_technical_fields": (
                    "flow_rate_lps,height_adj_min_mm,height_adj_max_mm"
                    if family == "easyflow"
                    else "flow_rate_lps" if family == "showerdrain_mplus" else ""
                ),
                "data_quality_status": (
                    "partial" if family in {"easyflow", "showerdrain_mplus"} else "complete"
                ),
            })

    final_assemblies = pd.DataFrame(rows)
    final_details = final_assemblies.rename(columns={"product_id": "detail_product_id"}).copy()
    products = final_assemblies[
        [
            "product_id", "ready_for_benchmark", "ready_for_customer_view",
            "customer_view_enabled",
        ]
    ].copy()
    products = pd.concat([
        products,
        pd.DataFrame({
            "product_id": [f"aco-non-assembled-{index}" for index in range(18)],
            "ready_for_benchmark": [False] * 18,
            "ready_for_customer_view": [False] * 18,
            "customer_view_enabled": [False] * 18,
        }),
    ], ignore_index=True)

    mplus_ids = final_assemblies.loc[
        final_assemblies["assembled_family"].eq("showerdrain_mplus"), "set_id"
    ]
    conditional = pd.DataFrame([
        {"set_id": set_id, "parameter_name": "flow_rate_lps", "condition_value": head}
        for set_id in mplus_ids
        for head in (10, 20)
    ])
    return {
        "Products": products,
        "Comparison": pd.DataFrame({"product_id": products["product_id"]}),
        "BOM_Options": pd.DataFrame({"option_id": range(251)}),
        "Final_Assemblies": final_assemblies,
        "Final_Set_Details": final_details,
        "Article_Variants": pd.DataFrame({"article_number": range(76)}),
        "Conditional_Technical_Values": conditional,
        "Eplus_Compatible_Grate_Evidence": pd.DataFrame({
            "set_id": [f"diagnostic-eplus-{index}" for index in range(3)]
        }),
        "Bline_Source_Evidence": pd.DataFrame({
            "article_number": [f"b-finished-set-{index}" for index in range(8)]
        }),
    }


def test_stable_baseline_and_customer_view_policy_are_classified_without_mutation():
    sheets = _stable_sheets()
    before = {name: frame.copy(deep=True) for name, frame in sheets.items()}

    report = build_customer_view_readiness_report(sheets)

    assert len(sheets["Products"]) == 80
    assert len(sheets["Comparison"]) == 80
    assert len(sheets["BOM_Options"]) == 251
    assert len(sheets["Final_Assemblies"]) == 62
    assert len(sheets["Final_Set_Details"]) == 62
    assert len(sheets["Article_Variants"]) == 76

    for source_sheet in ASSEMBLY_SHEETS:
        source = report[report["source_sheet"].eq(source_sheet)]
        assert len(source) == 62
        counts = source.groupby("category", observed=True).size().to_dict()
        assert counts == {
            "customer_view_ready": 26,
            "benchmark_ready_but_customer_disabled": 30,
            "blocked_partial": 2,
            "blocked_conditional": 4,
        }
        family_counts = source.groupby("assembled_family").size().to_dict()
        assert family_counts["showerdrain_cplus"] == 30
        assert family_counts["showerdrain_mplus"] == 4
        assert family_counts.get("showerdrain_eplus", 0) == 0
        assert family_counts.get("showerdrain_b", 0) == 0

    final_rows = report[report["source_sheet"].eq("Final_Assemblies")]
    cplus = final_rows[final_rows["assembled_family"].eq("showerdrain_cplus")]
    assert cplus["ready_for_benchmark"].eq(True).all()
    assert cplus["ready_for_customer_view"].eq(False).all()
    assert cplus["customer_view_enabled"].eq(False).all()
    assert cplus["category"].eq("benchmark_ready_but_customer_disabled").all()

    mplus = final_rows[final_rows["assembled_family"].eq("showerdrain_mplus")]
    easyflow = final_rows[final_rows["assembled_family"].eq("easyflow")]
    assert mplus["category"].eq("blocked_conditional").all()
    assert easyflow["category"].eq("blocked_partial").all()

    eplus = report[report["assembled_family"].eq("showerdrain_eplus")]
    bline = report[report["assembled_family"].eq("showerdrain_b")]
    assert len(eplus) == 3
    assert len(bline) == 8
    assert eplus["category"].eq("diagnostic_only_not_product").all()
    assert bline["category"].eq("diagnostic_only_not_product").all()
    assert not set(eplus["record_id"]) & set(sheets["Products"]["product_id"])
    assert not set(bline["record_id"]) & set(sheets["Products"]["product_id"])

    for name, expected in before.items():
        assert_frame_equal(sheets[name], expected)


def test_existing_workbook_report_is_read_only_and_cli_prints_actions(tmp_path):
    sheets = _stable_sheets()
    workbook = tmp_path / "benchmark_output.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    before_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()

    report = build_customer_view_readiness_report(load_report_sheets(workbook))
    after_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()

    assert before_hash == after_hash
    assert len(report) == 62 + 62 + 3 + 8

    result = subprocess.run(
        [sys.executable, "tools/report_customer_view_readiness.py", "--xlsx", str(workbook)],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Mode: read-only policy evaluation" in result.stdout
    assert "showerdrain_cplus: benchmark_ready_but_customer_disabled=30" in result.stdout
    assert "showerdrain_mplus: blocked_conditional=4" in result.stdout
    assert "showerdrain_eplus: 3 evidence rows" in result.stdout
    assert "showerdrain_b: 8 evidence rows" in result.stdout
    assert "do not auto-enable C+" in result.stdout
    assert before_hash == hashlib.sha256(workbook.read_bytes()).hexdigest()
