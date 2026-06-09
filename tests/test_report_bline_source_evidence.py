from pathlib import Path

import pandas as pd

from tools.report_bline_source_evidence import (
    build_export_evidence_dataframe,
    inspect_source_roots,
    workbook_inventory,
)


def test_bline_evidence_records_eight_explicit_integral_finished_set_variants():
    evidence = build_export_evidence_dataframe()

    assert len(evidence) == 8
    assert set(evidence["product_article_number"]) == {
        "9010.78.70", "9010.78.71", "9010.78.72", "9010.78.73",
        "3018172", "3018173", "3018174", "3018175",
    }
    assert evidence["compatibility_evidence_type"].eq(
        "explicit_article_level_integral_complete_set"
    ).all()
    assert evidence["article_level_compatibility_found"].eq(True).all()
    assert evidence["assembly_model"].eq("integral_all_in_one_set").all()
    assert evidence["body_article_number"].eq("").all()
    assert evidence["grate_article_number"].eq("").all()


def test_bline_evidence_preserves_conditions_and_is_diagnostic_only():
    evidence = build_export_evidence_dataframe()

    assert evidence["flow_rate_lps"].eq("").all()
    assert evidence["flow_rate_10mm_lps"].eq(0.4).all()
    assert evidence["flow_rate_20mm_lps"].eq(0.46).all()
    assert evidence["water_seal_mm"].eq(30).all()
    assert evidence["outlet_dn"].eq("DN50").all()
    assert evidence["height_adj_min_mm"].eq("").all()
    assert evidence["height_adj_max_mm"].eq("").all()
    assert evidence["installation_height_mm"].eq(80).all()
    for column in ("safe_to_generate", "ready_for_benchmark", "ready_for_customer_view"):
        assert evidence[column].eq(False).all()
    assert evidence["production_status_note"].str.contains("diagnostic/evidence-only", regex=False).all()


def test_bline_source_search_does_not_invent_separate_body_to_grate_matrix():
    sources = inspect_source_roots()

    assert not sources.empty
    product = sources[sources["source_path"].str.endswith("b_international_product.html")].iloc[0]
    assert product["explicit_integral_set_statement_found"]
    assert not product["explicit_separate_body_to_grate_matrix_found"]
    assert "9010.78.70" in product["article_numbers"]
    assert "3018175" in product["article_numbers"]


def test_workbook_inventory_is_read_only_and_counts_no_production_b_overlap(tmp_path: Path):
    path = tmp_path / "inventory.xlsx"
    evidence_ids = build_export_evidence_dataframe()["evidence_id"]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame({"product_id": ["stable-product"]}).to_excel(writer, sheet_name="Products", index=False)
        pd.DataFrame({"product_id": ["aco-b-candidate"], "product_family": ["showerdrain_b"]}).to_excel(writer, sheet_name="Candidates_All", index=False)
        pd.DataFrame({"product_id": []}).to_excel(writer, sheet_name="Components", index=False)
        pd.DataFrame({"product_id": []}).to_excel(writer, sheet_name="BOM_Options", index=False)
        pd.DataFrame({"product_id": ["aco-assembled-showerdrain-cplus-x"]}).to_excel(writer, sheet_name="Final_Assemblies", index=False)
        pd.DataFrame({"set_id": []}).to_excel(writer, sheet_name="Final_Set_Details", index=False)
        pd.DataFrame({"base_product_id": evidence_ids}).to_excel(writer, sheet_name="Article_Variants", index=False)

    before = path.read_bytes()
    inventory = workbook_inventory(path).set_index("sheet_name")
    assert inventory.loc["Products", "bline_rows"] == 0
    assert inventory.loc["Candidates_All", "bline_rows"] == 1
    assert inventory.loc["Final_Assemblies", "bline_rows"] == 0
    assert path.read_bytes() == before
