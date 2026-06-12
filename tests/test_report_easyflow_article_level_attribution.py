from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import pandas as pd

from tools import report_easyflow_article_level_attribution as mod

BASE_ID = "aco-easyflow-complete-dn50-ws50"
ASSEMBLY_IDS = (
    f"aco-assembled-easyflow-{BASE_ID}__aco-easyflow-top-plain",
    f"aco-assembled-easyflow-{BASE_ID}__aco-easyflow-top-locking",
)
CANONICAL_COUNTS = {
    "Products": 88,
    "Comparison": 88,
    "Scoring_Field_Coverage": 88,
    "Candidates_All": 118,
    "Components": 100,
    "BOM_Options": 251,
    "Final_Assemblies": 62,
    "Final_Set_Details": 62,
    "Article_Variants": 76,
    "Mplus_Compound_Mappings": 4,
    "Eplus_Proposal_Mappings": 3,
    "Eplus_Compatible_Grate_Evidence": 3,
    "Cplus_Compatible_Grate_Evidence": 30,
    "Bline_Source_Evidence": 8,
    "Conditional_Technical_Values": 24,
    "Scoring_Scenarios": 3,
    "Comparison_flow_head_10mm": 88,
    "Comparison_flow_head_20mm": 88,
}


def _final_assemblies() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "product_id": product_id,
                "assembled_family": "easyflow",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": None,
                "height_adj_min_mm": None,
                "height_adj_max_mm": None,
                "data_quality_status": "partial",
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
            }
            for product_id in ASSEMBLY_IDS
        ]
    )


def _final_set_details() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "assembled_product_id": product_id,
                "assembled_family": "easyflow",
                "base_product_id": BASE_ID,
                "component_id": product_id.split("__", 1)[1],
                "base_article_number": "",
                "article_variant_status": "multiple_candidate_articles",
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
            }
            for product_id in ASSEMBLY_IDS
        ]
    )


def _article_variants() -> pd.DataFrame:
    rows = []
    for article, flow, height_min, height_max, side_inlet in (
        ("2500.00.00", 1.5, 15, 96, "false"),
        ("2500.05.00", 1.0, 7, 75, "true"),
        ("2500.55.00", 1.5, 15, 96, "false"),
    ):
        rows.append(
            {
                "manufacturer": "aco",
                "base_product_id": BASE_ID,
                "article_number": article,
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": f"https://example.test/easyflow/{article}/",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": flow,
                "height_adj_min_mm": height_min,
                "height_adj_max_mm": height_max,
                "side_inlet": side_inlet,
                "row_text": f"ACO source table row for article {article}",
                "attribution_status": "candidate_variant",
                "why_not_promoted": "multiple_candidate_articles",
            }
        )
    return pd.DataFrame(rows)


def _explicit_record(article: str, assembly_index: int = 0, **overrides: object) -> pd.DataFrame:
    row = {
        "assembled_product_id": ASSEMBLY_IDS[assembly_index],
        "component_id": ASSEMBLY_IDS[assembly_index].split("__", 1)[1],
        "article_number": article,
        "source_url": f"https://example.test/record/{article}",
        "source_record": f"primary table row linking {ASSEMBLY_IDS[assembly_index]} to {article}",
        "water_seal_mm": 50,
        "outlet_dn": "DN50",
        "flow_rate_lps": 1.0 if article == "2500.05.00" else 1.5,
        "height_adj_min_mm": 7 if article == "2500.05.00" else 15,
        "height_adj_max_mm": 75 if article == "2500.05.00" else 96,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_current_canonical_easyflow_state_retains_all_candidates_and_stays_blocked():
    final_assemblies = _final_assemblies()
    final_set_details = _final_set_details()
    article_variants = _article_variants()
    before = deepcopy(
        {
            "assemblies": final_assemblies.to_dict("records"),
            "details": final_set_details.to_dict("records"),
            "variants": article_variants.to_dict("records"),
        }
    )

    report = mod.assess_easyflow_article_attribution(final_assemblies, final_set_details, article_variants)

    assert report.sheet_counts["Easyflow_Assemblies_Reviewed"] == 2
    assert report.sheet_counts["Easyflow_Candidate_Articles"] == 3
    assert report.sheet_counts["Easyflow_Explicit_Unique_Matches"] == 0
    assert report.sheet_counts["Easyflow_Unique_Technical_Matches"] == 0
    assert report.sheet_counts["Easyflow_Blocked_Ambiguous_Assemblies"] == 2
    assert report.sheet_counts["Easyflow_Promotion_Review_Ready"] == 0
    assert report.overall_status == "article_level_attribution_not_resolved"
    assert {row.article_number for row in report.source_evidence_inventory} == {
        "2500.00.00", "2500.05.00", "2500.55.00"
    }
    for result in report.assembly_results:
        assert result.attribution_method == "ambiguous_multiple_article_candidates"
        assert result.candidate_article_numbers == ("2500.00.00", "2500.05.00", "2500.55.00")
        assert result.conflicting_technical_fields == (
            "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "side_inlet"
        )
        assert result.flow_rate_lps is None
        assert result.height_adj_min_mm is None
        assert result.height_adj_max_mm is None
        assert result.article_level_attribution_found is False
        assert result.ready_for_article_promotion_review is False
        assert "Values from different article rows were not combined" in result.blocking_reason

    assert final_assemblies.to_dict("records") == before["assemblies"]
    assert final_set_details.to_dict("records") == before["details"]
    assert article_variants.to_dict("records") == before["variants"]
    assert final_assemblies[list(mod.AMBIGUOUS_FIELDS)].isna().all().all()
    assert final_assemblies["data_quality_status"].eq("partial").all()
    assert not final_assemblies["ready_for_benchmark"].any()
    assert not final_assemblies["ready_for_customer_view"].any()


def test_explicit_same_record_article_match_is_ready_only_for_manual_promotion_review():
    report = mod.assess_easyflow_article_attribution(
        _final_assemblies().iloc[[0]],
        _final_set_details().iloc[[0]],
        _article_variants(),
        source_frames={"Evidence": _explicit_record("2500.05.00")},
    )

    result = report.assembly_results[0]
    assert result.attribution_method == "explicit_unique_article_match"
    assert result.explicit_article_number == "2500.05.00"
    assert result.attributed_article_number == "2500.05.00"
    assert result.attribution_confidence == "high"
    assert result.matched_source_url.endswith("2500.05.00")
    assert "primary table row" in result.matched_source_record
    assert result.matched_technical_fields == ("water_seal_mm", "outlet_dn")
    assert result.flow_rate_lps == 1.0
    assert result.height_adj_min_mm == 7
    assert result.height_adj_max_mm == 75
    assert result.ready_for_article_promotion_review is True
    assert "separate manual approval branch" in result.recommended_next_action


def test_unique_exact_technical_tuple_matches_one_same_source_row_without_production_promotion():
    assemblies = _final_assemblies().iloc[[0]].copy()
    assemblies.loc[:, "flow_rate_lps"] = 1.0
    assemblies.loc[:, "height_adj_min_mm"] = 7
    assemblies.loc[:, "height_adj_max_mm"] = 75

    result = mod.assess_easyflow_article_attribution(
        assemblies, _final_set_details().iloc[[0]], _article_variants()
    ).assembly_results[0]

    assert result.attribution_method == "unique_source_backed_technical_match"
    assert result.attributed_article_number == "2500.05.00"
    assert result.matched_technical_fields == (
        "water_seal_mm", "outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"
    )
    assert result.article_level_attribution_found is True
    assert result.ready_for_article_promotion_review is False


def test_multiple_exact_technical_matches_remain_blocked():
    assemblies = _final_assemblies().iloc[[0]].copy()
    assemblies.loc[:, "flow_rate_lps"] = 1.5
    assemblies.loc[:, "height_adj_min_mm"] = 15
    assemblies.loc[:, "height_adj_max_mm"] = 96

    result = mod.assess_easyflow_article_attribution(
        assemblies, _final_set_details().iloc[[0]], _article_variants()
    ).assembly_results[0]

    assert result.attribution_method == "ambiguous_multiple_article_candidates"
    assert result.article_level_attribution_found is False
    assert result.attributed_article_number == ""


def test_values_from_different_source_rows_cannot_be_combined_into_a_match():
    assemblies = _final_assemblies().iloc[[0]].copy()
    assemblies.loc[:, "flow_rate_lps"] = 1.0
    assemblies.loc[:, "height_adj_min_mm"] = 7
    assemblies.loc[:, "height_adj_max_mm"] = 75
    variants = _article_variants().iloc[0:0].copy()
    base = _article_variants().iloc[1].to_dict()
    flow_only = {**base, "height_adj_min_mm": None, "height_adj_max_mm": None, "row_text": "flow-only row"}
    height_only = {**base, "flow_rate_lps": None, "row_text": "height-only row"}
    variants = pd.DataFrame([flow_only, height_only])

    result = mod.assess_easyflow_article_attribution(
        assemblies, _final_set_details().iloc[[0]], variants
    ).assembly_results[0]

    assert result.attribution_method == "insufficient_article_level_evidence"
    assert result.article_level_attribution_found is False
    assert result.attributed_article_number == ""


def test_family_level_article_mentions_are_insufficient():
    family_evidence = pd.DataFrame(
        [{
            "product_family": "easyflow",
            "source_url": "https://example.test/easyflow-family",
            "source_text_or_reason": "Family page mentions 2500.05.00",
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
        }]
    )
    result = mod.assess_easyflow_article_attribution(
        _final_assemblies().iloc[[0]],
        _final_set_details().iloc[[0]],
        _article_variants(),
        source_frames={"Evidence": family_evidence},
    ).assembly_results[0]

    assert result.explicit_article_number == ""
    assert result.attribution_method == "ambiguous_multiple_article_candidates"
    assert result.ready_for_article_promotion_review is False


def test_conflicting_explicit_evidence_remains_blocked():
    evidence = pd.concat(
        [_explicit_record("2500.00.00"), _explicit_record("2500.05.00")],
        ignore_index=True,
    )
    result = mod.assess_easyflow_article_attribution(
        _final_assemblies().iloc[[0]],
        _final_set_details().iloc[[0]],
        _article_variants(),
        source_frames={"Evidence": evidence},
    ).assembly_results[0]

    assert result.attribution_method == "conflicting_article_level_evidence"
    assert result.article_level_attribution_found is False
    assert result.ready_for_article_promotion_review is False
    assert "2500.00.00, 2500.05.00" in result.explicit_article_number


def test_empty_article_identity_is_invalid_and_blocked():
    variants = _article_variants().iloc[[0]].copy()
    variants.loc[:, "article_number"] = ""
    result = mod.assess_easyflow_article_attribution(
        _final_assemblies().iloc[[0]], _final_set_details().iloc[[0]], variants
    ).assembly_results[0]

    assert result.attribution_method == "invalid_source_record"
    assert result.article_level_attribution_found is False
    assert result.candidate_article_numbers == ()


def test_report_from_workbook_is_byte_for_byte_read_only_and_preserves_counts(tmp_path: Path):
    path = tmp_path / "canonical.xlsx"
    sheets: dict[str, pd.DataFrame] = {
        name: pd.DataFrame({"row": range(count)}) for name, count in CANONICAL_COUNTS.items()
    }
    sheets["Products"] = pd.DataFrame({"product_id": [f"product-{i}" for i in range(88)]})
    sheets["Comparison"] = pd.DataFrame({"product_id": [f"product-{i}" for i in range(88)]})
    sheets["Components"] = pd.DataFrame({"component_id": [f"component-{i}" for i in range(100)]})
    sheets["BOM_Options"] = pd.DataFrame({"row": range(251)})
    non_easyflow = pd.DataFrame(
        [{"product_id": f"other-{i}", "assembled_family": "other"} for i in range(60)]
    )
    sheets["Final_Assemblies"] = pd.concat([_final_assemblies(), non_easyflow], ignore_index=True)
    other_details = pd.DataFrame(
        [{"assembled_product_id": f"other-{i}", "assembled_family": "other"} for i in range(60)]
    )
    sheets["Final_Set_Details"] = pd.concat([_final_set_details(), other_details], ignore_index=True)
    filler_variants = pd.DataFrame(
        [{"article_number": f"9999.{i:02d}.00", "variant_type": "other", "product_family": "other", "source_url": "x"} for i in range(73)]
    )
    sheets["Article_Variants"] = pd.concat([_article_variants(), filler_variants], ignore_index=True)
    sheets["Evidence"] = pd.DataFrame(columns=["assembled_product_id", "article_number", "source_url"])

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name[:31], index=False)
    before = _sha256(path)

    report = mod.report_from_workbook(path)

    assert _sha256(path) == before
    assert report.workbook_byte_unchanged is True
    assert report.sheet_counts["Products"] == 88
    assert report.sheet_counts["Comparison"] == 88
    assert report.sheet_counts["Components"] == 100
    assert report.sheet_counts["BOM_Options"] == 251
    assert report.sheet_counts["Final_Assemblies"] == 62
    assert report.sheet_counts["Final_Set_Details"] == 62
    assert report.sheet_counts["Article_Variants"] == 76
    assert report.overall_status == "article_level_attribution_not_resolved"
    assert all(not row.ready_for_article_promotion_review for row in report.assembly_results)


def test_diagnostic_frame_has_required_review_fields():
    report = mod.assess_easyflow_article_attribution(
        _final_assemblies(), _final_set_details(), _article_variants()
    )
    required = {
        "assembled_product_id", "product_family", "selected_component_id",
        "candidate_article_numbers", "explicit_article_number", "attributed_article_number",
        "attribution_method", "attribution_confidence", "matched_source_url",
        "matched_source_record", "matched_technical_fields", "conflicting_technical_fields",
        "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm",
        "article_level_attribution_found", "ready_for_article_promotion_review",
        "blocking_reason", "recommended_next_action",
    }
    assert required.issubset(report.diagnostic_frame().columns)
    assert set(report.diagnostic_frame()["attribution_method"]).issubset(mod.ATTRIBUTION_STATES)
