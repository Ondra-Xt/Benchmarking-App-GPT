from __future__ import annotations

from copy import deepcopy

import pandas as pd

from tools import report_easyflow_article_level_attribution as mod


BASE_ID = "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50"
ASSEMBLY_IDS = (
    f"aco-assembled-easyflow-{BASE_ID}__aco-easyflow-aufsatzstuecke-fuer-designroste",
    f"aco-assembled-easyflow-{BASE_ID}__aco-easyflow-grate-design-roste-design-roste",
)


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
    for article, flow, height_min, height_max in (
        ("2500.00.00", 1.5, 15, 96),
        ("2500.05.00", 1.0, 7, 75),
        ("2500.55.00", 1.5, 15, 96),
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
                "attribution_status": "candidate_variant",
                "why_not_promoted": "multiple_candidate_articles",
            }
        )
    return pd.DataFrame(rows)


def test_current_easyflow_assemblies_remain_blocked_by_three_source_backed_articles():
    final_assemblies = _final_assemblies()
    final_set_details = _final_set_details()
    article_variants = _article_variants()
    before = {
        "final_assemblies": deepcopy(final_assemblies.to_dict("records")),
        "final_set_details": deepcopy(final_set_details.to_dict("records")),
        "article_variants": deepcopy(article_variants.to_dict("records")),
    }

    report = mod.assess_easyflow_article_attribution(
        final_assemblies, final_set_details, article_variants
    )

    assert report.sheet_counts["Easyflow_Final_Assemblies"] == 2
    assert report.sheet_counts["Easyflow_Unique_Attributions"] == 0
    assert report.sheet_counts["Easyflow_Blocked_Attributions"] == 2
    assert report.unique_attribution_found_for_all is False
    for result in report.assembly_results:
        assert result.status == "multiple_source_backed_article_matches"
        assert result.unique_article_attribution_found is False
        assert result.attributed_article_number == ""
        assert result.matching_article_numbers == ("2500.00.00", "2500.05.00", "2500.55.00")
        assert result.distinct_flow_rates_lps == (1.0, 1.5)
        assert result.distinct_height_ranges_mm == ("15-96", "7-75")
        assert "selected top component" in result.blocking_reason
        assert "does not identify the drain-body article" in result.blocking_reason
        assert "keep flow_rate_lps, height_adj_min_mm, and height_adj_max_mm blocked" in result.blocking_reason

    assert final_assemblies.to_dict("records") == before["final_assemblies"]
    assert final_set_details.to_dict("records") == before["final_set_details"]
    assert article_variants.to_dict("records") == before["article_variants"]
    assert final_assemblies[list(mod.AMBIGUOUS_FIELDS)].isna().all().all()


def test_explicit_source_backed_article_reference_is_diagnostic_only_unique_proof():
    final_assemblies = _final_assemblies().iloc[[0]].copy()
    final_set_details = _final_set_details().iloc[[0]].copy()
    final_set_details.loc[:, "base_article_number"] = "2500.05.00"

    report = mod.assess_easyflow_article_attribution(
        final_assemblies, final_set_details, _article_variants()
    )

    result = report.assembly_results[0]
    assert result.status == "unique_explicit_article_reference"
    assert result.unique_article_attribution_found is True
    assert result.attributed_article_number == "2500.05.00"
    assert result.matching_article_numbers == ("2500.05.00",)
    assert result.distinct_flow_rates_lps == (1.0,)
    assert result.distinct_height_ranges_mm == ("7-75",)
    assert final_assemblies[list(mod.AMBIGUOUS_FIELDS)].isna().all().all()


def test_single_source_backed_technical_match_is_reported_without_promotion():
    final_assemblies = _final_assemblies().iloc[[0]].copy()
    final_set_details = _final_set_details().iloc[[0]].copy()
    variants = _article_variants()
    variants.loc[variants["article_number"] != "2500.55.00", "water_seal_mm"] = 30

    report = mod.assess_easyflow_article_attribution(
        final_assemblies, final_set_details, variants
    )

    result = report.assembly_results[0]
    assert result.status == "unique_source_backed_technical_match"
    assert result.unique_article_attribution_found is True
    assert result.attributed_article_number == "2500.55.00"
    assert final_assemblies[list(mod.AMBIGUOUS_FIELDS)].isna().all().all()
    assert not final_assemblies["product_id"].astype(str).str.startswith("aco-easyflow-article-").any()


def test_non_evidence_rows_cannot_be_used_for_attribution():
    final_assemblies = _final_assemblies().iloc[[0]].copy()
    final_set_details = _final_set_details().iloc[[0]].copy()
    variants = _article_variants().iloc[[0]].copy()
    variants.loc[:, "source_url"] = ""

    report = mod.assess_easyflow_article_attribution(
        final_assemblies, final_set_details, variants
    )

    result = report.assembly_results[0]
    assert result.status == "no_source_backed_article_match"
    assert result.unique_article_attribution_found is False
