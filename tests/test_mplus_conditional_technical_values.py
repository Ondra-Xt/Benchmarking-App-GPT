import pandas as pd

from src.excel_export import (
    _extract_conditional_technical_values,
    _extract_final_assemblies,
    _extract_final_set_details,
    _append_mplus_final_assembly_rows,
    _extract_mplus_compound_mappings,
)


def _mplus_mapping_rows():
    rows = []
    for article, water_seal, outlet_dn in [
        ("9010.81.20", "50", "DN40/DN50"),
        ("9010.81.21", "30", "DN40/DN50"),
        ("9010.81.22", "25", "DN40"),
        ("9010.81.23", "50", "DN50"),
    ]:
        drain_id = f"aco-{article.replace('.', '')}"
        rows.append(
            {
                "set_id": f"diagnostic-mplus-channel-body-25-128__{drain_id}__mplus-design-roste-elektropoliert",
                "product_family": "showerdrain_mplus",
                "assembly_model": "channel_body_x_drain_body_x_grate",
                "channel_body_id": "channel-body-25-128",
                "channel_body_article_number": "",
                "drain_body_id": drain_id,
                "drain_body_article_number": article,
                "grate_id": "mplus-design-roste-elektropoliert",
                "grate_article_number": "",
                "source_url_channel_body": "https://example.test/mplus/channel",
                "source_url_drain_body": "https://example.test/mplus/drain",
                "source_url_grate": "https://example.test/mplus/grate",
                "water_seal_mm": water_seal,
                "outlet_dn": outlet_dn,
                "height_adj_min_mm": "25",
                "height_adj_max_mm": "128",
                "flow_rate_lps": "",
                "flow_rate_lps_10mm_head": 0.4,
                "flow_rate_lps_20mm_head": 0.46,
                "selected_default_flow_rate_lps": "",
                "accessory_flow_reduction_lps": 0.1,
                "flow_policy": "split_fields_only",
                "flow_evidence_type": "explicit_drain_body_family_level",
                "flow_confidence": "medium",
                "flow_article_specific": False,
                "flow_attribution_scope": "drain_body_family_level",
                "missing_technical_fields": "flow_rate_lps",
                "data_quality_status": "partial",
                "safe_to_generate": False,
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocking_reason": "blocked_pending_conditional_parameter_scoring",
                "recommended_next_action": "implement scoring/export handling for conditional parameter values before production M+ assemblies",
                "production_status_note": "diagnostic/conditional-parameter-only; conditional flow data available; no Products/BOM/assembly generation change",
            }
        )
    return pd.DataFrame(rows)


def test_each_mplus_mapping_gets_two_condition_specific_flow_rows():
    mappings = _mplus_mapping_rows()
    conditional = _extract_conditional_technical_values(mappings)

    assert len(mappings) == 4
    assert len(conditional) == 8
    assert conditional.groupby("set_id").size().to_dict() == {set_id: 2 for set_id in mappings["set_id"]}

    for set_id, rows in conditional.groupby("set_id"):
        assert set_id in set(mappings["set_id"])
        assert set(rows["parameter_name"]) == {"flow_rate_lps"}
        assert set(rows["unit"]) == {"l/s"}
        assert set(rows["condition_type"]) == {"head_water_level"}
        assert set(rows["condition_unit"]) == {"mm"}
        variants = {
            (int(row.condition_value), round(float(row.value), 2), row.condition_label)
            for row in rows.itertuples(index=False)
        }
        assert variants == {
            (10, 0.4, "10 mm head water level"),
            (20, 0.46, "20 mm head water level"),
        }


def test_conditional_rows_preserve_mapping_links_evidence_and_blocking_status():
    mappings = _mplus_mapping_rows()
    conditional = _extract_conditional_technical_values(mappings)

    mapping_by_set_id = mappings.set_index("set_id")
    for row in conditional.itertuples(index=False):
        mapping = mapping_by_set_id.loc[row.set_id]
        assert row.product_family == mapping["product_family"]
        assert row.assembly_model == mapping["assembly_model"]
        assert row.channel_body_id == mapping["channel_body_id"]
        assert row.drain_body_id == mapping["drain_body_id"]
        assert row.grate_id == mapping["grate_id"]
        assert row.source_url_channel_body == mapping["source_url_channel_body"]
        assert row.source_url_drain_body == mapping["source_url_drain_body"]
        assert row.source_url_grate == mapping["source_url_grate"]
        assert row.evidence_type == mapping["flow_evidence_type"]
        assert row.confidence == mapping["flow_confidence"]
        assert row.attribution_scope == mapping["flow_attribution_scope"]
        assert row.article_specific is False
        assert row.safe_to_generate is False
        assert row.ready_for_benchmark is False
        assert row.ready_for_customer_view is False
        assert row.blocking_reason == "blocked_pending_conditional_parameter_scoring"
        assert "implement scoring/export handling for conditional parameter values" in row.recommended_next_action
        assert "conditional flow values available in Conditional_Technical_Values" in row.production_status_note


def test_mplus_final_assembly_rows_are_generated_but_blocked():
    mappings = _mplus_mapping_rows()
    products, comparison = _append_mplus_final_assembly_rows(pd.DataFrame(), pd.DataFrame(), mappings)
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, pd.DataFrame(), pd.DataFrame())
    conditional = _extract_conditional_technical_values(mappings)

    assert len(products) == 4
    assert len(comparison) == 4
    assert len(final_assemblies) == 4
    assert len(final_set_details) == 4
    assert products["product_id"].str.startswith("aco-assembled-showerdrain-mplus-").all()
    assert final_assemblies["assembled_family"].eq("showerdrain_mplus").all()

    for field in ["channel_body_id", "drain_body_id", "grate_id", "source_url_channel_body", "source_url_drain_body", "source_url_grate"]:
        assert products[field].fillna("").ne("").all()
        assert final_assemblies[field].fillna("").ne("").all()

    assert products["flow_rate_lps"].fillna("").eq("").all()
    assert comparison["flow_rate_lps"].fillna("").eq("").all()
    assert final_assemblies["flow_rate_lps"].fillna("").eq("").all()
    assert products["flow_rate_status"].eq("conditional").all()
    assert final_assemblies["flow_rate_status"].eq("conditional").all()
    assert products["ready_for_benchmark"].eq(False).all()
    assert products["ready_for_customer_view"].eq(False).all()
    assert final_set_details["ready_for_benchmark"].eq(False).all()
    assert final_set_details["ready_for_customer_view"].eq(False).all()
    assert final_set_details["blocked_reason"].eq("blocked_pending_conditional_parameter_scoring").all()
    assert set(conditional["set_id"]) == set(mappings["set_id"])


def test_mplus_default_flow_stays_empty_while_final_rows_are_blocked():
    mappings = _mplus_mapping_rows()
    products = pd.DataFrame(
        [
            {
                "manufacturer": "aco",
                "product_id": "aco-showerdrain-cplus-standard-h92",
                "product_family": "showerdrain_cplus",
                "flow_rate_lps": 0.91,
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "height_adj_min_mm": 80,
                "height_adj_max_mm": 128,
            }
        ]
    )
    bom = pd.DataFrame(columns=["product_id", "component_id"])
    components = pd.DataFrame(columns=["product_id"])

    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, bom, components)
    conditional = _extract_conditional_technical_values(mappings)

    assert (mappings["selected_default_flow_rate_lps"].fillna("") == "").all()
    assert (mappings["flow_rate_lps"].fillna("") == "").all()
    assert (mappings["selected_default_flow_rate_lps"].fillna("") == "").all()
    assert (mappings["flow_rate_lps"].fillna("") == "").all()
    assert not final_assemblies["product_id"].fillna("").str.contains("showerdrain-mplus").any()
    assert final_set_details.empty or not final_set_details["assembled_product_id"].fillna("").str.contains("showerdrain-mplus").any()
    assert not set(conditional["set_id"]) & set(products["product_id"])


def test_extracted_mplus_mappings_remain_four_proposal_only_rows():
    products = pd.DataFrame()
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, pd.DataFrame(), pd.DataFrame())

    mappings = _extract_mplus_compound_mappings(
        pd.DataFrame(),
        products,
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        final_assemblies,
        final_set_details,
    )
    conditional = _extract_conditional_technical_values(mappings)

    assert len(mappings) == 4
    assert len(conditional) == 8
    assert (mappings["selected_default_flow_rate_lps"].fillna("") == "").all()
    assert (mappings["flow_rate_lps"].fillna("") == "").all()
    assert set(pd.to_numeric(conditional["condition_value"])) == {10, 20}
    assert set(pd.to_numeric(conditional["value"]).round(2)) == {0.4, 0.46}
