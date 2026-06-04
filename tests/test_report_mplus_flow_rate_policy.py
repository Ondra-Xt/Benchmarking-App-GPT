import pandas as pd

import tools.diagnose_mplus_flow_rate_sources as flow_mod
import tools.report_mplus_compound_assembly_mapping as mapping_mod
import tools.report_mplus_flow_rate_policy as mod


HTML = """
<main>
  <h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1>
  <p>Artikel-Nr. 9010.81.20 9010.81.21 9010.81.22 9010.81.23</p>
  <p>Abflussleistung: 0,4 l/s mit 10 mm Aufstau</p>
  <p>0,46 l/s mit 20 mm Aufstau</p>
  <p>Zubehör: Reduziert die Abflussleistung um 0,1 l/s</p>
</main>
"""


def _flow_diagnostic(monkeypatch):
    def fake_get(url, timeout=35):
        return 200, "https://example.test/mplus/drain/", HTML, ""

    monkeypatch.setattr(flow_mod.aco, "_safe_get_text", fake_get)
    return flow_mod.build_diagnostic(["https://example.test/mplus/drain/"])


def _mapping_report(*, safe=0, blocked=4):
    mappings = tuple(
        mapping_mod.ProposedCompoundMappingRow(
            channel_body_id="aco-channel",
            channel_body_article_number="9010.90.10",
            drain_body_id=f"aco-{article.replace('.', '')}",
            drain_body_article_number=article,
            grate_id="aco-grate",
            grate_article_number="9010.90.30",
            proposed_compound_set_id=f"diagnostic-mplus-{article}",
            proposed_product_name=f"ACO ShowerDrain M+ {article}",
            product_family="showerdrain_mplus",
            assembly_model=mapping_mod.ASSEMBLY_MODEL,
            source_url_channel_body="https://example.test/mplus/channel/",
            source_url_drain_body="https://example.test/mplus/drain/",
            source_url_grate="https://example.test/mplus/grate/",
            water_seal_mm="50",
            outlet_dn="DN50",
            flow_rate_lps="",
            height_adj_min_mm="25",
            height_adj_max_mm="128",
            missing_technical_fields=("flow_rate_lps",),
            mapping_confidence="medium",
            blocking_reason=mapping_mod.BLOCKING_REASON,
            safe_to_generate=False,
        )
        for article in flow_mod.TARGET_MPLUS_DRAIN_ARTICLES
    )
    return mapping_mod.MPlusCompoundMappingReport(
        sheet_counts={
            "Products": 46,
            "Comparison": 46,
            "Candidates_All": 118,
            "Components": 100,
            "BOM_Options": 221,
            "Final_Assemblies": 28,
            "Final_Set_Details": 28,
        },
        raw_candidate_counts={},
        deduplicated_candidate_counts={"article_level_drain_bodies": 4},
        channel_bodies=(),
        drain_bodies=(),
        article_level_drain_bodies=mappings,
        generic_drain_body_pages=(),
        grates=(),
        optional_accessories=(),
        proposed_mappings=mappings,
        mapping_confidence_summary={"medium": 4},
        safe_to_generate_counts={"safe": safe, "blocked": blocked},
        missing_technical_field_summary={
            "flow_rate_lps": 4,
            "water_seal_mm": 0,
            "outlet_dn": 0,
            "height_adj_min_mm": 0,
            "height_adj_max_mm": 0,
        },
        risk_checks=mapping_mod.RiskCheckSummary(missing_required_technical_fields=("flow_rate_lps",)),
    )


def test_build_policy_report_recommends_diagnostic_split_fields_without_product_write(monkeypatch):
    report = mod.build_policy_report(_flow_diagnostic(monkeypatch), _mapping_report())

    assert report.recommended_policy in {"split_fields_only", "keep_unset"}
    assert report.recommended_policy == "split_fields_only"
    assert report.selected_default_flow_rate_lps == ""
    assert report.proposed_conservative_candidate == "0.4"
    assert report.proposed_high_head_candidate == "0.46"
    assert report.safe_to_write_products_flow_rate_lps is False
    assert report.blocking_reason == "blocked_pending_conditional_parameter_scoring"
    assert report.production_behavior_changed is False
    assert report.mapping_summary.article_level_drain_bodies == 4
    assert report.mapping_summary.proposed_mplus_compound_mappings == 4
    assert report.mapping_summary.safe_to_generate == 0
    assert report.mapping_summary.blocked == 4
    assert report.mapping_summary.missing_technical_field_summary["flow_rate_lps"] == 4
    assert report.risk_checks.accessory_reduction_not_treated_as_product_flow is True
    assert report.risk_checks.head_values_not_collapsed_silently is True
    assert report.risk_checks.no_production_write_proposed_by_default is True
    assert report.risk_checks.no_article_level_flow_table_evidence_falsely_claimed is True
    assert report.risk_checks.mplus_compound_mappings_remain_blocked is True


def test_policy_options_cover_required_choices_and_risks(monkeypatch):
    report = mod.build_policy_report(_flow_diagnostic(monkeypatch), _mapping_report())
    options = {row.policy: row for row in report.policy_options}

    assert set(options) == {
        "conservative_10mm_head",
        "higher_20mm_head",
        "split_fields_only",
        "keep_unset",
    }
    assert options["conservative_10mm_head"].selected_flow_rate_lps == "0.4"
    assert options["higher_20mm_head"].selected_flow_rate_lps == "0.46"
    assert options["split_fields_only"].selected_flow_rate_lps == ""
    assert options["keep_unset"].selected_flow_rate_lps == ""
    assert options["conservative_10mm_head"].production_safe is False
    assert options["higher_20mm_head"].risk_level == "high"
    assert "no benchmark/scoring impact" in options["split_fields_only"].benchmark_impact
    assert options["keep_unset"].production_safe is True


def test_print_report_includes_required_sections_and_current_evidence(monkeypatch, capsys):
    report = mod.build_policy_report(_flow_diagnostic(monkeypatch), _mapping_report())

    mod.print_report(report)
    out = capsys.readouterr().out

    assert "Current M+ mapping summary" in out
    assert "article-level drain bodies: 4" in out
    assert "proposed M+ compound mappings: 4" in out
    assert "safe_to_generate: 0" in out
    assert "blocked: 4" in out
    assert "Current M+ flow evidence summary" in out
    assert "flow_rate_lps_10mm_head: 0.4" in out
    assert "flow_rate_lps_20mm_head: 0.46" in out
    assert "accessory_flow_reduction_lps: 0.1 (explicitly excluded from product flow)" in out
    assert "evidence_type: explicit_drain_body_family_level" in out
    assert "confidence: medium" in out
    assert "article_specific: False" in out
    assert "flow_attribution_scope: drain_body_family_level" in out
    assert "Policy option table" in out
    assert "conservative_10mm_head" in out
    assert "higher_20mm_head" in out
    assert "split_fields_only" in out
    assert "keep_unset" in out
    assert "Recommended policy" in out
    assert "recommended_policy: split_fields_only" in out
    assert "selected_default_flow_rate_lps: (empty)" in out
    assert "safe_to_write_products_flow_rate_lps: False" in out
    assert "Risk checks" in out
    assert "accessory reduction not treated as product flow: yes" in out
    assert "10mm and 20mm values not collapsed silently: yes" in out
    assert "no production write proposed by default: yes" in out
    assert "no article-level flow table evidence falsely claimed: yes" in out
    assert "M+ compound mappings remain blocked: yes" in out
    assert "production behavior changed: no" in out


def test_build_current_report_uses_diagnostics_without_changing_generation(monkeypatch):
    flow_diag = _flow_diagnostic(monkeypatch)
    mapping_report = _mapping_report()

    monkeypatch.setattr(mod.aco, "discover_candidates", lambda **kwargs: ([{"product_id": "candidate"}], {}))
    monkeypatch.setattr(
        mod.pipeline,
        "run_update",
        lambda candidates_all, config: (
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame(),
            {},
            pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(mod, "_extract_final_assemblies", lambda products: pd.DataFrame())
    monkeypatch.setattr(mod, "_extract_final_set_details", lambda final_assemblies, bom_options, components: pd.DataFrame())
    monkeypatch.setattr(mod.compound_mapping, "build_report", lambda *args: mapping_report)
    monkeypatch.setattr(mod.flow_sources, "build_diagnostic", lambda: flow_diag)

    report = mod.build_current_report()

    assert report.safe_to_write_products_flow_rate_lps is False
    assert report.mapping_summary.safe_to_generate == 0
    assert report.production_behavior_changed is False
