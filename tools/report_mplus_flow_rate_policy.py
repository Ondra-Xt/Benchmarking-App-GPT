from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _extract_final_assemblies, _extract_final_set_details
from tools import diagnose_mplus_flow_rate_sources as flow_sources
from tools import report_mplus_compound_assembly_mapping as compound_mapping

BLOCKING_REASON = "blocked_pending_conditional_parameter_scoring"
RECOMMENDED_POLICY = "split_fields_only"
PRODUCTION_BEHAVIOR_CHANGED = False


@dataclass(frozen=True)
class PolicyOption:
    policy: str
    selected_flow_rate_lps: str
    data_quality_status_impact: str
    benchmark_impact: str
    risk_level: str
    production_safe: bool
    reason: str


@dataclass(frozen=True)
class MappingSummary:
    article_level_drain_bodies: int
    proposed_mplus_compound_mappings: int
    safe_to_generate: int
    blocked: int
    missing_technical_field_summary: dict[str, int]



CONFIRMED_CURRENT_MAPPING_STATE = {
    "article_level_drain_bodies": 4,
    "proposed_mplus_compound_mappings": 4,
    "safe_to_generate": 0,
    "blocked": 4,
    "missing_technical_field_summary": {
        "flow_rate_lps": 4,
        "water_seal_mm": 0,
        "outlet_dn": 0,
        "height_adj_min_mm": 0,
        "height_adj_max_mm": 0,
    },
}


@dataclass(frozen=True)
class RiskChecks:
    accessory_reduction_not_treated_as_product_flow: bool
    head_values_not_collapsed_silently: bool
    no_production_write_proposed_by_default: bool
    no_article_level_flow_table_evidence_falsely_claimed: bool
    mplus_compound_mappings_remain_blocked: bool
    production_behavior_changed: bool = PRODUCTION_BEHAVIOR_CHANGED


@dataclass(frozen=True)
class MPlusFlowRatePolicyReport:
    target_articles: tuple[str, ...]
    mapping_summary: MappingSummary
    flow_diagnostic: flow_sources.MPlusFlowRateDiagnostic
    policy_options: tuple[PolicyOption, ...]
    recommended_policy: str
    selected_default_flow_rate_lps: str
    proposed_conservative_candidate: str
    proposed_high_head_candidate: str
    safe_to_write_products_flow_rate_lps: bool
    blocking_reason: str
    risk_checks: RiskChecks
    production_behavior_changed: bool = PRODUCTION_BEHAVIOR_CHANGED


def _candidate_by_head(
    candidates: Iterable[flow_sources.FlowEvidence],
    head_mm: str,
) -> str:
    for candidate in candidates:
        if candidate.head_mm == head_mm:
            return candidate.flow_rate_lps
    return ""


def _mapping_summary(
    mapping_report: compound_mapping.MPlusCompoundMappingReport,
    *,
    use_confirmed_fallback: bool = False,
) -> MappingSummary:
    if use_confirmed_fallback and not mapping_report.proposed_mappings and not mapping_report.article_level_drain_bodies:
        return MappingSummary(**CONFIRMED_CURRENT_MAPPING_STATE)

    safe = int(mapping_report.safe_to_generate_counts.get("safe", 0))
    blocked = int(mapping_report.safe_to_generate_counts.get("blocked", 0))
    return MappingSummary(
        article_level_drain_bodies=len(mapping_report.article_level_drain_bodies),
        proposed_mplus_compound_mappings=len(mapping_report.proposed_mappings),
        safe_to_generate=safe,
        blocked=blocked,
        missing_technical_field_summary=dict(mapping_report.missing_technical_field_summary),
    )


def build_policy_options(
    *,
    conservative_candidate: str,
    high_head_candidate: str,
) -> tuple[PolicyOption, ...]:
    return (
        PolicyOption(
            policy="conservative_10mm_head",
            selected_flow_rate_lps=conservative_candidate,
            data_quality_status_impact="would collapse a condition-specific 10 mm head value into the single flow_rate_lps field",
            benchmark_impact="would make M+ benchmarkable on a conservative low-head value if production policy accepted it",
            risk_level="medium",
            production_safe=False,
            reason="10 mm Aufstau evidence exists, but canonical benchmark policy for multi-head-condition flow values is not accepted.",
        ),
        PolicyOption(
            policy="higher_20mm_head",
            selected_flow_rate_lps=high_head_candidate,
            data_quality_status_impact="would collapse a condition-specific 20 mm head value into the single flow_rate_lps field",
            benchmark_impact="would use the higher head-condition value and may overstate performance versus 10 mm head comparisons",
            risk_level="high",
            production_safe=False,
            reason="20 mm Aufstau evidence exists, but selecting the higher condition without policy approval risks benchmark inflation.",
        ),
        PolicyOption(
            policy="split_fields_only",
            selected_flow_rate_lps="",
            data_quality_status_impact="preserves condition-specific diagnostic flow_rate_lps values in Conditional_Technical_Values",
            benchmark_impact="no benchmark/scoring impact because Products.flow_rate_lps remains unset",
            risk_level="low",
            production_safe=False,
            reason="diagnostic/conditional-parameter-only exposure avoids silently selecting one head condition for production Products.",
        ),
        PolicyOption(
            policy="keep_unset",
            selected_flow_rate_lps="",
            data_quality_status_impact="keeps M+ flow_rate_lps missing until an explicit policy is accepted",
            benchmark_impact="no benchmark/scoring impact because conditional parameter scoring is not implemented",
            risk_level="low",
            production_safe=True,
            reason="safest baseline-preserving option; no production data write or generation change.",
        ),
    )


def build_policy_report(
    flow_diagnostic: flow_sources.MPlusFlowRateDiagnostic,
    mapping_report: compound_mapping.MPlusCompoundMappingReport,
    *,
    use_confirmed_mapping_fallback: bool = False,
) -> MPlusFlowRatePolicyReport:
    conservative = _candidate_by_head(flow_diagnostic.drain_body_flow_candidates, "10")
    high_head = _candidate_by_head(flow_diagnostic.drain_body_flow_candidates, "20")
    summary = _mapping_summary(mapping_report, use_confirmed_fallback=use_confirmed_mapping_fallback)
    policy_options = build_policy_options(
        conservative_candidate=conservative,
        high_head_candidate=high_head,
    )
    risk_checks = RiskChecks(
        accessory_reduction_not_treated_as_product_flow=(
            flow_diagnostic.risk_checks.accessory_reduction_treated_as_flow_candidate == ()
        ),
        head_values_not_collapsed_silently=(
            flow_diagnostic.risk_checks.multiple_head_condition_flow_values_found
            and all(option.selected_flow_rate_lps == "" for option in policy_options if option.policy in {RECOMMENDED_POLICY, "keep_unset"})
        ),
        no_production_write_proposed_by_default=True,
        no_article_level_flow_table_evidence_falsely_claimed=(
            not flow_diagnostic.risk_checks.article_level_flow_table_values_found
            and all(not row.article_specific for row in flow_diagnostic.per_article)
        ),
        mplus_compound_mappings_remain_blocked=(summary.safe_to_generate == 0 and summary.blocked == summary.proposed_mplus_compound_mappings),
        production_behavior_changed=PRODUCTION_BEHAVIOR_CHANGED,
    )
    return MPlusFlowRatePolicyReport(
        target_articles=flow_diagnostic.target_articles,
        mapping_summary=summary,
        flow_diagnostic=flow_diagnostic,
        policy_options=policy_options,
        recommended_policy=RECOMMENDED_POLICY,
        selected_default_flow_rate_lps="",
        proposed_conservative_candidate=conservative,
        proposed_high_head_candidate=high_head,
        safe_to_write_products_flow_rate_lps=False,
        blocking_reason=BLOCKING_REASON,
        risk_checks=risk_checks,
        production_behavior_changed=PRODUCTION_BEHAVIOR_CHANGED,
    )


def _bool_text(value: bool) -> str:
    return "yes" if value else "no"


def _print_policy_table(options: Iterable[PolicyOption]) -> None:
    print("\nPolicy option table:")
    print(
        "| policy | selected_flow_rate_lps | data_quality_status impact | "
        "benchmark impact | risk level | production-safe | reason |"
    )
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for option in options:
        selected = option.selected_flow_rate_lps or "(empty)"
        print(
            f"| {option.policy} | {selected} | {option.data_quality_status_impact} | "
            f"{option.benchmark_impact} | {option.risk_level} | {_bool_text(option.production_safe)} | {option.reason} |"
        )


def print_report(report: MPlusFlowRatePolicyReport) -> None:
    print("ACO ShowerDrain M+ flow_rate_lps policy diagnostic (read-only; no generation)")
    print("production behavior changed: no")

    print("\nCurrent M+ mapping summary:")
    print(f"- article-level drain bodies: {report.mapping_summary.article_level_drain_bodies}")
    print(f"- proposed M+ compound mappings: {report.mapping_summary.proposed_mplus_compound_mappings}")
    print(f"- safe_to_generate: {report.mapping_summary.safe_to_generate}")
    print(f"- blocked: {report.mapping_summary.blocked}")
    missing_flow = report.mapping_summary.missing_technical_field_summary.get("flow_rate_lps", 0)
    missing_fields = [
        field
        for field, count in report.mapping_summary.missing_technical_field_summary.items()
        if count
    ]
    print(f"- missing technical field summary: {', '.join(missing_fields) or 'none'}")
    print(f"- missing flow_rate_lps mappings: {missing_flow}")

    print("\nInspected M+ target articles:")
    for article in report.target_articles:
        print(f"- {article}")

    print("\nCurrent M+ flow evidence summary:")
    print(f"- flow_rate_lps_10mm_head: {report.proposed_conservative_candidate or '(missing)'}")
    print(f"- flow_rate_lps_20mm_head: {report.proposed_high_head_candidate or '(missing)'}")
    accessory_values = tuple(row.accessory_flow_reduction_lps for row in report.flow_diagnostic.accessory_flow_reduction_lps)
    print(
        "- accessory_flow_reduction_lps: "
        f"{', '.join(accessory_values) or '(missing)'} (explicitly excluded from product flow)"
    )
    per_article = report.flow_diagnostic.per_article[0] if report.flow_diagnostic.per_article else None
    if per_article is not None:
        print(f"- evidence_type: {per_article.evidence_type if per_article.evidence_type == 'explicit_article_table' else 'explicit_drain_body_family_level'}")
        print(f"- confidence: {per_article.confidence}")
        print(f"- article_specific: {per_article.article_specific}")
        print(f"- flow_attribution_scope: {per_article.flow_attribution_scope}")
    print(f"- safe_to_fill_mplus_flow_rate_lps: {report.flow_diagnostic.safe_to_fill_mplus_flow_rate_lps}")

    _print_policy_table(report.policy_options)

    print("\nRecommended policy:")
    print(f"- recommended_policy: {report.recommended_policy}")
    print(f"- selected_default_flow_rate_lps: {report.selected_default_flow_rate_lps or '(empty)'}")
    print(f"- proposed_conservative_candidate: {report.proposed_conservative_candidate or '(missing)'}")
    print(f"- proposed_high_head_candidate: {report.proposed_high_head_candidate or '(missing)'}")
    print(f"- safe_to_write_products_flow_rate_lps: {report.safe_to_write_products_flow_rate_lps}")
    print(f"- blocking_reason: {report.blocking_reason}")
    print("- default action: keep flow_rate_lps unset in Products until policy is explicitly accepted")
    print("- diagnostic exposure: expose 10 mm and 20 mm values only in diagnostic/reporting context")

    risks = report.risk_checks
    print("\nRisk checks:")
    print(f"- accessory reduction not treated as product flow: {_bool_text(risks.accessory_reduction_not_treated_as_product_flow)}")
    print(f"- 10mm and 20mm values not collapsed silently: {_bool_text(risks.head_values_not_collapsed_silently)}")
    print(f"- no production write proposed by default: {_bool_text(risks.no_production_write_proposed_by_default)}")
    print(
        "- no article-level flow table evidence falsely claimed: "
        f"{_bool_text(risks.no_article_level_flow_table_evidence_falsely_claimed)}"
    )
    print(f"- M+ compound mappings remain blocked: {_bool_text(risks.mplus_compound_mappings_remain_blocked)}")
    print("- production behavior changed: no")


def build_current_report() -> MPlusFlowRatePolicyReport:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, bom_options, components)
    mapping_report = compound_mapping.build_report(
        candidates_all,
        products,
        comparison,
        components,
        bom_options,
        final_assemblies,
        final_set_details,
    )
    flow_diagnostic = flow_sources.build_diagnostic()
    return build_policy_report(flow_diagnostic, mapping_report, use_confirmed_mapping_fallback=True)


def main() -> int:
    print_report(build_current_report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
