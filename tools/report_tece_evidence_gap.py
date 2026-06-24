from __future__ import annotations

import argparse
import io
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import SOURCE_PACK_TECHNICAL_FIELDS, TeceSourcePackReport, load_source_pack
from tools.tece_report_output import write_json_output, write_text_output
from tools.report_tece_compatibility_diagnostics import build_compatibility_diagnostics_report

RECOMMENDED_NEXT_ACTIONS = [
    "Replace synthetic fixtures with real TECE public/approved source files.",
    "Add article-level TECE channel/body product data.",
    "Add article-level cover/grate product data.",
    "Add explicit cover/grate compatibility matrix.",
    "Add technical datasheets for each production article or complete set.",
    "Preserve all conditional technical values with their conditions.",
    "Run validator, inventory report, classification report, and gap report again.",
]


@dataclass(frozen=True)
class TeceEvidenceGapReport:
    source_pack_path: str
    source_pack_file_count: int
    source_pack_candidate_count: int
    article_numbers: list[str]
    family_counts: dict[str, int]
    role_counts: dict[str, int]
    evidence_scope_counts: dict[str, int]
    page_range_label_counts: dict[str, int]
    technical_field_coverage: dict[str, int]
    missing_field_counts: dict[str, int]
    cover_grate_matrix_evidence_exists: bool
    assembly_matrix_evidence_exists: bool
    article_level_compatibility_evidence_exists: bool
    compatibility_diagnostic_available: bool
    compatibility_candidate_count: int
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    overall_status: str
    gap_summary: dict[str, bool]
    recommended_next_actions: list[str]


def _manifest_sources(report: TeceSourcePackReport) -> list[dict[str, Any]]:
    if not report.manifest:
        return []
    return [source for source in report.manifest.get("sources", []) if isinstance(source, dict)]


def _source_is_synthetic(source: dict[str, Any]) -> bool:
    haystack = " ".join(str(source.get(key, "")) for key in ("source_url", "document_title", "notes", "source_origin")).lower()
    source_file = str(source.get("source_file", "")).lower()
    marked_synthetic = "synthetic" in haystack or "test fixture" in haystack or "example.invalid" in haystack
    real_pdf = source_file.endswith(".pdf") and not marked_synthetic
    # approved_for_benchmark_evidence=False means not approved evidence, not synthetic.
    return marked_synthetic and not real_pdf


def _synthetic_fixture_only(report: TeceSourcePackReport) -> bool:
    sources = _manifest_sources(report)
    if sources:
        return bool(sources) and all(_source_is_synthetic(source) for source in sources)
    return bool(report.rows) and all("synthetic_test_fixture_only" in row.production_blocking_reason for row in report.rows)


def _has_complete_technical_data(report: TeceSourcePackReport) -> bool:
    if not report.rows:
        return False
    return all(count == 0 for count in report.missing_field_counts.values())


def build_evidence_gap_report(source_pack: str | Path) -> TeceEvidenceGapReport:
    report = load_source_pack(source_pack)
    summary = report.source_pack_classification_summary or {}
    role_counts = dict(summary.get("role_counts", {}))
    family_counts = dict(summary.get("family_counts", {}))
    synthetic_only = _synthetic_fixture_only(report)

    has_channel_or_body = any(role_counts.get(role, 0) > 0 for role in ("channel_body", "drain_body", "complete_set"))
    has_cover_or_grate = role_counts.get("cover_or_grate", 0) > 0
    real_cover_matrix = report.cover_grate_matrix_evidence_exists and not synthetic_only
    real_assembly_matrix = report.assembly_matrix_evidence_exists and not synthetic_only
    real_article_compat = report.article_level_compatibility_evidence_exists and not synthetic_only
    complete_technical = _has_complete_technical_data(report)
    compatibility_diagnostics = build_compatibility_diagnostics_report(source_pack)
    approved_missing = not any(source.get("approved_for_benchmark_evidence") is True for source in _manifest_sources(report))

    gap_summary = {
        "missing_article_level_channel_or_body_data": not has_channel_or_body or synthetic_only,
        "missing_article_level_cover_or_grate_data": not has_cover_or_grate or synthetic_only,
        "missing_explicit_cover_grate_compatibility_matrix": not real_cover_matrix,
        "missing_explicit_assembly_matrix": not real_assembly_matrix,
        "missing_complete_technical_datasheets": not complete_technical,
        "missing_conditional_value_policy_review": True,
        "synthetic_fixture_only": synthetic_only,
        "approved_benchmark_evidence_missing": approved_missing,
    }

    if synthetic_only:
        overall = "OVERALL: TECE_EVIDENCE_GAP_SYNTHETIC_ONLY"
    elif gap_summary["missing_explicit_cover_grate_compatibility_matrix"]:
        overall = "OVERALL: TECE_EVIDENCE_GAP_COMPATIBILITY_BLOCKED"
    elif gap_summary["missing_complete_technical_datasheets"]:
        overall = "OVERALL: TECE_EVIDENCE_GAP_TECHNICAL_DATA_INCOMPLETE"
    else:
        overall = "OVERALL: TECE_EVIDENCE_GAP_TECHNICAL_DATA_INCOMPLETE"

    return TeceEvidenceGapReport(
        source_pack_path=report.source_pack_path,
        source_pack_file_count=report.source_pack_file_count,
        source_pack_candidate_count=report.source_pack_candidate_count,
        article_numbers=report.article_numbers,
        family_counts=family_counts,
        role_counts=role_counts,
        evidence_scope_counts=report.evidence_scope_counts or {},
        page_range_label_counts=report.page_range_label_counts or {},
        technical_field_coverage=report.technical_field_coverage,
        missing_field_counts=report.missing_field_counts,
        cover_grate_matrix_evidence_exists=real_cover_matrix,
        assembly_matrix_evidence_exists=real_assembly_matrix,
        article_level_compatibility_evidence_exists=real_article_compat and compatibility_diagnostics.explicit_article_level_compatibility_evidence_exists,
        compatibility_diagnostic_available=compatibility_diagnostics.compatibility_diagnostic_available,
        compatibility_candidate_count=compatibility_diagnostics.compatibility_candidate_count,
        production_promotion_blocked=True,
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        overall_status=overall,
        gap_summary=gap_summary,
        recommended_next_actions=RECOMMENDED_NEXT_ACTIONS,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnostic-only TECE evidence-gap report.")
    parser.add_argument("--source-pack", required=True, help="Path to local TECE source-pack files.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", help="Write report output to this UTF-8 path instead of stdout.")
    args = parser.parse_args(argv)

    report = build_evidence_gap_report(args.source_pack)
    payload = asdict(report)
    if args.json:
        write_json_output(payload, args.out)
    else:
        stream = io.StringIO()
        print("TECE evidence-gap report (diagnostic-only; production promotion blocked)", file=stream)
        for key, value in payload.items():
            if key in {"gap_summary", "recommended_next_actions"}:
                print(f"{key}:", file=stream)
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        print(f"  {sub_key}: {sub_value}", file=stream)
                else:
                    for index, action in enumerate(value, 1):
                        print(f"  {index}. {action}", file=stream)
            elif key != "overall_status":
                print(f"{key}: {json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value}", file=stream)
        print(report.overall_status, file=stream)
        write_text_output(stream.getvalue().rstrip("\n"), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
