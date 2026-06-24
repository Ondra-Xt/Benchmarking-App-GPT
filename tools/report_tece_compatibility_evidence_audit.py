from __future__ import annotations

import argparse
import io
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_compatibility_diagnostics import build_compatibility_diagnostics_report
from tools.tece_report_output import write_json_output, write_text_output


def _sample(pairing: dict[str, Any]) -> dict[str, Any]:
    return {
        "body_channel_drain_article_number": pairing.get("body_or_drain_article"),
        "cover_grate_plate_article_number": pairing.get("cover_grate_plate_article"),
        "nominal_length_mm": pairing.get("nominal_length_mm"),
        "evidence_level": pairing.get("evidence_level") or pairing.get("evidence_type"),
        "evidence_confidence": pairing.get("evidence_confidence"),
        "evidence_text_or_reason": pairing.get("evidence_text_or_reason"),
        "source_file": pairing.get("source_file"),
        "source_page_start": pairing.get("source_page_start"),
        "source_page_end": pairing.get("source_page_end"),
        "page_range_label": pairing.get("page_range_label"),
        "why_not_production_safe": pairing.get("why_not_production_safe"),
        "requires_manual_review": pairing.get("requires_manual_review"),
    }


def build_audit_report(source_pack: str | Path, max_examples_per_family: int = 20) -> dict[str, Any]:
    diagnostics = build_compatibility_diagnostics_report(source_pack)
    payload = asdict(diagnostics)
    pairings = [pairing for family in payload["families"] for pairing in family["possible_pairings"]]
    excluded_pairings = [pairing for family in payload["families"] for pairing in family.get("excluded_pairings", [])]

    role_pair_counts = Counter(pairing.get("role_pair", "unknown_to_unknown") for pairing in pairings)
    page_range_counts = Counter(pairing.get("page_range_label") or "(none)" for pairing in pairings + excluded_pairings)
    explicit_text_count = payload["evidence_level_counts"].get("explicit_text_pairing", 0)

    examples: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    excluded_examples: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for pairing in pairings:
        family = pairing.get("family") or "unknown"
        level = pairing.get("evidence_level") or pairing.get("evidence_type") or "unknown"
        if len(examples[family][level]) < max_examples_per_family:
            examples[family][level].append(_sample(pairing))
    for pairing in excluded_pairings:
        family = pairing.get("family") or "unknown"
        reason = pairing.get("excluded_reason") or "unknown"
        if len(excluded_examples[family][reason]) < max_examples_per_family:
            sample = _sample(pairing)
            sample["excluded_reason"] = reason
            sample["role_pair"] = pairing.get("role_pair")
            excluded_examples[family][reason].append(sample)

    complete_set_examples = {family["product_family"]: family.get("complete_set_articles", [])[:max_examples_per_family] for family in payload["families"]}
    unknown_role_examples = {family["product_family"]: family.get("unknown_role_articles", [])[:max_examples_per_family] for family in payload["families"]}

    return {
        "source_pack_path": payload["source_pack_path"],
        "compatibility_candidate_count": payload["compatibility_candidate_count"],
        "actionable_candidate_count": payload["actionable_candidate_count"],
        "excluded_candidate_count": payload["excluded_candidate_count"],
        "excluded_reason_counts": payload["excluded_reason_counts"],
        "complete_set_article_count": payload["complete_set_article_count"],
        "unknown_role_row_count": payload["unknown_role_row_count"],
        "family_source_row_counts": payload["family_source_row_counts"],
        "family_actionable_candidate_counts": payload["family_actionable_candidate_counts"],
        "family_excluded_reason_counts": payload["family_excluded_reason_counts"],
        "evidence_level_counts": payload["evidence_level_counts"],
        "family_evidence_level_counts": payload["family_evidence_level_counts"],
        "role_pair_counts": dict(sorted(role_pair_counts.items())),
        "page_range_label_counts": dict(sorted(page_range_counts.items())),
        "explicit_text_pairing_candidate_count": explicit_text_count,
        "production_safe_candidate_count": payload["production_safe_candidate_count"],
        "diagnostic_only_candidate_count": payload["diagnostic_only_candidate_count"],
        "production_promotion_blocked": payload["production_promotion_blocked"],
        "ready_for_benchmark": payload["ready_for_benchmark"],
        "ready_for_customer_view": payload["ready_for_customer_view"],
        "actionable_examples_by_family_and_evidence_level": {family: dict(levels) for family, levels in sorted(examples.items())},
        "examples_by_family_and_evidence_level": {family: dict(levels) for family, levels in sorted(examples.items())},
        "excluded_examples_by_family_and_reason": {family: dict(reasons) for family, reasons in sorted(excluded_examples.items())},
        "complete_set_examples_by_family": complete_set_examples,
        "unknown_role_examples_by_family": unknown_role_examples,
        "audit_notes": [
            "explicit_section_pairing means articles share a catalogue section/page range only; it is not article-level production evidence.",
            "same_length_same_family_candidate is diagnostic-only and never production compatibility evidence.",
            "Actionable candidates are role pairs that could become assemblies after manual evidence review; excluded candidates preserve noisy raw diagnostics with a reason.",
            "Same-role pairings and unknown-role pairings without explicit text are excluded because they are not actionable assembly candidates.",
            "Complete set articles are summarized separately instead of broadly paired into assemblies.",
            "explicit_text_pairing candidates require manual review and remain blocked until reviewed production criteria are implemented.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnostic-only TECE compatibility evidence audit report.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    parser.add_argument("--max-examples-per-family", type=int, default=20)
    args = parser.parse_args(argv)
    report = build_audit_report(args.source_pack, max_examples_per_family=args.max_examples_per_family)
    if args.json:
        write_json_output(report, args.out)
    else:
        stream = io.StringIO()
        print("TECE compatibility evidence audit (diagnostic-only; production promotion blocked)", file=stream)
        for key, value in report.items():
            print(f"{key}: {json.dumps(value, ensure_ascii=False, sort_keys=True)}", file=stream)
        write_text_output(stream.getvalue().rstrip("\n"), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
