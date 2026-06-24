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

from tools import report_tece_compatibility_diagnostics as compat_mod
from tools.tece_report_output import write_json_output, write_text_output

EVIDENCE_RANK = {
    "explicit_text_pairing": 0,
    "explicit_section_pairing": 1,
    "same_length_same_family_candidate": 2,
}
CONFIDENCE_RANK = {"high": 0, "medium": 1, "low": 2}
KNOWN_ROLE_PAIRS = {
    "drain_body_to_cover_or_grate",
    "profile_body_to_drain_body",
    "profile_channel_to_drain_body",
    "drain_body_to_cover_plate",
}
TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "installation_height_mm",
)


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _technical_fields(row: dict[str, Any] | None) -> list[str]:
    if not row:
        return []
    return [field for field in TECHNICAL_FIELDS if _present(row.get(field))]


def _article_lookup(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for family in payload["families"]:
        family_name = family["product_family"]
        for bucket in (
            "candidate_body_channel_drain_articles",
            "candidate_cover_grate_plate_articles",
            "complete_set_articles",
            "unknown_role_articles",
        ):
            for row in family.get(bucket, []):
                lookup[(family_name, str(row.get("article_number", "")))] = row
    return lookup


def _row_from_pairing(pairing: dict[str, Any], lookup: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    family = pairing.get("family") or "unknown"
    body_article = pairing.get("body_or_drain_article")
    cover_article = pairing.get("cover_grate_plate_article")
    body_row = lookup.get((family, str(body_article)))
    cover_row = lookup.get((family, str(cover_article)))
    technical = sorted(set(_technical_fields(body_row) + _technical_fields(cover_row)))
    conditional_values = []
    for row in (body_row, cover_row):
        if row:
            conditional_values.extend(row.get("conditional_technical_values") or [])
    return {
        "family": family,
        "page_range_label": pairing.get("page_range_label"),
        "role_pair": pairing.get("role_pair"),
        "body_or_profile_article_number": body_article,
        "cover_or_grate_or_drain_article_number": cover_article,
        "nominal_length_mm": pairing.get("nominal_length_mm"),
        "evidence_level": pairing.get("evidence_level") or pairing.get("evidence_type"),
        "evidence_confidence": pairing.get("evidence_confidence"),
        "source_file": pairing.get("source_file"),
        "source_page_start": pairing.get("source_page_start"),
        "source_page_end": pairing.get("source_page_end"),
        "evidence_text_or_reason": pairing.get("evidence_text_or_reason"),
        "technical_fields_available": technical,
        "conditional_technical_values_available": conditional_values,
        "why_not_production_safe": pairing.get("why_not_production_safe"),
        "manual_review_status": "pending_review",
    }


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        EVIDENCE_RANK.get(row.get("evidence_level"), 99),
        CONFIDENCE_RANK.get(row.get("evidence_confidence"), 99),
        0 if row.get("role_pair") in KNOWN_ROLE_PAIRS else 1,
        -len(row.get("technical_fields_available") or []),
        0 if _present(row.get("nominal_length_mm")) else 1,
        str(row.get("family") or ""),
        str(row.get("body_or_profile_article_number") or ""),
        str(row.get("cover_or_grate_or_drain_article_number") or ""),
    )


def build_shortlist_report(source_pack: str | Path, max_per_family: int = 50, min_evidence_level: str | None = None) -> dict[str, Any]:
    diagnostics = compat_mod.build_compatibility_diagnostics_report(source_pack)
    payload = asdict(diagnostics)
    lookup = _article_lookup(payload)
    minimum_rank = EVIDENCE_RANK.get(min_evidence_level, 99) if min_evidence_level else 99
    rows_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_actionable = [pairing for family in payload["families"] for pairing in family["possible_pairings"]]
    for pairing in all_actionable:
        level = pairing.get("evidence_level") or pairing.get("evidence_type")
        if EVIDENCE_RANK.get(level, 99) <= minimum_rank:
            row = _row_from_pairing(pairing, lookup)
            rows_by_family[row["family"]].append(row)
    shortlist: list[dict[str, Any]] = []
    for family in sorted(rows_by_family):
        shortlist.extend(sorted(rows_by_family[family], key=_sort_key)[:max_per_family])
    shortlist = sorted(shortlist, key=_sort_key)
    evidence_counts = Counter(row["evidence_level"] for row in shortlist)
    role_counts = Counter(row["role_pair"] for row in shortlist)
    family_counts = Counter(row["family"] for row in shortlist)
    return {
        "source_pack_path": payload["source_pack_path"],
        "summary": {
            "total_actionable_candidate_count": payload["actionable_candidate_count"],
            "shortlisted_candidate_count": len(shortlist),
            "family_shortlist_counts": dict(sorted(family_counts.items())),
            "evidence_level_counts": dict(sorted(evidence_counts.items())),
            "role_pair_counts": dict(sorted(role_counts.items())),
            "production_safe_candidate_count": 0,
            "diagnostic_only_candidate_count": len(shortlist),
            "production_promotion_blocked": True,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
        },
        "shortlisted_candidates": shortlist,
        "report_notes": [
            "Diagnostic-only human-review shortlist; it does not create production assemblies.",
            "Same nominal length is only a ranking hint when applicable and is not production compatibility evidence.",
            "Production promotion remains blocked until manually approved explicit evidence fixtures and criteria exist.",
        ],
    }


def _text_report(report: dict[str, Any]) -> str:
    stream = io.StringIO()
    print("TECE actionable review shortlist (diagnostic-only; production promotion blocked)", file=stream)
    print("Summary", file=stream)
    for key, value in report["summary"].items():
        print(f"  {key}: {json.dumps(value, ensure_ascii=False, sort_keys=True)}", file=stream)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in report["shortlisted_candidates"]:
        grouped[row["family"]].append(row)
    for family in sorted(grouped):
        print(f"\n{family}", file=stream)
        for row in grouped[family]:
            print(
                "  - "
                f"{row['role_pair']} {row['body_or_profile_article_number']} -> {row['cover_or_grate_or_drain_article_number']} "
                f"len={row['nominal_length_mm']} evidence={row['evidence_level']} confidence={row['evidence_confidence']} "
                f"status={row['manual_review_status']}",
                file=stream,
            )
            print(f"    reason: {row['evidence_text_or_reason']}", file=stream)
    return stream.getvalue().rstrip("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnostic-only TECE actionable compatibility review shortlist.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    parser.add_argument("--max-per-family", type=int, default=50)
    parser.add_argument("--min-evidence-level", choices=sorted(EVIDENCE_RANK), default=None)
    args = parser.parse_args(argv)
    report = build_shortlist_report(args.source_pack, max_per_family=args.max_per_family, min_evidence_level=args.min_evidence_level)
    if args.json:
        write_json_output(report, args.out)
    else:
        write_text_output(_text_report(report), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
