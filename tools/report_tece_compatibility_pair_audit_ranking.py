from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.apply_tece_unknown_role_review_csv import DIAGNOSTIC_ONLY_NOTE
from tools.report_tece_overlay_compatibility_diagnostic import (
    PAIR_TYPES,
    _article,
    _family,
    _nominal_length,
    _overlay_role,
    _pair,
    _read_csv,
    _sample_row,
    _source_file,
    _status,
)
from tools.report_tece_reviewed_role_overlay_qa import build_qa_report
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

BLOCKED_UNKNOWN_ARTICLES = ["650700", "650800", "651500"]
CSV_COLUMNS = [
    "pair_type", "diagnostic_review_priority", "status", "drain_body_article", "candidate_article",
    "drain_body_nominal_length_mm", "candidate_nominal_length_mm", "candidate_role",
    "drain_body_source_file", "candidate_source_file", "drain_body_evidence_text_snippet",
    "candidate_evidence_text_snippet", "diagnostic_only", "production_safe", "production_promotion_blocked",
    "ready_for_benchmark", "ready_for_customer_view", "audit_note",
]
PRIORITY_ORDER = {
    "high_review_priority": 0,
    "medium_review_priority": 1,
    "blocked_review": 2,
    "unresolved_length_missing": 3,
    "excluded_not_paired": 4,
}
TEXT_COLUMNS = ("evidence_text_snippet", "evidence_text", "evidence_text_or_reason", "product_name", "description", "name", "review_apply_note")


def _load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as fh:
        data = json.load(fh)
    return data if isinstance(data, dict) else {"valid": False, "errors": ["report is not a JSON object"]}


def _evidence(row: dict[str, Any]) -> str:
    for col in TEXT_COLUMNS:
        text = _clean(row.get(col))
        if text:
            return text[:500]
    return ""


def _has_article(value: str) -> bool:
    return bool(_clean(value))


def _priority(pair: dict[str, Any], blocked_unknown_articles: set[str]) -> str:
    if pair["pair_type"] == "drain_body_to_unknown" or pair["candidate_article"] in blocked_unknown_articles:
        return "blocked_review"
    if pair["status"] == "unresolved_length_missing":
        return "unresolved_length_missing"
    if pair["pair_type"] == "drain_body_to_cover_or_grate" and pair["status"] == "diagnostic_exact_length_match" and _has_article(pair["drain_body_article"]) and _has_article(pair["candidate_article"]):
        return "high_review_priority"
    if pair["pair_type"] == "drain_body_to_accessory" and pair["status"] == "diagnostic_exact_length_match":
        return "medium_review_priority"
    return "excluded_not_paired"


def _pair_row(body: dict[str, Any], other: dict[str, Any], pair_type: str, blocked_unknown_articles: set[str]) -> dict[str, Any] | None:
    body_len = _nominal_length(body)
    other_len = _nominal_length(other)
    if body_len and other_len and body_len != other_len:
        return None
    pair = _pair(body, other, pair_type)
    if pair["status"] == "unresolved_length_mismatch":
        return None
    pair.update({
        "drain_body_source_file": _source_file(body),
        "candidate_source_file": _source_file(other),
        "drain_body_evidence_text_snippet": _evidence(body),
        "candidate_evidence_text_snippet": _evidence(other),
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "audit_note": "diagnostic audit row only; role and length do not imply production compatibility",
    })
    pair["diagnostic_review_priority"] = _priority(pair, blocked_unknown_articles)
    return pair


def _sort_key(pair: dict[str, Any]) -> tuple[Any, ...]:
    length = _clean(pair.get("drain_body_nominal_length_mm")) or _clean(pair.get("candidate_nominal_length_mm"))
    try:
        numeric = int(float(length))
    except ValueError:
        numeric = 10**9
    return (PRIORITY_ORDER.get(pair.get("diagnostic_review_priority"), 99), pair.get("pair_type", ""), numeric, pair.get("drain_body_article", ""), pair.get("candidate_article", ""))


def _rows_for(bodies: list[dict[str, Any]], others: list[dict[str, Any]], pair_type: str, blocked: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for body in bodies:
        for other in others:
            row = _pair_row(body, other, pair_type, blocked)
            if row is not None:
                rows.append(row)
    return rows


def build_pair_audit_ranking(preview_csv: str | Path, qa_report: str | Path, compatibility_report: str | Path, family: str = "TECEdrainline", csv_out: str | Path | None = None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        qa = build_qa_report(preview_csv, qa_report, family=family)
    except Exception as exc:
        qa = {"valid": False, "errors": [str(exc)], "warnings": []}
    if qa.get("valid") is not True:
        errors.append("QA report is invalid; pair audit ranking is blocked")
        errors.extend(str(e) for e in qa.get("errors", []))
    warnings.extend(str(w) for w in qa.get("warnings", []))

    try:
        compat = _load_json(compatibility_report)
    except Exception as exc:
        compat = {"valid": False, "errors": [str(exc)]}
    if compat.get("valid") is not True:
        errors.append("compatibility report is invalid; pair audit ranking is blocked")
        errors.extend(str(e) for e in compat.get("errors", []))

    rows = _read_csv(preview_csv)
    family_rows = [row for row in rows if _family(row) == family]
    role_counts = Counter(_overlay_role(row) for row in family_rows)
    bodies = [row for row in family_rows if _overlay_role(row) == "drain_body"]
    covers = [row for row in family_rows if _overlay_role(row) == "cover_or_grate"]
    profiles = [row for row in family_rows if _overlay_role(row) == "profile_cover"]
    accessories = [row for row in family_rows if _overlay_role(row) == "accessory"]
    complete_sets = [row for row in family_rows if _overlay_role(row) == "complete_set"]
    unknowns = [row for row in family_rows if _overlay_role(row) == "unknown" and _status(row) in {"skipped_blocked", "blocked"}]
    blocked_articles = sorted(_article(row) for row in unknowns)
    blocked_set = set(blocked_articles)

    pairs: list[dict[str, Any]] = []
    pairs.extend(_rows_for(bodies, covers, "drain_body_to_cover_or_grate", blocked_set))
    pairs.extend(_rows_for(bodies, profiles, "drain_body_to_profile_cover", blocked_set))
    pairs.extend(_rows_for(bodies, accessories, "drain_body_to_accessory", blocked_set))
    pairs.extend(_rows_for(bodies, unknowns, "drain_body_to_unknown", blocked_set))
    pairs.sort(key=_sort_key)

    type_counts = Counter(pair["pair_type"] for pair in pairs)
    status_counts = Counter(pair["status"] for pair in pairs)
    priority_counts = Counter(pair["diagnostic_review_priority"] for pair in pairs)
    input_type = compat.get("candidate_pair_counts_by_type", {}) if isinstance(compat, dict) else {}
    input_status = compat.get("candidate_pair_counts_by_status", {}) if isinstance(compat, dict) else {}
    recomputed_type = {key: type_counts.get(key, 0) for key in PAIR_TYPES}
    recomputed_status = dict(sorted(status_counts.items()))

    if input_type and {key: int(input_type.get(key, 0)) for key in PAIR_TYPES} != recomputed_type:
        errors.append("recomputed pair counts by type differ from compatibility report")
    if input_status and {key: int(input_status.get(key, 0)) for key in sorted(set(input_status) | set(recomputed_status))} != {key: recomputed_status.get(key, 0) for key in sorted(set(input_status) | set(recomputed_status))}:
        errors.append("recomputed pair counts by status differ from compatibility report")
    if family == "TECEdrainline" and blocked_articles != BLOCKED_UNKNOWN_ARTICLES:
        errors.append("blocked unknown articles must remain exactly 650700, 650800, 651500")
    if int(compat.get("production_safe_candidate_count", 0) or 0) != 0:
        errors.append("production_safe_candidate_count must remain 0")
    if any(pair.get("production_safe") is not False or pair.get("ready_for_benchmark") is not False or pair.get("ready_for_customer_view") is not False for pair in pairs):
        errors.append("diagnostic pairs must not be production-safe or customer/benchmark-ready")

    if csv_out:
        with Path(csv_out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows([{col: pair.get(col, "") for col in CSV_COLUMNS} for pair in pairs])

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "preview_csv_path": str(Path(preview_csv)),
        "qa_report_path": str(Path(qa_report)),
        "compatibility_report_path": str(Path(compatibility_report)),
        "family": family,
        "overlay_role_counts": dict(sorted(role_counts.items())),
        "input_candidate_pair_counts_by_type": input_type,
        "recomputed_pair_counts_by_type": recomputed_type,
        "input_candidate_pair_counts_by_status": input_status,
        "recomputed_pair_counts_by_status": recomputed_status,
        "review_priority_counts": dict(sorted(priority_counts.items())),
        "high_review_priority_count": priority_counts.get("high_review_priority", 0),
        "medium_review_priority_count": priority_counts.get("medium_review_priority", 0),
        "blocked_review_count": priority_counts.get("blocked_review", 0),
        "unresolved_length_missing_count": priority_counts.get("unresolved_length_missing", 0),
        "excluded_not_paired_count": priority_counts.get("excluded_not_paired", 0) + len(complete_sets),
        "blocked_unknown_articles": blocked_articles,
        "top_high_priority_pairs_sample": [p for p in pairs if p["diagnostic_review_priority"] == "high_review_priority"][:25],
        "unresolved_length_missing_sample": [p for p in pairs if p["diagnostic_review_priority"] == "unresolved_length_missing"][:25],
        "blocked_review_sample": [p for p in pairs if p["diagnostic_review_priority"] == "blocked_review"][:25],
        "complete_set_rows_sample": [_sample_row(row) for row in complete_sets[:25]],
        "evidence_source_file_counts": dict(sorted(Counter(_source_file(row) for row in family_rows).items())),
        "csv_out_path": str(Path(csv_out)) if csv_out else "",
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build diagnostic-only TECE compatibility pair audit/ranking report.")
    parser.add_argument("--preview-csv", required=True)
    parser.add_argument("--qa-report", required=True)
    parser.add_argument("--compatibility-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", required=True)
    parser.add_argument("--csv-out")
    args = parser.parse_args(argv)
    report = build_pair_audit_ranking(args.preview_csv, args.qa_report, args.compatibility_report, family=args.family, csv_out=args.csv_out)
    write_json_output(report, out=args.out)
    if not args.json:
        write_text_output("valid: " + str(report["valid"]))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
