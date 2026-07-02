from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.apply_tece_unknown_role_review_csv import DIAGNOSTIC_ONLY_NOTE
from tools.report_tece_reviewed_role_overlay_qa import build_qa_report
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

ROLE_COLUMNS = ("applied_article_role", "article_role", "tece_article_role_candidate")
LENGTH_COLUMNS = ("nominal_length_mm", "length_mm", "nominal_length", "length", "product_length_mm")
TEXT_COLUMNS = (
    "evidence_text", "evidence_text_or_reason", "product_name", "description", "name", "review_apply_note",
)
PAIR_TYPES = (
    "drain_body_to_cover_or_grate",
    "drain_body_to_profile_cover",
    "drain_body_to_accessory",
    "drain_body_to_unknown",
)
UNKNOWN_BLOCKED_STATUSES = {"skipped_blocked", "blocked"}


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _family(row: dict[str, Any]) -> str:
    return _clean(row.get("tece_family_candidate") or row.get("product_family") or row.get("family"))


def _article(row: dict[str, Any]) -> str:
    return _clean(row.get("article_number") or row.get("product_article_number") or row.get("article"))


def _overlay_role(row: dict[str, Any]) -> str:
    for col in ROLE_COLUMNS:
        role = _clean(row.get(col))
        if role:
            return role
    return "unknown"


def _original_role(row: dict[str, Any]) -> str:
    return _clean(row.get("original_article_role") or row.get("article_role") or row.get("tece_article_role_candidate")) or "unknown"


def _status(row: dict[str, Any]) -> str:
    return _clean(row.get("review_apply_status"))


def _source_file(row: dict[str, Any]) -> str:
    return _clean(row.get("source_file") or row.get("evidence_source_file")) or "(blank)"


def _row_text(row: dict[str, Any]) -> str:
    return " ".join(_clean(row.get(col)) for col in TEXT_COLUMNS if _clean(row.get(col)))


def _length_from_value(value: Any) -> str:
    text = _clean(value).replace(",", ".")
    if not text:
        return ""
    match = re.search(r"(?<!\d)(\d{2,4})(?:\.0+)?\s*(?:mm)?(?!\d)", text, flags=re.I)
    return match.group(1) if match else ""


def _nominal_length(row: dict[str, Any]) -> str:
    for col in LENGTH_COLUMNS:
        length = _length_from_value(row.get(col))
        if length:
            return length
    # Conservative text parsing: require a length cue or mm unit in evidence text.
    text = _row_text(row)
    cue_match = re.search(r"(?i)(?:length|länge|laenge|nominal)[^\d]{0,20}(\d{2,4})\s*(?:mm)?", text)
    if cue_match:
        return cue_match.group(1)
    mm_match = re.search(r"(?<!\d)(\d{2,4})\s*mm\b", text, flags=re.I)
    return mm_match.group(1) if mm_match else ""


def _sample_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_number": _article(row),
        "applied_article_role": _overlay_role(row),
        "original_article_role": _original_role(row),
        "review_apply_status": _status(row),
        "nominal_length_mm": _nominal_length(row),
        "source_file": _source_file(row),
    }


def _pair(body: dict[str, Any], other: dict[str, Any], pair_type: str) -> dict[str, Any]:
    body_len = _nominal_length(body)
    other_len = _nominal_length(other)
    status = "diagnostic_exact_length_match" if body_len and body_len == other_len else "unresolved_length_missing"
    if body_len and other_len and body_len != other_len:
        status = "unresolved_length_mismatch"
    return {
        "pair_type": pair_type,
        "status": status,
        "drain_body_article": _article(body),
        "candidate_article": _article(other),
        "drain_body_nominal_length_mm": body_len,
        "candidate_nominal_length_mm": other_len,
        "candidate_role": _overlay_role(other),
        "diagnostic_only": True,
        "production_safe": False,
        "production_promotion_blocked": True,
    }


def _pairs_for(bodies: list[dict[str, Any]], others: list[dict[str, Any]], pair_type: str) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for body in bodies:
        body_len = _nominal_length(body)
        for other in others:
            other_len = _nominal_length(other)
            if body_len and other_len and body_len != other_len:
                continue
            pairs.append(_pair(body, other, pair_type))
    return pairs


def build_overlay_compatibility_diagnostic(preview_csv: str | Path, qa_report: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    preview_csv_path = str(Path(preview_csv))
    qa_report_path = str(Path(qa_report))
    rows: list[dict[str, Any]] = []

    try:
        qa = build_qa_report(preview_csv, qa_report, family=family)
    except Exception as exc:  # pragma: no cover
        qa = {"valid": False, "errors": [f"failed to validate QA report: {exc}"], "warnings": []}
    if qa.get("valid") is not True:
        errors.append("QA report is invalid; compatibility diagnostic is blocked")
        errors.extend(str(e) for e in qa.get("errors", []))
    warnings.extend(str(w) for w in qa.get("warnings", []))

    try:
        rows = _read_csv(preview_csv)
    except Exception as exc:  # pragma: no cover
        errors.append(f"failed to read preview CSV: {exc}")

    family_rows = [row for row in rows if _family(row) == family]
    role_counts = Counter(_overlay_role(row) for row in family_rows)
    bodies = [row for row in family_rows if _overlay_role(row) == "drain_body"]
    covers = [row for row in family_rows if _overlay_role(row) == "cover_or_grate"]
    profiles = [row for row in family_rows if _overlay_role(row) == "profile_cover"]
    accessories = [row for row in family_rows if _overlay_role(row) == "accessory"]
    complete_sets = [row for row in family_rows if _overlay_role(row) == "complete_set"]
    unknowns = [row for row in family_rows if _overlay_role(row) == "unknown"]
    blocked_unknowns = [row for row in unknowns if _status(row) in UNKNOWN_BLOCKED_STATUSES]

    pairs = []
    pairs.extend(_pairs_for(bodies, covers, "drain_body_to_cover_or_grate"))
    pairs.extend(_pairs_for(bodies, profiles, "drain_body_to_profile_cover"))
    pairs.extend(_pairs_for(bodies, accessories, "drain_body_to_accessory"))
    pairs.extend(_pairs_for(bodies, blocked_unknowns, "drain_body_to_unknown"))

    pair_type_counts = Counter(pair["pair_type"] for pair in pairs)
    pair_status_counts = Counter(pair["status"] for pair in pairs)
    evidence_counts = Counter(_source_file(row) for row in family_rows)
    unresolved = [pair for pair in pairs if str(pair["status"]).startswith("unresolved")]

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "preview_csv_path": preview_csv_path,
        "qa_report_path": qa_report_path,
        "family": family,
        "total_preview_rows": len(rows),
        "overlay_role_counts": dict(sorted(role_counts.items())),
        "drain_body_count": len(bodies),
        "cover_or_grate_count": len(covers),
        "profile_cover_count": len(profiles),
        "accessory_count": len(accessories),
        "complete_set_count": len(complete_sets),
        "unresolved_unknown_count": len(unknowns),
        "blocked_unknown_articles": sorted(_article(row) for row in blocked_unknowns),
        "candidate_pair_counts_by_type": {key: pair_type_counts.get(key, 0) for key in PAIR_TYPES},
        "candidate_pair_counts_by_status": dict(sorted(pair_status_counts.items())),
        "candidate_pairs_sample": pairs[:25],
        "unresolved_candidates_sample": unresolved[:25],
        "complete_set_rows_sample": [_sample_row(row) for row in complete_sets[:25]],
        "evidence_source_file_counts": dict(sorted(evidence_counts.items())),
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def _text(report: dict[str, Any]) -> str:
    return "\n".join(f"{key}: {report[key]}" for key in report if key not in {"candidate_pairs_sample", "unresolved_candidates_sample", "complete_set_rows_sample"})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a diagnostic-only TECE overlay compatibility candidate report.")
    parser.add_argument("--preview-csv", required=True)
    parser.add_argument("--qa-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", help="Optional JSON output path.")
    args = parser.parse_args(argv)
    report = build_overlay_compatibility_diagnostic(args.preview_csv, args.qa_report, family=args.family)
    if args.out:
        write_json_output(report, out=args.out)
    elif args.json:
        write_json_output(report)
    else:
        write_text_output(_text(report))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
