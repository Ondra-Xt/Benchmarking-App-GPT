from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from tools.report_tece_unknown_role_contexts import _clean

SORTIMENTSLISTE = "tece_sortimentsliste_de_2026.pdf"
BLOCKED_UNKNOWN_ARTICLES = {"650700", "650800", "651500"}
DIAGNOSTIC_ONLY_NOTE = (
    "Diagnostic-only TECE high-priority pair shortlist for human review; this exporter writes only the requested "
    "CSV/JSON artifacts and does not mutate source-pack files, TECE extraction/classification logic, production "
    "promotion flags, ACO canonical export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details."
)
CSV_COLUMNS = [
    "shortlist_id", "family", "pair_type", "nominal_length_mm", "drain_body_article", "candidate_article",
    "candidate_role", "drain_body_source_file", "candidate_source_file", "drain_body_evidence_text_snippet",
    "candidate_evidence_text_snippet", "diagnostic_review_priority", "status", "diagnostic_only",
    "production_safe", "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view",
    "reviewer_pair_decision", "reviewer_notes", "safe_to_apply_automatically",
]


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _load_report(path: str | Path) -> tuple[dict[str, Any], list[str]]:
    try:
        with Path(path).open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        return {}, [f"pair audit report could not be read as JSON: {exc}"]
    if not isinstance(data, dict):
        return {}, ["pair audit report is not a JSON object"]
    if data.get("valid") is not True:
        return data, ["pair audit report is invalid"] + [str(e) for e in data.get("errors", [])]
    return data, []


def _bool_false(value: Any) -> bool:
    return _clean(value).lower() in {"", "false", "0", "no", "n"} or value is False


def _length(value: Any) -> str:
    text = _clean(value)
    return text[:-2] if text.endswith(".0") else text


def _length_num(value: Any) -> int:
    try:
        return int(float(_length(value)))
    except ValueError:
        return 10**9


def _evidence_priority(row: dict[str, Any]) -> str:
    body = _clean(row.get("drain_body_source_file"))
    cand = _clean(row.get("candidate_source_file"))
    if body == SORTIMENTSLISTE and cand == SORTIMENTSLISTE:
        return "both_sortimentsliste_2026"
    if cand == SORTIMENTSLISTE:
        return "candidate_sortimentsliste_2026"
    if body == SORTIMENTSLISTE:
        return "drain_body_sortimentsliste_2026"
    return "other_sources"


def _evidence_rank(row: dict[str, Any]) -> int:
    return {
        "both_sortimentsliste_2026": 0,
        "candidate_sortimentsliste_2026": 1,
        "drain_body_sortimentsliste_2026": 2,
        "other_sources": 3,
    }[_evidence_priority(row)]


def _eligible(row: dict[str, Any]) -> bool:
    body_len = _length(row.get("drain_body_nominal_length_mm"))
    cand_len = _length(row.get("candidate_nominal_length_mm"))
    return (
        _clean(row.get("diagnostic_review_priority")) == "high_review_priority"
        and _clean(row.get("pair_type")) == "drain_body_to_cover_or_grate"
        and _clean(row.get("status")) == "diagnostic_exact_length_match"
        and _bool_false(row.get("production_safe"))
        and _bool_false(row.get("ready_for_benchmark"))
        and _bool_false(row.get("ready_for_customer_view"))
        and bool(_clean(row.get("drain_body_article")))
        and bool(_clean(row.get("candidate_article")))
        and body_len != ""
        and body_len == cand_len
        and _clean(row.get("drain_body_article")) not in BLOCKED_UNKNOWN_ARTICLES
        and _clean(row.get("candidate_article")) not in BLOCKED_UNKNOWN_ARTICLES
    )


def _deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            _clean(row.get("drain_body_article")), _clean(row.get("candidate_article")),
            _length(row.get("drain_body_nominal_length_mm")), _length(row.get("candidate_nominal_length_mm")),
        )
        current = best.get(key)
        if current is None or (_evidence_rank(row), _clean(row.get("candidate_article"))) < (_evidence_rank(current), _clean(current.get("candidate_article"))):
            best[key] = row
    return list(best.values())


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (_length_num(row.get("drain_body_nominal_length_mm")), _clean(row.get("drain_body_article")), _evidence_rank(row), _clean(row.get("candidate_article")))


def _apply_caps(rows: list[dict[str, Any]], max_rows: int, max_per_body: int, max_per_length: int) -> tuple[list[dict[str, Any]], int]:
    out: list[dict[str, Any]] = []
    by_body: Counter[str] = Counter()
    by_len: Counter[str] = Counter()
    skipped = 0
    for row in sorted(rows, key=_sort_key):
        body = _clean(row.get("drain_body_article"))
        length = _length(row.get("drain_body_nominal_length_mm"))
        if len(out) >= max_rows or by_body[body] >= max_per_body or by_len[length] >= max_per_length:
            skipped += 1
            continue
        out.append(row)
        by_body[body] += 1
        by_len[length] += 1
    return out, skipped


def _validate_export(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for idx, row in enumerate(rows, start=1):
        if not _bool_false(row.get("production_safe")):
            errors.append(f"row {idx} has production_safe=true")
        if not _bool_false(row.get("ready_for_benchmark")):
            errors.append(f"row {idx} has ready_for_benchmark=true")
        if not _bool_false(row.get("ready_for_customer_view")):
            errors.append(f"row {idx} has ready_for_customer_view=true")
        if {_clean(row.get("drain_body_article")), _clean(row.get("candidate_article"))} & BLOCKED_UNKNOWN_ARTICLES:
            errors.append(f"row {idx} contains a blocked unknown article")
        if _clean(row.get("diagnostic_review_priority")) != "high_review_priority":
            errors.append(f"row {idx} is not high_review_priority")
        if _clean(row.get("status")) != "diagnostic_exact_length_match":
            errors.append(f"row {idx} is not an exact-length match")
        if _length(row.get("drain_body_nominal_length_mm")) != _length(row.get("candidate_nominal_length_mm")):
            errors.append(f"row {idx} has non-matching nominal lengths")
    return errors


def build_shortlist(pair_audit_csv: str | Path, pair_audit_report: str | Path, family: str = "TECEdrainline", out: str | Path | None = None, json_out: str | Path | None = None, max_rows: int = 300, max_pairs_per_drain_body: int = 8, max_pairs_per_length: int = 80) -> dict[str, Any]:
    report, report_errors = _load_report(pair_audit_report)
    rows = _read_csv(pair_audit_csv) if not report_errors else []
    summary: dict[str, Any] = {
        "valid": False, "errors": list(report_errors), "warnings": [],
        "pair_audit_csv_path": str(pair_audit_csv), "pair_audit_report_path": str(pair_audit_report), "family": family,
        "input_pair_rows": len(rows), "eligible_high_priority_rows": 0, "deduplicated_high_priority_rows": 0,
        "exported_shortlist_rows": 0, "max_rows": max_rows, "max_pairs_per_drain_body": max_pairs_per_drain_body,
        "max_pairs_per_length": max_pairs_per_length, "length_distribution": {}, "drain_body_distribution_sample": {},
        "evidence_priority_distribution": {}, "skipped_by_caps_count": 0, "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    if report_errors:
        if json_out:
            Path(json_out).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        return summary

    eligible = [row for row in rows if _eligible(row)]
    deduped = _deduplicate(eligible)
    exported, skipped = _apply_caps(deduped, max_rows, max_pairs_per_drain_body, max_pairs_per_length)
    validation_errors = _validate_export(exported)

    output_rows = []
    for idx, row in enumerate(exported, start=1):
        output_rows.append({
            "shortlist_id": f"TECE-HP-{idx:04d}", "family": family, "pair_type": "drain_body_to_cover_or_grate",
            "nominal_length_mm": _length(row.get("drain_body_nominal_length_mm")),
            "drain_body_article": _clean(row.get("drain_body_article")), "candidate_article": _clean(row.get("candidate_article")),
            "candidate_role": _clean(row.get("candidate_role")), "drain_body_source_file": _clean(row.get("drain_body_source_file")),
            "candidate_source_file": _clean(row.get("candidate_source_file")),
            "drain_body_evidence_text_snippet": _clean(row.get("drain_body_evidence_text_snippet")),
            "candidate_evidence_text_snippet": _clean(row.get("candidate_evidence_text_snippet")),
            "diagnostic_review_priority": "high_review_priority", "status": "diagnostic_exact_length_match",
            "diagnostic_only": "true", "production_safe": "false", "production_promotion_blocked": "true",
            "ready_for_benchmark": "false", "ready_for_customer_view": "false", "reviewer_pair_decision": "",
            "reviewer_notes": "", "safe_to_apply_automatically": "",
        })
    if out and not validation_errors:
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows(output_rows)

    summary.update({
        "valid": not validation_errors, "errors": validation_errors,
        "eligible_high_priority_rows": len(eligible), "deduplicated_high_priority_rows": len(deduped),
        "exported_shortlist_rows": len(output_rows) if not validation_errors else 0,
        "length_distribution": dict(Counter(r["nominal_length_mm"] for r in output_rows)),
        "drain_body_distribution_sample": dict(sorted(Counter(r["drain_body_article"] for r in output_rows).items())[:25]),
        "evidence_priority_distribution": dict(Counter(_evidence_priority(row) for row in exported)),
        "skipped_by_caps_count": skipped,
    })
    if json_out:
        Path(json_out).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export diagnostic-only TECE high-priority pair shortlist.")
    parser.add_argument("--pair-audit-csv", required=True)
    parser.add_argument("--pair-audit-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out")
    parser.add_argument("--max-rows", type=int, default=300)
    parser.add_argument("--max-pairs-per-drain-body", type=int, default=8)
    parser.add_argument("--max-pairs-per-length", type=int, default=80)
    args = parser.parse_args(argv)
    summary = build_shortlist(**vars(args))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("valid") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
