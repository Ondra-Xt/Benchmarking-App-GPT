from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

DEFAULT_SOURCE_PACK = Path("local_source_packs/tece/pilot_001")
EXPECTED_FAMILY = "TECEdrainprofile"
PAIR_TYPE = "drain_body_to_profile_cover"
RECOMMENDED_NEXT_ACTION = "manually review TECEdrainprofile drain body to profile cover article-level compatibility before any production generation"
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile pair audit; length match is not production evidence; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This report is read-only and diagnostic-only. It audits TECEdrainprofile drain body to profile cover candidate pairs for manual review only and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."

CSV_COLUMNS = [
    "audit_pair_id", "family", "pair_type", "drain_body_article", "profile_cover_article",
    "nominal_length_mm", "drain_body_role", "profile_cover_role", "drain_body_source_file",
    "profile_cover_source_file", "drain_body_evidence_text_snippet", "profile_cover_evidence_text_snippet",
    "drain_body_length_mm", "profile_cover_length_mm", "pairing_status", "review_priority",
    "diagnostic_only", "production_safe", "production_promotion_blocked", "ready_for_benchmark",
    "ready_for_customer_view", "recommended_next_action", "production_status_note",
]


def _row_value(row: Any, name: str, default: Any = "") -> Any:
    if isinstance(row, dict):
        return row.get(name, default)
    return getattr(row, name, default)


def _family(row: Any) -> str:
    family = _clean(_row_value(row, "tece_family_candidate") or _row_value(row, "product_family") or _row_value(row, "family"))
    return "TECEdrainpoint S" if family.lower() == "tecedrainpoint s" else family


def _role(row: Any) -> str:
    return _clean(_row_value(row, "tece_article_role_candidate") or _row_value(row, "article_role") or _row_value(row, "role")) or "unknown"


def _article(row: Any) -> str:
    return _clean(_row_value(row, "article_number") or _row_value(row, "article") or _row_value(row, "product_article_number"))


def _source_file(row: Any) -> str:
    return _clean(_row_value(row, "source_file") or _row_value(row, "evidence_source_file"))


def _evidence(row: Any) -> str:
    for name in ("evidence_text", "evidence_text_snippet", "product_name", "description", "name"):
        value = _clean(_row_value(row, name))
        if value:
            return value[:500]
    return ""


def _length(row: Any) -> str:
    for name in ("nominal_length_mm", "length_mm", "length", "product_length_mm"):
        value = _clean(_row_value(row, name)).replace(",", ".")
        if value:
            match = re.search(r"(?<!\d)(\d{2,4})(?:\.0+)?\s*(?:mm)?(?!\d)", value, re.I)
            if match:
                return match.group(1)
    text = _evidence(row)
    match = re.search(r"(?i)(?:nennlänge|länge|laenge|length|nominal)[^\d]{0,30}(\d{2,4})\s*(?:mm)?", text)
    return match.group(1) if match else ""


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _candidate_row(body: Any, cover: Any) -> dict[str, Any] | None:
    body_len = _length(body)
    cover_len = _length(cover)
    if body_len and cover_len and body_len != cover_len:
        return None
    status = "diagnostic_exact_length_match" if body_len and cover_len and body_len == cover_len else "unresolved_length_missing"
    body_ev = _evidence(body)
    cover_ev = _evidence(cover)
    if status == "unresolved_length_missing":
        priority = "unresolved_length_missing"
    elif body_ev and cover_ev:
        priority = "high_review_priority"
    else:
        priority = "medium_review_priority"
    return {
        "family": EXPECTED_FAMILY,
        "pair_type": PAIR_TYPE,
        "drain_body_article": _article(body),
        "profile_cover_article": _article(cover),
        "nominal_length_mm": body_len if body_len == cover_len else "",
        "drain_body_role": "drain_body",
        "profile_cover_role": "profile_cover",
        "drain_body_source_file": _source_file(body),
        "profile_cover_source_file": _source_file(cover),
        "drain_body_evidence_text_snippet": body_ev,
        "profile_cover_evidence_text_snippet": cover_ev,
        "drain_body_length_mm": body_len,
        "profile_cover_length_mm": cover_len,
        "pairing_status": status,
        "review_priority": priority,
        "diagnostic_only": _bool_text(True),
        "production_safe": _bool_text(False),
        "production_promotion_blocked": _bool_text(True),
        "ready_for_benchmark": _bool_text(False),
        "ready_for_customer_view": _bool_text(False),
        "recommended_next_action": RECOMMENDED_NEXT_ACTION,
        "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def validate_audit_rows(rows: list[dict[str, Any]], family: str = EXPECTED_FAMILY) -> dict[str, list[Any]]:
    invalid_family_rows = [r for r in rows if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_role_rows = [r for r in rows if r.get("pair_type") != PAIR_TYPE or r.get("drain_body_role") != "drain_body" or r.get("profile_cover_role") != "profile_cover"]
    production_leakage_rows = [r for r in rows if _clean(r.get("production_safe")).lower() == "true"]
    readiness_leakage_rows = [r for r in rows if _clean(r.get("ready_for_benchmark")).lower() == "true" or _clean(r.get("ready_for_customer_view")).lower() == "true"]
    counts = Counter((r.get("drain_body_article"), r.get("profile_cover_article"), r.get("nominal_length_mm")) for r in rows)
    duplicate_pair_keys = [{"drain_body_article": k[0], "profile_cover_article": k[1], "nominal_length_mm": k[2], "count": v} for k, v in sorted(counts.items()) if v > 1]
    return {"invalid_family_rows": invalid_family_rows, "invalid_role_rows": invalid_role_rows, "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows, "duplicate_pair_keys": duplicate_pair_keys}


def build_audit(source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    rows: list[Any] = []
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")
    try:
        report = load_source_pack(source_pack)
        rows = list(report.rows)
    except Exception as exc:
        report = None
        errors.append(f"required input data cannot be loaded: {exc}")
    family_rows = [r for r in rows if _family(r) == EXPECTED_FAMILY]
    role_counts = Counter(_role(r) for r in family_rows)
    bodies = [r for r in family_rows if _role(r) == "drain_body"]
    covers = [r for r in family_rows if _role(r) == "profile_cover"]
    candidates = [p for b in bodies for c in covers for p in [_candidate_row(b, c)] if p is not None]
    candidates.sort(key=lambda r: (r["pairing_status"] != "diagnostic_exact_length_match", int(r["nominal_length_mm"] or r["drain_body_length_mm"] or r["profile_cover_length_mm"] or 10**9), r["drain_body_article"], r["profile_cover_article"]))
    for idx, row in enumerate(candidates, 1):
        row["audit_pair_id"] = f"TECE-DP-AUDIT-{idx:06d}"
    gates = validate_audit_rows(candidates, family)
    for key, message in (("duplicate_pair_keys", "duplicate pair keys exist"), ("invalid_family_rows", "non-TECEdrainprofile rows included"), ("invalid_role_rows", "invalid pair type or role rows included"), ("production_leakage_rows", "production_safe leakage found"), ("readiness_leakage_rows", "readiness leakage found")):
        if gates[key]:
            errors.append(message)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows([{col: row.get(col, "") for col in CSV_COLUMNS} for row in candidates])
    status_counts = Counter(r["pairing_status"] for r in candidates)
    priority_counts = Counter(r["review_priority"] for r in candidates)
    report_json = {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "source_pack_path": str(source_pack),
        "family": family,
        "input_inventory_row_count": len(rows),
        "family_inventory_row_count": len(family_rows),
        "drain_body_count": len(bodies),
        "profile_cover_count": len(covers),
        "accessory_count": role_counts.get("accessory", 0),
        "complete_set_count": role_counts.get("complete_set", 0),
        "unknown_count": role_counts.get("unknown", 0),
        "candidate_pair_count": len(candidates),
        "candidate_pair_counts_by_status": dict(sorted(status_counts.items())),
        "candidate_pair_counts_by_priority": dict(sorted(priority_counts.items())),
        **gates,
        "output_csv_path": str(out) if out else "",
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    return candidates, report_json


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only diagnostic TECEdrainprofile compatibility pair audit.")
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    _rows, report = build_audit(args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    else:
        write_text_output("valid: " + str(report["valid"]))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
