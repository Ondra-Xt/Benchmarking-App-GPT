from __future__ import annotations

import argparse
import csv
import json
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
TARGET_ROLES = {"drain_body", "profile_cover"}
EXPECTED_TARGET_ROW_COUNT = 42
EXPECTED_DRAIN_BODY_COUNT = 12
EXPECTED_PROFILE_COVER_COUNT = 30
RECOMMENDED_NEXT_ACTION = "review extracted TECEdrainprofile nominal lengths before applying any compatibility pairing"
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile length extraction; no source-pack mutation; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This report is read-only and diagnostic-only. It diagnoses TECEdrainprofile nominal length extraction candidates for drain body and profile cover rows and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."

CSV_COLUMNS = [
    "length_diagnostic_id", "family", "article_number", "article_role", "source_file",
    "evidence_text_snippet", "candidate_length_values_mm", "selected_diagnostic_length_mm",
    "extraction_status", "extraction_confidence", "extraction_reason", "diagnostic_only",
    "production_safe", "production_promotion_blocked", "ready_for_benchmark",
    "ready_for_customer_view", "recommended_next_action", "production_status_note",
]


def _row_value(row: Any, name: str, default: Any = "") -> Any:
    return row.get(name, default) if isinstance(row, dict) else getattr(row, name, default)


def _family(row: Any) -> str:
    family = _clean(_row_value(row, "tece_family_candidate") or _row_value(row, "product_family") or _row_value(row, "family"))
    return "TECEdrainpoint S" if family.lower() == "tecedrainpoint s" else family


def _role(row: Any) -> str:
    return _clean(_row_value(row, "tece_article_role_candidate") or _row_value(row, "article_role") or _row_value(row, "role")) or "unknown"


def _article(row: Any) -> str:
    return _clean(_row_value(row, "article_number") or _row_value(row, "article") or _row_value(row, "product_article_number"))


def _source_file(row: Any) -> str:
    return _clean(_row_value(row, "source_file") or _row_value(row, "evidence_source_file"))


def _evidence_text(row: Any) -> str:
    fields = ("evidence_text", "evidence_text_snippet", "product_name", "description", "name", "classification_reason")
    return " ".join(dict.fromkeys(_clean(_row_value(row, field)) for field in fields if _clean(_row_value(row, field))))


def _snippet(text: str, candidates: list[int]) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    if candidates:
        hit = re.search(rf"(?<!\d)({'|'.join(str(v) for v in candidates)})\s*mm", text, re.I)
        if hit:
            start = max(0, hit.start() - 180)
            return text[start:hit.end() + 220][:500]
    return text[:500]


def _plausible_length(value: str) -> int | None:
    try:
        number = int(float(value.replace(",", ".")))
    except ValueError:
        return None
    return number if 600 <= number <= 1600 and number % 10 == 0 else None


def extract_length_candidates(text: str, article_number: str = "") -> tuple[list[int], bool, str]:
    """Return sorted unique plausible nominal lengths, whether nominal context exists, and reason."""
    normalized = re.sub(r"\s+", " ", text or " ").strip()
    explicit: list[int] = []
    generic: list[int] = []
    for match in re.finditer(r"(?i)\b(?:Nennlänge|Nennlaenge|nominal\s+length)\b[^.;\n]{0,80}", normalized):
        segment = match.group(0)
        for value in re.findall(r"(?<!\d)(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b", segment, re.I):
            parsed = _plausible_length(value)
            if parsed is not None:
                explicit.append(parsed)
    scrubbed = re.sub(r"(?i)\b(?:DN\s*\d{2,3}|RG\s*\d{1,4}|LE\s*\d+|\d+\s*St\.|Sperrwasserhöhe\s*[:=]?\s*\d+\s*mm|water\s*seal\s*[:=]?\s*\d+\s*mm|Breite\s*[:=]?\s*\d+\s*mm|width\s*[:=]?\s*\d+\s*mm|Aufbauhöhe\s*[:=]?\s*\d+\s*mm|installation\s*height\s*[:=]?\s*\d+\s*mm|Best\.-?Nr\.?\s*\d{5,8}|Artikel(?:nummer)?\s*[:=]?\s*\d{5,8})\b", " ", normalized)
    if article_number:
        scrubbed = scrubbed.replace(article_number, " ")
    for value in re.findall(r"(?<!\d)(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b", scrubbed, re.I):
        parsed = _plausible_length(value)
        if parsed is not None:
            generic.append(parsed)
    candidates = sorted(set(explicit or generic))
    if explicit:
        reason = "explicit Nennlänge/nominal-length context found"
    elif candidates:
        reason = "generic plausible millimeter length candidate found"
    else:
        reason = "no plausible TECEdrainprofile nominal length candidate found in evidence text"
    return candidates, bool(explicit), reason


def _diagnostic_row(row: Any, idx: int) -> dict[str, Any]:
    article = _article(row)
    evidence = _evidence_text(row)
    candidates, explicit, reason = extract_length_candidates(evidence, article)
    if len(candidates) == 1:
        status = "single_length_candidate_found"
        selected = str(candidates[0])
        confidence = "high" if explicit else "medium"
    elif len(candidates) > 1:
        status = "multiple_length_candidates_found"
        selected = ""
        confidence = "low"
    else:
        status = "no_length_candidate_found"
        selected = ""
        confidence = "low"
    return {
        "length_diagnostic_id": f"TECE-DP-LEN-{idx:06d}",
        "family": EXPECTED_FAMILY,
        "article_number": article,
        "article_role": _role(row),
        "source_file": _source_file(row),
        "evidence_text_snippet": _snippet(evidence, candidates),
        "candidate_length_values_mm": ";".join(str(v) for v in candidates),
        "selected_diagnostic_length_mm": selected,
        "extraction_status": status,
        "extraction_confidence": confidence,
        "extraction_reason": reason,
        "diagnostic_only": "true",
        "production_safe": "false",
        "production_promotion_blocked": "true",
        "ready_for_benchmark": "false",
        "ready_for_customer_view": "false",
        "recommended_next_action": RECOMMENDED_NEXT_ACTION,
        "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def validate_rows(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    counts = Counter((r.get("article_number"), r.get("article_role")) for r in rows)
    return {
        "duplicate_article_role_rows": [{"article_number": k[0], "article_role": k[1], "count": v} for k, v in sorted(counts.items()) if v > 1],
        "invalid_family_rows": [r for r in rows if _clean(r.get("family")) != EXPECTED_FAMILY],
        "invalid_role_rows": [r for r in rows if _clean(r.get("article_role")) not in TARGET_ROLES],
        "production_leakage_rows": [r for r in rows if _clean(r.get("production_safe")).lower() == "true"],
        "readiness_leakage_rows": [r for r in rows if _clean(r.get("ready_for_benchmark")).lower() == "true" or _clean(r.get("ready_for_customer_view")).lower() == "true"],
        "diagnostic_only_leakage_rows": [r for r in rows if _clean(r.get("diagnostic_only")).lower() != "true"],
    }


def build_report(source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    inventory_rows: list[Any] = []
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")
    try:
        source_report = load_source_pack(source_pack)
        inventory_rows = list(source_report.rows)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
    family_rows = [r for r in inventory_rows if _family(r) == EXPECTED_FAMILY]
    targets = [r for r in family_rows if _role(r) in TARGET_ROLES]
    targets.sort(key=lambda r: (_role(r), _article(r), _source_file(r)))
    rows = [_diagnostic_row(row, idx) for idx, row in enumerate(targets, 1)]
    gates = validate_rows(rows)
    for key, msg in (("invalid_family_rows", "non-TECEdrainprofile rows included"), ("invalid_role_rows", "roles other than drain_body/profile_cover included"), ("production_leakage_rows", "production_safe leakage found"), ("readiness_leakage_rows", "readiness leakage found"), ("diagnostic_only_leakage_rows", "diagnostic_only leakage found")):
        if gates[key]:
            errors.append(msg)
    role_counts = Counter(_role(r) for r in targets)
    if len(rows) != len(targets): errors.append("output_row_count != target_row_count")
    if len(targets) != EXPECTED_TARGET_ROW_COUNT: errors.append("target_row_count != 42")
    if role_counts.get("drain_body", 0) != EXPECTED_DRAIN_BODY_COUNT: errors.append("drain_body_count != 12")
    if role_counts.get("profile_cover", 0) != EXPECTED_PROFILE_COVER_COUNT: errors.append("profile_cover_count != 30")
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows([{c: r.get(c, "") for c in CSV_COLUMNS} for r in rows])
    status_counts = Counter(r["extraction_status"] for r in rows)
    confidence_counts = Counter(r["extraction_confidence"] for r in rows)
    selected = [r["selected_diagnostic_length_mm"] for r in rows if r["selected_diagnostic_length_mm"]]
    report = {
        "valid": not errors, "errors": errors, "warnings": warnings, "source_pack_path": str(source_pack), "family": family,
        "input_inventory_row_count": len(inventory_rows), "family_inventory_row_count": len(family_rows),
        "drain_body_count": role_counts.get("drain_body", 0), "profile_cover_count": role_counts.get("profile_cover", 0),
        "target_row_count": len(targets), "output_row_count": len(rows),
        "extraction_status_counts": dict(sorted(status_counts.items())), "extraction_confidence_counts": dict(sorted(confidence_counts.items())),
        "role_counts": dict(sorted(role_counts.items())), "rows_with_selected_length_count": len(selected),
        "rows_with_multiple_length_candidates_count": status_counts.get("multiple_length_candidates_found", 0),
        "rows_with_no_length_candidate_count": status_counts.get("no_length_candidate_found", 0),
        "selected_length_distribution": dict(sorted(Counter(selected).items())), **gates,
        "output_csv_path": str(out) if out else "", "production_safe_candidate_count": sum(1 for r in rows if r["production_safe"] == "true"),
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    return rows, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only diagnostic TECEdrainprofile length extraction report.")
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    _rows, report = build_report(args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json: write_json_output(report)
    else: write_text_output("valid: " + str(report["valid"]))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
