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

from tools.report_tece_source_inventory import load_source_pack  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402

DEFAULT_SOURCE_PACK = Path("local_source_packs/tece/pilot_001")
EXPECTED_FAMILY = "TECEdrainprofile"
EXPECTED_INPUT_INVENTORY_ROW_COUNT = 436
EXPECTED_FAMILY_INVENTORY_ROW_COUNT = 76
EXPECTED_MACHINE_ROLE_COUNTS = {"accessory": 5, "complete_set": 4, "drain_body": 12, "profile_cover": 30, "unknown": 25}
RETAINED_DRAIN_BODY_ARTICLE_NUMBERS = ["673001", "673002", "673003"]
REQUIRED_REQUIREMENT_IDS = [f"TECE-DP-COMPAT-EVIDENCE-REQ-{i:06d}" for i in range(1, 6)]
REQUIRED_TEMPLATE_IDS = [f"TECE-DP-COMPAT-EVIDENCE-TEMPLATE-{i:06d}" for i in range(1, 6)]
MANUAL_EVIDENCE_FIELDS = [
    "source_document_name", "source_document_version", "source_page_or_section", "source_url_or_path",
    "source_text_excerpt", "reviewed_evidence_summary", "reviewer_decision", "reviewer_notes",
    "safe_to_use_for_future_diagnostic_design",
]
BLOCKING_FIELDS = [
    "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed",
    "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed",
]
CSV_COLUMNS = [
    "template_row_id", "family", "requirement_id", "evidence_collection_area", "evidence_target_type",
    "article_number", "article_scope", "evidence_status", "evidence_required", "required_evidence_type",
    "accepted_source_examples", "rejected_inference_methods", "source_document_name", "source_document_version",
    "source_page_or_section", "source_url_or_path", "source_text_excerpt", "reviewed_evidence_summary",
    "reviewer_decision", "reviewer_notes", "safe_to_use_for_future_diagnostic_design",
    "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed",
    "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed",
    "diagnostic_only", "recommended_next_action", "production_status_note",
]
RECOMMENDED_NEXT_ACTION = "Manually collect explicit TECEdrainprofile source evidence in the blank evidence fields, then validate it with a separate diagnostic validator before any future pairing design; do not generate pair rows, mutate source-pack, or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit compatibility evidence collection template; manual fields are blank; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This evidence collection template is read-only and diagnostic-only. It exports blank manual evidence collection rows for TECEdrainprofile retained drain-body length evidence, explicit compatibility evidence, and profile-cover scope confirmation, and confirms that compatibility pair generation, candidate pair matrix generation, length overlay, source-pack mutation, TECE logic change, ACO export change, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion remain blocked. Production promotion remains blocked."


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _clean(v).lower() in {"1", "true", "yes", "y"}


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _counts(vals: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(vals).items()))


def _dups(vals: list[str]) -> list[str]:
    c = Counter(vals)
    return sorted(k for k, v in c.items() if k and v > 1)


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"requirements report missing or cannot be loaded: {exc}")
        return {}


def _read_csv(path: str | Path, errors: list[str]) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"requirements CSV missing or cannot be loaded: {exc}")
        return []


def _source_pack_counts(source_pack: str | Path, family: str, errors: list[str]) -> tuple[int, int, dict[str, int]]:
    try:
        report = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return 0, 0, {}
    rows = list(getattr(report, "rows", []) or [])
    family_rows = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    roles = [_clean(getattr(r, "tece_article_role_candidate", "")) or _clean(getattr(r, "article_role", "")) or "unknown" for r in family_rows]
    return len(rows), len(family_rows), _counts(roles)


def _base_row(template_id: str, req_id: str, family: str, area: str, target: str, article: str, scope: str, required: str, evidence_type: str, accepted: str, rejected: str) -> dict[str, str]:
    row = {c: "" for c in CSV_COLUMNS}
    row.update({
        "template_row_id": template_id, "family": family, "requirement_id": req_id,
        "evidence_collection_area": area, "evidence_target_type": target, "article_number": article,
        "article_scope": scope, "evidence_status": "needs_manual_evidence", "evidence_required": required,
        "required_evidence_type": evidence_type, "accepted_source_examples": accepted,
        "rejected_inference_methods": rejected, "diagnostic_only": "true",
        "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE,
    })
    for field in BLOCKING_FIELDS:
        row[field] = "false"
    return row


def template_rows(family: str = EXPECTED_FAMILY) -> list[dict[str, str]]:
    rows = []
    for idx, article in enumerate(RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, 1):
        rows.append(_base_row(
            f"TECE-DP-COMPAT-EVIDENCE-TEMPLATE-{idx:06d}", "TECE-DP-COMPAT-EVIDENCE-REQ-000001", family,
            "retained_drain_body_nominal_length_evidence", "retained_drain_body", article, article,
            f"explicit nominal length evidence for retained diagnostic drain body {article}, or explicit source confirmation that this drain body is length-independent",
            "explicit_catalog_or_datasheet_nominal_length_or_length_independence_statement",
            "TECE catalog row/page; TECE datasheet; official TECE article page; official technical drawing with article-specific length or length-independence statement",
            "article-number inference; nearby-row inference; profile-cover length inheritance; DN/water-seal/height inference",
        ))
    rows.append(_base_row(
        "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000004", "TECE-DP-COMPAT-EVIDENCE-REQ-000002", family,
        "article_level_drain_body_to_profile_cover_compatibility_matrix", "compatibility_matrix_or_statement", "",
        "retained_drain_bodies_673001_673002_673003_to_tecedrainprofile_profile_covers",
        "explicit TECEdrainprofile drain-body-to-profile-cover compatibility matrix or article-level compatibility statement",
        "explicit_article_level_compatibility_matrix_or_statement",
        "TECE compatibility matrix; official catalog compatibility table; official datasheet statement naming article-level drain body and profile cover compatibility",
        "same family inference; same length inference; section proximity inference; generic profile-cover compatibility assumption",
    ))
    rows.append(_base_row(
        "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000005", "TECE-DP-COMPAT-EVIDENCE-REQ-000003", family,
        "profile_cover_scope_confirmation", "profile_cover_scope", "", "30_tecedrainprofile_profile_cover_rows",
        "confirmation that the 30 source-pack TECEdrainprofile profile_cover rows are complete and are the correct cover scope for future compatibility evidence evaluation",
        "source_pack_scope_confirmation_against_catalog_section",
        "TECE catalog TECEdrainprofile cover/profile section; official TECE product listing; official catalog table covering all profile cover rows",
        "generated pair matrix; production promotion; inferred compatibility",
    ))
    return rows


def _count_true(rows: list[dict[str, Any]], field: str) -> int:
    return sum(_bool(r.get(field)) for r in rows)


def build_evidence_collection_template(requirements_report: str | Path, requirements_csv: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, rows: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors: list[str] = []; warnings: list[str] = []
    req_report = _read_json(requirements_report, errors)
    req_csv_rows = _read_csv(requirements_csv, errors)
    input_count, family_count, machine_counts = _source_pack_counts(source_pack, family, errors)
    tmpl_rows = list(rows) if rows is not None else template_rows(family)

    req_csv_ids = [_clean(r.get("requirement_id")) for r in req_csv_rows]
    template_ids = [_clean(r.get("template_row_id")) for r in tmpl_rows]
    areas = [_clean(r.get("evidence_collection_area")) for r in tmpl_rows]
    targets = [_clean(r.get("evidence_target_type")) for r in tmpl_rows]
    statuses = [_clean(r.get("evidence_status")) for r in tmpl_rows]
    duplicate_template_ids = _dups(template_ids)
    article_keys = [_clean(r.get("article_number")) for r in tmpl_rows if _clean(r.get("evidence_target_type")) == "retained_drain_body"]
    duplicate_article_rows = _dups(article_keys)
    manual_prefilled = [
        {"template_row_id": _clean(r.get("template_row_id")), "field": f, "value": _clean(r.get(f))}
        for r in tmpl_rows for f in MANUAL_EVIDENCE_FIELDS if _clean(r.get(f))
    ]
    invalid_family_rows = [_clean(r.get("template_row_id")) or str(i) for i, r in enumerate(tmpl_rows, 1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_template_rows = [_clean(r.get("template_row_id")) or str(i) for i, r in enumerate(tmpl_rows, 1) if _clean(r.get("template_row_id")) not in REQUIRED_TEMPLATE_IDS or _clean(r.get("requirement_id")) not in REQUIRED_REQUIREMENT_IDS or _clean(r.get("evidence_status")) != "needs_manual_evidence"]
    invalid_manual_field_rows = sorted({_clean(x["template_row_id"]) for x in manual_prefilled})
    invalid_blocking_rows = [_clean(r.get("template_row_id")) or str(i) for i, r in enumerate(tmpl_rows, 1) if any(_bool(r.get(f)) for f in BLOCKING_FIELDS)]
    production_leakage_rows = invalid_blocking_rows[:]
    readiness_leakage_rows = [_clean(r.get("template_row_id")) or str(i) for i, r in enumerate(tmpl_rows, 1) if _bool(r.get("benchmark_ready_allowed")) or _bool(r.get("customer_view_allowed"))]
    diagnostic_only_leakage_rows = [_clean(r.get("template_row_id")) or str(i) for i, r in enumerate(tmpl_rows, 1) if not _bool(r.get("diagnostic_only"))]

    generated_pairs = 0; generated_matrix = 0; generated_length_overlay = 0; proposed_mutations = 0; production_safe = 0
    report_generated_fields = ["generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
    report_values = {f: _int(req_report.get(f)) for f in report_generated_fields}

    checks = [
        (req_report.get("valid") is True, "requirements report valid must be true"),
        (family == EXPECTED_FAMILY and req_report.get("family") == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (input_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT and family_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts == EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (len(req_csv_rows) == 5 and set(REQUIRED_REQUIREMENT_IDS).issubset(req_csv_ids), "requirements CSV must contain exactly 5 rows and required requirement IDs"),
        (_int(req_report.get("requirement_row_count")) == 5, "requirements report requirement_row_count must be 5"),
        (req_report.get("retained_drain_body_article_numbers") == RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, "retained_drain_body_article_numbers must be exactly 673001/673002/673003"),
        (_int(req_report.get("retained_drain_body_article_count")) == 3, "retained_drain_body_article_count must be 3"),
        (_int(req_report.get("source_pack_profile_cover_count")) == 30, "source_pack_profile_cover_count must be 30"),
        (_int(req_report.get("reviewed_effective_drain_body_count")) == 3, "reviewed_effective_drain_body_count must be 3"),
        (_int(req_report.get("reviewed_effective_accessory_count")) == 8, "reviewed_effective_accessory_count must be 8"),
        (_int(req_report.get("reviewed_effective_profile_cover_count")) == 1, "reviewed_effective_profile_cover_count must be 1"),
        (_int(req_report.get("retained_unresolved_length_count")) == 3, "retained_unresolved_length_count must be 3"),
        (_int(req_report.get("diagnostic_reclassified_from_unknown_count")) == 9, "diagnostic_reclassified_from_unknown_count must be 9"),
        (_int(req_report.get("explicit_article_level_compatibility_evidence_count")) == 0, "explicit_article_level_compatibility_evidence_count must be 0"),
        (_int(req_report.get("required_evidence_area_count")) == 5, "required_evidence_area_count must be 5"),
        (all(_int(req_report.get(f)) == 0 for f in ["pairing_design_allowed_after_collection_count", "compatibility_pair_generation_allowed_count", "candidate_pair_matrix_allowed_count", "length_overlay_allowed_count", "source_pack_mutation_allowed_count", "production_promotion_allowed_count", "benchmark_ready_allowed_count", "customer_view_allowed_count"]), "requirements report allowed counters must be 0"),
        (len(tmpl_rows) == 5, "template_row_count must be 5"),
        (set(REQUIRED_TEMPLATE_IDS).issubset(template_ids), "required template IDs are missing"),
        (not duplicate_template_ids, "duplicate template row IDs detected"),
        (not duplicate_article_rows, "duplicate retained drain body article template rows detected"),
        (not manual_prefilled, "manual evidence fields must be blank"),
        (not invalid_blocking_rows and not readiness_leakage_rows and not diagnostic_only_leakage_rows, "production/readiness/diagnostic leakage detected"),
        (all(v == 0 for v in report_values.values()), "generation/mutation/promotion counters must be 0"),
        (req_report.get("production_promotion_blocked") is True, "production_promotion_blocked must be true"),
        (req_report.get("ready_for_benchmark") is False, "ready_for_benchmark must be false"),
        (req_report.get("ready_for_customer_view") is False, "ready_for_customer_view must be false"),
        (not invalid_family_rows and not invalid_template_rows, "invalid template rows detected"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows(tmpl_rows)

    report = {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family,
        "source_pack_path": str(source_pack), "requirements_report_path": str(requirements_report), "requirements_csv_path": str(requirements_csv),
        "requirements_report_valid": req_report.get("valid") is True,
        "input_inventory_row_count": input_count, "family_inventory_row_count": family_count, "current_machine_role_counts": machine_counts,
        "requirements_csv_row_count": len(req_csv_rows), "requirement_row_count": _int(req_report.get("requirement_row_count")),
        "template_row_count": len(tmpl_rows), "template_row_ids": template_ids,
        "evidence_collection_area_counts": _counts(areas), "evidence_target_type_counts": _counts(targets), "evidence_status_counts": _counts(statuses),
        "retained_drain_body_article_numbers": req_report.get("retained_drain_body_article_numbers") or [],
        "retained_drain_body_template_row_count": targets.count("retained_drain_body"),
        "compatibility_matrix_template_row_count": targets.count("compatibility_matrix_or_statement"),
        "profile_cover_scope_template_row_count": targets.count("profile_cover_scope"),
        "manual_evidence_fields_blank_count": sum(1 for r in tmpl_rows for f in MANUAL_EVIDENCE_FIELDS if not _clean(r.get(f))),
        "manual_evidence_fields_prefilled_rows": manual_prefilled,
        "source_pack_profile_cover_count": _int(req_report.get("source_pack_profile_cover_count")),
        "explicit_article_level_compatibility_evidence_count": _int(req_report.get("explicit_article_level_compatibility_evidence_count")),
        "compatibility_pair_generation_allowed_count": _count_true(tmpl_rows, "compatibility_pair_generation_allowed"),
        "candidate_pair_matrix_allowed_count": _count_true(tmpl_rows, "candidate_pair_matrix_allowed"),
        "length_overlay_allowed_count": _count_true(tmpl_rows, "length_overlay_allowed"),
        "source_pack_mutation_allowed_count": _count_true(tmpl_rows, "source_pack_mutation_allowed"),
        "production_promotion_allowed_count": _count_true(tmpl_rows, "production_promotion_allowed"),
        "benchmark_ready_allowed_count": _count_true(tmpl_rows, "benchmark_ready_allowed"),
        "customer_view_allowed_count": _count_true(tmpl_rows, "customer_view_allowed"),
        "generated_compatibility_pair_count": generated_pairs + report_values["generated_compatibility_pair_count"],
        "generated_candidate_pair_matrix_row_count": generated_matrix + report_values["generated_candidate_pair_matrix_row_count"],
        "generated_length_overlay_row_count": generated_length_overlay + report_values["generated_length_overlay_row_count"],
        "proposed_source_pack_mutation_count": proposed_mutations + report_values["proposed_source_pack_mutation_count"],
        "production_safe_candidate_count": production_safe + report_values["production_safe_candidate_count"],
        "duplicate_template_row_ids": duplicate_template_ids, "duplicate_article_template_rows": duplicate_article_rows,
        "invalid_family_rows": invalid_family_rows, "invalid_template_rows": invalid_template_rows,
        "invalid_manual_field_rows": invalid_manual_field_rows, "invalid_blocking_rows": invalid_blocking_rows,
        "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "production_promotion_blocked": req_report.get("production_promotion_blocked"),
        "ready_for_benchmark": req_report.get("ready_for_benchmark"), "ready_for_customer_view": req_report.get("ready_for_customer_view"),
        "output_csv_path": str(out) if out else None, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    report["valid"] = not errors
    return tmpl_rows, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export read-only TECEdrainprofile explicit compatibility evidence collection template.")
    parser.add_argument("--requirements-report", required=True)
    parser.add_argument("--requirements-csv", required=True)
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    _rows, report = build_evidence_collection_template(args.requirements_report, args.requirements_csv, args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
