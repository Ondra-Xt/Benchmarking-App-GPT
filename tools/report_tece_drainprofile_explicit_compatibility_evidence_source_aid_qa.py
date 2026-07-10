from __future__ import annotations

import argparse, csv, json, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import (  # noqa:E402
    DEFAULT_SOURCE_PACK, EXPECTED_FAMILY, EXPECTED_FAMILY_INVENTORY_ROW_COUNT,
    EXPECTED_INPUT_INVENTORY_ROW_COUNT, EXPECTED_MACHINE_ROLE_COUNTS,
    RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, _bool, _clean, _counts, _dups, _int,
)
from tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid import (  # noqa:E402
    REQUIRED_SOURCE_AID_IDS,
)
from tools.report_tece_source_inventory import load_source_pack  # noqa:E402
from tools.tece_report_output import write_json_output  # noqa:E402

QA_COLUMNS = [
    "qa_row_id", "family", "source_aid_id", "expected_source_aid_id", "article_number", "expected_article_number",
    "aid_area", "expected_aid_area", "aid_target_type", "expected_aid_target_type", "source_aid_status",
    "expected_source_aid_status", "mapping_status", "blocking_status", "qa_status", "evidence_decision_allowed",
    "evidence_complete", "ready_for_future_diagnostic_design", "compatibility_pair_generation_allowed",
    "candidate_pair_matrix_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed",
    "benchmark_ready_allowed", "customer_view_allowed", "diagnostic_only", "recommended_next_action", "production_status_note",
]
QA_ROW_IDS = [f"TECE-DP-COMPAT-EVIDENCE-SOURCE-AID-QA-{i:06d}" for i in range(1, 6)]
EXPECTED_MAPPING = [
    (REQUIRED_SOURCE_AID_IDS[0], "673001", "retained_drain_body_nominal_length_evidence", "retained_drain_body_article_lookup", "source_pack_article_context_found"),
    (REQUIRED_SOURCE_AID_IDS[1], "673002", "retained_drain_body_nominal_length_evidence", "retained_drain_body_article_lookup", "source_pack_article_context_found"),
    (REQUIRED_SOURCE_AID_IDS[2], "673003", "retained_drain_body_nominal_length_evidence", "retained_drain_body_article_lookup", "source_pack_article_context_found"),
    (REQUIRED_SOURCE_AID_IDS[3], "", "article_level_drain_body_to_profile_cover_compatibility_matrix", "compatibility_matrix_source_search_pointer", "explicit_compatibility_matrix_not_found_in_current_source_pack"),
    (REQUIRED_SOURCE_AID_IDS[4], "", "profile_cover_scope_confirmation", "profile_cover_scope_summary", "source_pack_profile_cover_scope_summary_available"),
]
FALSE_FIELDS = ["evidence_decision_allowed", "evidence_complete", "ready_for_future_diagnostic_design", "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed"]
ZERO_FIELDS = ["generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
REPORT_EXPECTED = {
    "evidence_template_row_count": 5, "validation_evidence_csv_row_count": 5, "validation_template_csv_row_count": 5,
    "validation_template_report_row_count": 5, "source_aid_row_count": 5, "retained_drain_body_source_aid_row_count": 3,
    "retained_drain_body_source_pack_found_count": 3, "compatibility_matrix_source_aid_row_count": 1,
    "explicit_compatibility_matrix_found_count": 0, "profile_cover_scope_source_aid_row_count": 1, "source_pack_profile_cover_count": 30,
}
RECOMMENDED_NEXT_ACTION = "QA passed for diagnostic source-aid output; use it only as a pointer for manual evidence collection and validate the filled evidence template separately before any future diagnostic design."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile source-aid QA report; no evidence acceptance; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This source-aid QA report is read-only and diagnostic-only. It validates that the TECEdrainprofile source-aid export contains only expected diagnostic source pointers and confirms that it does not accept evidence, generate compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."

def _read_csv(path: str | Path, errors: list[str]) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh: return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"source-aid CSV missing or cannot be loaded: {exc}"); return []

def _read_json(path: str | Path, label: str, errors: list[str]) -> dict[str, Any]:
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return {}

def _source_pack(source_pack: str | Path, family: str, errors: list[str]):
    try: rows = list(load_source_pack(source_pack).rows or [])
    except Exception as exc: errors.append(f"source-pack cannot be loaded: {exc}"); return [], [], {}
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    roles = [_clean(getattr(r, "tece_article_role_candidate", "")) or _clean(getattr(r, "article_role", "")) or "unknown" for r in fam]
    return rows, fam, _counts(roles)

def _row_ref(row: dict[str, str], idx: int) -> str: return _clean(row.get("source_aid_id")) or str(idx)

def _build_rows(source_rows: list[dict[str, str]], family: str) -> list[dict[str, str]]:
    out=[]
    for i, expected in enumerate(EXPECTED_MAPPING):
        actual = source_rows[i] if i < len(source_rows) else {}
        sid, article, area, target, status = expected
        mapping_ok = (_clean(actual.get("source_aid_id")), _clean(actual.get("article_number")), _clean(actual.get("aid_area")), _clean(actual.get("aid_target_type")), _clean(actual.get("source_aid_status"))) == expected
        row = {c:"" for c in QA_COLUMNS}
        row.update({"qa_row_id": QA_ROW_IDS[i], "family": family, "source_aid_id": _clean(actual.get("source_aid_id")), "expected_source_aid_id": sid, "article_number": _clean(actual.get("article_number")), "expected_article_number": article, "aid_area": _clean(actual.get("aid_area")), "expected_aid_area": area, "aid_target_type": _clean(actual.get("aid_target_type")), "expected_aid_target_type": target, "source_aid_status": _clean(actual.get("source_aid_status")), "expected_source_aid_status": status, "mapping_status": "pass" if mapping_ok else "fail"})
        for f in FALSE_FIELDS: row[f] = "false"
        row["diagnostic_only"] = "true"
        row["blocking_status"] = "pass"
        row["qa_status"] = "pass" if row["mapping_status"] == "pass" else "fail"
        row["recommended_next_action"] = RECOMMENDED_NEXT_ACTION; row["production_status_note"] = PRODUCTION_STATUS_NOTE
        out.append(row)
    return out

def build_qa(source_aid_csv: str | Path, source_aid_report: str | Path, evidence_validation_report: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, qa_rows_override: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors=[]; warnings=[]
    source_rows = _read_csv(source_aid_csv, errors)
    source_report = _read_json(source_aid_report, "source-aid report", errors)
    validation = _read_json(evidence_validation_report, "evidence validation report", errors)
    inv_rows, fam_rows, role_counts = _source_pack(source_pack, family, errors)
    qa_rows = qa_rows_override if qa_rows_override is not None else _build_rows(source_rows, family)
    qa_ids=[_clean(r.get("qa_row_id")) for r in qa_rows]; ids=[_clean(r.get("source_aid_id")) for r in source_rows]
    dup_qa=_dups(qa_ids); dup_ids=_dups(ids)
    drain_articles=[_clean(r.get("article_number")) for r in source_rows if _clean(r.get("aid_target_type"))=="retained_drain_body_article_lookup"]
    dup_article=_dups(drain_articles)
    invalid_family=[_row_ref(r,i) for i,r in enumerate(source_rows,1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_source=[_row_ref(r,i) for i,r in enumerate(source_rows,1) if _clean(r.get("source_aid_id")) not in REQUIRED_SOURCE_AID_IDS]
    invalid_mapping=[_clean(r.get("qa_row_id")) or str(i) for i,r in enumerate(qa_rows,1) if _clean(r.get("mapping_status")) != "pass"]
    invalid_blocking=[_clean(r.get("qa_row_id")) or str(i) for i,r in enumerate(qa_rows,1) if _clean(r.get("blocking_status")) != "pass" or any(_bool(r.get(f)) for f in FALSE_FIELDS) or not _bool(r.get("diagnostic_only"))]
    production_leak=[_clean(r.get("qa_row_id")) or str(i) for i,r in enumerate(qa_rows,1) if any(_bool(r.get(f)) for f in ["compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed"])]
    readiness_leak=[_clean(r.get("qa_row_id")) or str(i) for i,r in enumerate(qa_rows,1) if any(_bool(r.get(f)) for f in ["ready_for_future_diagnostic_design","benchmark_ready_allowed","customer_view_allowed"])]
    diag_leak=[_clean(r.get("qa_row_id")) or str(i) for i,r in enumerate(qa_rows,1) if not _bool(r.get("diagnostic_only"))]
    counts={f:sum(_bool(r.get(f)) for r in qa_rows) for f in FALSE_FIELDS}
    gen={f:_int(source_report.get(f)) for f in ZERO_FIELDS}
    checks=[
        (source_report.get("valid") is True,"source-aid report valid must be true"), (validation.get("valid") is True,"evidence validation report valid must be true"),
        (family==EXPECTED_FAMILY and source_report.get("family")==EXPECTED_FAMILY,"family must be TECEdrainprofile"),
        (len(inv_rows)==EXPECTED_INPUT_INVENTORY_ROW_COUNT and len(fam_rows)==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and role_counts==EXPECTED_MACHINE_ROLE_COUNTS and _int(source_report.get("input_inventory_row_count"))==EXPECTED_INPUT_INVENTORY_ROW_COUNT and _int(source_report.get("family_inventory_row_count"))==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and source_report.get("current_machine_role_counts")==EXPECTED_MACHINE_ROLE_COUNTS,"source-pack baseline counts differ from expected"),
        (len(source_rows)==5,"source-aid CSV must contain exactly 5 rows"), (_int(source_report.get("source_aid_row_count"))==5,"source-aid report row count must be 5"),
        (ids==REQUIRED_SOURCE_AID_IDS,"required source_aid_ids are missing or out of order"), (len(qa_rows)==5,"QA row count must be 5"), (qa_ids==QA_ROW_IDS,"required QA row IDs are missing or out of order"),
        (not dup_qa and not dup_ids and not dup_article,"duplicate QA/source-aid/article rows detected"), (not invalid_family and not invalid_source and not invalid_mapping,"invalid source-aid or mapping rows detected"),
        (not invalid_blocking and not production_leak and not readiness_leak and not diag_leak,"production/readiness/diagnostic leakage detected"),
        (all(v==0 for v in gen.values()),"generation/mutation/promotion counters must be 0"),
        (source_report.get("retained_drain_body_article_numbers")==RETAINED_DRAIN_BODY_ARTICLE_NUMBERS,"retained drain body article numbers differ"),
        (all(_int(source_report.get(k))==v for k,v in REPORT_EXPECTED.items()),"source-aid report values differ from expected"),
        (source_report.get("production_promotion_blocked") is True and source_report.get("ready_for_benchmark") is False and source_report.get("ready_for_customer_view") is False,"production/readiness report gates differ"),
    ]
    for ok,msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report={"valid":not errors,"errors":errors,"warnings":warnings,"family":family,"source_pack_path":str(source_pack),"source_aid_csv_path":str(source_aid_csv),"source_aid_report_path":str(source_aid_report),"evidence_validation_report_path":str(evidence_validation_report),"source_aid_report_valid":source_report.get("valid") is True,"evidence_validation_report_valid":validation.get("valid") is True,"input_inventory_row_count":len(inv_rows),"family_inventory_row_count":len(fam_rows),"current_machine_role_counts":role_counts,"source_aid_csv_row_count":len(source_rows),"source_aid_report_row_count":_int(source_report.get("source_aid_row_count")),"qa_row_count":len(qa_rows),"source_aid_ids":ids,"qa_row_ids":qa_ids,"aid_area_counts":_counts([_clean(r.get("aid_area")) for r in source_rows]),"aid_target_type_counts":_counts([_clean(r.get("aid_target_type")) for r in source_rows]),"source_aid_status_counts":_counts([_clean(r.get("source_aid_status")) for r in source_rows]),"retained_drain_body_article_numbers":source_report.get("retained_drain_body_article_numbers"),"retained_drain_body_source_aid_row_count":sum(_clean(r.get("aid_target_type"))=="retained_drain_body_article_lookup" for r in source_rows),"retained_drain_body_source_pack_found_count":sum(_clean(r.get("aid_target_type"))=="retained_drain_body_article_lookup" and _bool(r.get("source_pack_row_found")) for r in source_rows),"compatibility_matrix_source_aid_row_count":sum(_clean(r.get("aid_target_type"))=="compatibility_matrix_source_search_pointer" for r in source_rows),"explicit_compatibility_matrix_found_count":_int(source_report.get("explicit_compatibility_matrix_found_count")),"profile_cover_scope_source_aid_row_count":sum(_clean(r.get("aid_target_type"))=="profile_cover_scope_summary" for r in source_rows),"source_pack_profile_cover_count":_int(source_report.get("source_pack_profile_cover_count")),"mapping_status_counts":_counts([_clean(r.get("mapping_status")) for r in qa_rows]),"blocking_status_counts":_counts([_clean(r.get("blocking_status")) for r in qa_rows]),"qa_status_counts":_counts([_clean(r.get("qa_status")) for r in qa_rows]),**{f"{f}_count":v for f,v in counts.items()},**gen,"duplicate_qa_row_ids":dup_qa,"duplicate_source_aid_ids":dup_ids,"duplicate_article_source_aid_rows":dup_article,"invalid_family_rows":invalid_family,"invalid_source_aid_rows":invalid_source,"invalid_mapping_rows":invalid_mapping,"invalid_blocking_rows":invalid_blocking,"production_leakage_rows":production_leak,"readiness_leakage_rows":readiness_leak,"diagnostic_only_leakage_rows":diag_leak,"production_promotion_blocked":source_report.get("production_promotion_blocked"),"ready_for_benchmark":source_report.get("ready_for_benchmark"),"ready_for_customer_view":source_report.get("ready_for_customer_view"),"output_csv_path":str(out) if out else None,"diagnostic_only_note":DIAGNOSTIC_ONLY_NOTE}
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w",encoding="utf-8",newline="") as fh:
            w=csv.DictWriter(fh, fieldnames=QA_COLUMNS, extrasaction="ignore"); w.writeheader(); w.writerows(qa_rows)
    return qa_rows, report

def main(argv: list[str] | None=None) -> int:
    p=argparse.ArgumentParser(description="Report read-only TECEdrainprofile explicit compatibility evidence source-aid QA.")
    p.add_argument("--source-aid-csv", required=True); p.add_argument("--source-aid-report", required=True); p.add_argument("--evidence-validation-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); _rows, report=build_qa(a.source_aid_csv,a.source_aid_report,a.evidence_validation_report,a.source_pack,a.family,a.out)
    write_json_output(report,out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1
if __name__ == "__main__": raise SystemExit(main())
