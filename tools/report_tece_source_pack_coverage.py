from __future__ import annotations

import argparse
import io
import json
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import SOURCE_PACK_TECHNICAL_FIELDS, load_source_pack
from tools.tece_report_output import write_json_output, write_text_output

FAMILIES = ["TECEdrainway", "TECEdrainprofile", "TECEdrainline", "TECEdrainpoint S"]
SENTINELS = {
    "TECEdrainline": {"600906", "601006", "601206", "601506"},
    "TECEdrainpoint S": {"3601050"},
    "TECEdrainprofile": {"670821", "670921", "671021", "671221", "671621", "673001", "673002", "673003"},
}


def _family_key(value: str) -> str:
    if "point" in value.lower():
        return "TECEdrainpoint S"
    for family in FAMILIES:
        if family.lower().replace(" s", "") in value.lower():
            return family
    return value or "unknown"


def build_coverage_report(source_pack: str | Path) -> dict[str, Any]:
    report = load_source_pack(source_pack)
    rows = list(report.rows)
    manifest_sources = (report.manifest or {}).get("sources", []) if report.manifest else []
    manifest_by_family: dict[str, dict[str, Any]] = {}
    for src in manifest_sources:
        label = " ".join(str(src.get(k, "")) for k in ("product_family_hint", "page_range_label", "document_title"))
        fam = _family_key(label)
        if fam in FAMILIES:
            item = manifest_by_family.setdefault(fam, {"page_start": src.get("page_start", ""), "page_end": src.get("page_end", ""), "page_range_labels": []})
            if src.get("page_range_label"):
                item["page_range_labels"].append(src.get("page_range_label"))
    families: dict[str, Any] = {}
    for family in FAMILIES:
        fam_rows = [row for row in rows if _family_key(row.tece_family_candidate or row.product_family or row.page_range_label) == family]
        article_numbers = sorted({row.article_number for row in fam_rows})
        labels = sorted({str(row.catalogue_page_label) for row in fam_rows if row.catalogue_page_label})
        warnings = []
        if not fam_rows:
            warnings.append("no_extracted_rows_for_family_page_range")
        missing = sorted(SENTINELS.get(family, set()) - set(article_numbers))
        if missing:
            warnings.append("missing_sentinel_articles")
        tech = {field: sum(1 for row in fam_rows if str(getattr(row, field, "") or "").strip() != "") for field in SOURCE_PACK_TECHNICAL_FIELDS}
        families[family] = {
            "manifest_page_start": manifest_by_family.get(family, {}).get("page_start", ""),
            "manifest_page_end": manifest_by_family.get(family, {}).get("page_end", ""),
            "manifest_page_range_labels": sorted(set(manifest_by_family.get(family, {}).get("page_range_labels", []))),
            "extracted_row_count": len(fam_rows),
            "unique_article_count": len(article_numbers),
            "role_counts": dict(sorted(Counter(row.tece_article_role_candidate or "unknown" for row in fam_rows).items())),
            "missing_sentinel_article_numbers": missing,
            "technical_field_coverage": tech,
            "extraction_warnings": warnings,
            "catalogue_page_labels": labels,
            "article_numbers_sample": article_numbers[:50],
        }
    return {
        "source_pack_path": report.source_pack_path,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "production_safe_candidate_count": 0,
        "source_pack_candidate_count": report.source_pack_candidate_count,
        "families": families,
        "summary_by_family": families,
        "report_note": "Diagnostic-only full source-pack coverage report; partial extraction warnings indicate sections needing audit before shortlist review.",
    }


def _text_report(payload: dict[str, Any]) -> str:
    stream = io.StringIO()
    print("TECE source-pack coverage (diagnostic-only)", file=stream)
    for family, data in payload["families"].items():
        print(f"{family}: rows={data['extracted_row_count']} unique={data['unique_article_count']} warnings={data['extraction_warnings']}", file=stream)
    return stream.getvalue().rstrip("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report diagnostic-only TECE source-pack extraction coverage by drainage family.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    payload = build_coverage_report(args.source_pack)
    if args.json:
        write_json_output(payload, args.out)
    else:
        write_text_output(_text_report(payload), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
