from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _article_snippet, _candidate_terms, _clean, _pattern_key

COLUMNS = [
    "article_number",
    "product_family",
    "article_role",
    "tece_article_role_candidate",
    "extraction_method",
    "extraction_priority",
    "source_file",
    "page_range_label",
    "candidate_terms",
    "context_group",
    "evidence_text_snippet",
    "reviewer_role_decision",
    "reviewer_notes",
    "safe_to_apply_automatically",
]


def export_unknown_role_review_csv(source_pack: str | Path, out: str | Path, family: str = "TECEdrainline") -> int:
    """Write a diagnostic-only manual review CSV for unknown-role TECE rows.

    The reviewer_* and safe_to_apply_automatically columns are intentionally blank.
    This function only reads source-pack rows and writes a review aid; it does not
    mutate inventory roles, promote TECE rows, or touch canonical exports.
    """
    inventory = load_source_pack(source_pack)
    count = 0
    with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        for row in inventory.rows:
            row_family = _clean(row.tece_family_candidate or row.product_family)
            role = _clean(row.tece_article_role_candidate) or "unknown"
            if row_family != family or role != "unknown":
                continue
            snippet = _article_snippet(row.article_number, row.evidence_text)
            terms = _candidate_terms(snippet)
            writer.writerow({
                "article_number": row.article_number,
                "product_family": row.product_family,
                "article_role": row.tece_article_role_candidate,
                "tece_article_role_candidate": row.tece_article_role_candidate,
                "extraction_method": row.extraction_method,
                "extraction_priority": row.extraction_priority,
                "source_file": row.source_file,
                "page_range_label": row.page_range_label,
                "candidate_terms": "; ".join(terms),
                "context_group": _pattern_key(terms, snippet),
                "evidence_text_snippet": snippet,
                "reviewer_role_decision": "",
                "reviewer_notes": "",
                "safe_to_apply_automatically": "",
            })
            count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export diagnostic-only TECE unknown-role manual review CSV.")
    parser.add_argument("--source-pack", required=True, help="Path to a TECE source pack directory or file.")
    parser.add_argument("--out", required=True, help="Output CSV path (UTF-8 with BOM for Excel).")
    parser.add_argument("--family", default="TECEdrainline", help="TECE family to export; default: TECEdrainline.")
    args = parser.parse_args(argv)
    export_unknown_role_review_csv(args.source_pack, args.out, family=args.family)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
