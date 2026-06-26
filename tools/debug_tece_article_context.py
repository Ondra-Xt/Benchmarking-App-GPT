from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print diagnostic TECE source-pack row context for selected articles.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--article", action="append", required=True)
    args = parser.parse_args(argv)
    wanted = set(args.article)
    report = load_source_pack(args.source_pack)
    for row in report.rows:
        if row.article_number in wanted:
            print("=" * 80)
            print(f"article_number: {row.article_number}")
            print(f"source_file: {row.source_file}")
            print(f"page_range_label: {row.page_range_label}")
            print(f"extraction_method: {row.extraction_method}")
            print(f"extraction_priority: {row.extraction_priority}")
            print(f"role: {row.tece_article_role_candidate}")
            print(f"nominal_length_mm: {row.nominal_length_mm}")
            print(f"finish_or_color: {row.finish_or_color}")
            print("evidence_text:")
            print(row.evidence_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
