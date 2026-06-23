from __future__ import annotations

import argparse
import io
import json
import sys
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.tece_report_output import write_json_output, write_text_output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only diagnostic TECE source-pack classification report.")
    parser.add_argument("--source-pack", required=True, help="Path to local TECE source-pack files.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", help="Write report output to this UTF-8 path instead of stdout.")
    args = parser.parse_args(argv)

    report = load_source_pack(args.source_pack)
    payload = {
        "source_pack_path": report.source_pack_path,
        "source_pack_candidate_count": report.source_pack_candidate_count,
        "production_promotion_blocked": report.production_promotion_blocked,
        "ready_for_benchmark": report.ready_for_benchmark,
        "ready_for_customer_view": report.ready_for_customer_view,
        "source_pack_classification_summary": report.source_pack_classification_summary,
        "page_range_label_counts": report.page_range_label_counts,
        "rows": [asdict(row) for row in report.rows],
    }
    if args.json:
        write_json_output(payload, args.out)
    else:
        stream = io.StringIO()
        print("TECE source-pack classification (diagnostic-only)", file=stream)
        print(f"source_pack_candidate_count: {report.source_pack_candidate_count}", file=stream)
        print(f"production_promotion_blocked: {report.production_promotion_blocked}", file=stream)
        print(f"ready_for_benchmark: {report.ready_for_benchmark}", file=stream)
        print(f"ready_for_customer_view: {report.ready_for_customer_view}", file=stream)
        print(f"page_range_label_counts: {json.dumps(report.page_range_label_counts or {}, sort_keys=True)}", file=stream)
        print("source_pack_classification_summary:", file=stream)
        print(json.dumps(report.source_pack_classification_summary or {}, indent=2, sort_keys=True), file=stream)
        for row in report.rows:
            print(
                f"- {row.source_file}: article={row.article_number} role={row.tece_article_role_candidate} "
                f"family={row.tece_family_candidate} confidence={row.classification_confidence} "
                f"blocked_by={row.production_blocking_reason}",
                file=stream,
            )
        write_text_output(stream.getvalue().rstrip("\n"), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
