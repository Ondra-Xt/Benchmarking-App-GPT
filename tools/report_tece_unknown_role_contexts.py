from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.tece_report_output import write_json_output, write_text_output


TERM_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Rost", re.compile(r"\b(?:Rost|Designrost)\b", re.I)),
    ("Abdeckung", re.compile(r"\b(?:Abdeckung|Designabdeckung|cover|grate)\b", re.I)),
    ("Ablaufkörper", re.compile(r"\b(?:Ablaufkörper|Ablaufkoerper)\b", re.I)),
    ("Ablaufset", re.compile(r"\b(?:Ablaufset|Komplettset|Komplett-Set|complete\s+set)\b", re.I)),
    ("Ablauf", re.compile(r"\bAblauf\b", re.I)),
    ("Zubehör", re.compile(r"\b(?:Zubehör|accessory)\b", re.I)),
    ("Ersatzteil", re.compile(r"\b(?:Ersatzteil|spare\s+part)\b", re.I)),
    ("Montagefüße", re.compile(r"\b(?:Montagefüße|Montagefuesse)\b", re.I)),
    ("Dichtband", re.compile(r"\bDichtband\b", re.I)),
    ("Schallschutz", re.compile(r"\bSchallschutz(?:matte|streifen)?\b", re.I)),
)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_article(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _article_snippet(article_number: str, evidence_text: str, width: int = 260) -> str:
    text = _clean(evidence_text)
    article = _normalize_article(article_number)
    if not text or not article:
        return text[:width]
    article_pattern = r"\s*".join(re.escape(char) for char in article)
    match = re.search(article_pattern, text)
    if not match:
        return text[:width]
    start = max(0, match.start() - width // 2)
    end = min(len(text), match.end() + width // 2)
    snippet = text[start:end].strip()
    relative_article_start = match.start() - start
    tece_starts = [m.start() for m in re.finditer(r"(?i)\bTECEdrain", snippet[:relative_article_start])]
    if tece_starts:
        snippet = snippet[tece_starts[-1]:]
    return snippet


def _candidate_terms(snippet: str) -> list[str]:
    return [label for label, pattern in TERM_PATTERNS if pattern.search(snippet)]


def _pattern_key(candidate_terms: list[str], snippet: str) -> str:
    if candidate_terms:
        return "candidate_terms:" + "|".join(candidate_terms)
    lowered = snippet.lower()
    if "duschrinne" in lowered:
        return "ambiguous:duschrinne_only_or_no_role_term"
    if "product table row" in lowered or "best.-nr" in lowered or "best-nr" in lowered:
        return "no_candidate_terms:table_row"
    return "no_candidate_terms:other"


def build_unknown_role_context_report(source_pack: str | Path, family: str = "TECEdrainline", max_examples_per_group: int = 5) -> dict[str, Any]:
    inventory = load_source_pack(source_pack)
    unknown_rows = [
        row for row in inventory.rows
        if _clean(row.tece_family_candidate or row.product_family) == family
        and (_clean(row.tece_article_role_candidate) or "unknown") == "unknown"
    ]

    groups: dict[str, dict[str, Any]] = {}
    term_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()

    for row in unknown_rows:
        snippet = _article_snippet(row.article_number, row.evidence_text)
        terms = _candidate_terms(snippet)
        term_counter.update(terms or ["(none)"])
        method_counter.update([_clean(row.extraction_method) or "unknown"])
        key = _pattern_key(terms, snippet)
        group = groups.setdefault(key, {"count": 0, "candidate_terms": terms, "examples": []})
        group["count"] += 1
        if len(group["examples"]) < max_examples_per_group:
            group["examples"].append({
                "article_number": row.article_number,
                "product_family": row.product_family,
                "article_role": row.tece_article_role_candidate,
                "tece_article_role_candidate": row.tece_article_role_candidate,
                "extraction_method": row.extraction_method,
                "extraction_priority": row.extraction_priority,
                "source_file": row.source_file,
                "page_range_label": row.page_range_label,
                "candidate_terms": terms,
                "evidence_text_snippet": snippet,
            })

    sorted_groups = {
        key: {
            **value,
            "examples": value["examples"],
        }
        for key, value in sorted(groups.items(), key=lambda item: (-item[1]["count"], item[0]))
    }
    return {
        "source_pack_path": str(source_pack),
        "family": family,
        "unknown_role_count": len(unknown_rows),
        "candidate_term_counts": dict(sorted(term_counter.items())),
        "extraction_method_counts": dict(sorted(method_counter.items())),
        "groups": sorted_groups,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "report_note": "Diagnostic-only unknown role context report; inventory roles and canonical exports are not modified.",
    }


def _write_text(report: dict[str, Any], stream: Any) -> None:
    print(f"source_pack_path: {report['source_pack_path']}", file=stream)
    print(f"family: {report['family']}", file=stream)
    print(f"unknown_role_count: {report['unknown_role_count']}", file=stream)
    print("groups:", file=stream)
    for key, group in report["groups"].items():
        print(f"- {key}: {group['count']}", file=stream)
        for example in group["examples"]:
            print(
                "  "
                f"article={example['article_number']} role={example['tece_article_role_candidate']} "
                f"method={example['extraction_method']} priority={example['extraction_priority']} "
                f"source={example['source_file']} terms={','.join(example['candidate_terms']) or '(none)'}",
                file=stream,
            )
            print(f"    snippet={example['evidence_text_snippet']}", file=stream)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report diagnostic-only TECE unknown-role source contexts.")
    parser.add_argument("--source-pack", required=True, help="Path to a TECE source pack directory or file.")
    parser.add_argument("--family", default="TECEdrainline", help="TECE family to inspect; default: TECEdrainline.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--out", help="Optional output path.")
    parser.add_argument("--max-examples-per-group", type=int, default=5)
    args = parser.parse_args(argv)

    report = build_unknown_role_context_report(args.source_pack, family=args.family, max_examples_per_group=args.max_examples_per_group)
    if args.json:
        write_json_output(report, args.out)
    else:
        write_text_output(lambda stream: _write_text(report, stream), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
