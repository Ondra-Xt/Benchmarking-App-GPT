"""Report diagnostic-only ACO ShowerDrain B article evidence.

ShowerDrain B is sold as an integral all-in-one set.  This module deliberately
records those finished-set article variants without inventing a base-to-grate
matrix and without feeding any rows back into production assembly generation.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd
from bs4 import BeautifulSoup
from pypdf import PdfReader

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOTS = (REPO_ROOT / "tests" / "fixtures" / "aco_b", REPO_ROOT / "docs")
PRODUCT_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "aco_b" / "b_international_product.html"
SOURCE_URL = "https://www.buildingdrainage.aco/products/collect/bathroom-drainage/channel/aco-showerdrain-b/aco-showerdrain-b"

ARTICLE_RE = re.compile(r"\b(?:9010\.78\.(?:70|71|72|73)|301817[2-5])\b")
BLINE_RE = re.compile(r"(?:showerdrain[-\s]?b|\bb[-\s]?line\b)", re.IGNORECASE)
ALL_IN_ONE_RE = re.compile(
    r"(?:all-in-one.{0,100}(?:channel|rinne).{0,50}(?:grat(?:e|ing)|rost).{0,50}(?:gully|ablauf)|"
    r"(?:channel|rinne).{0,50}(?:grat(?:e|ing)|rost).{0,50}(?:gully|ablauf).{0,100}(?:ready-to-install|montagefertig))",
    re.IGNORECASE | re.DOTALL,
)

EVIDENCE_COLUMNS = (
    "evidence_id", "product_family", "assembly_model", "product_article_number",
    "body_article_number", "grate_article_number", "variant_condition", "length_l1_mm",
    "length_l2_mm", "width_mm", "flow_rate_lps", "flow_rate_10mm_lps",
    "flow_rate_20mm_lps", "water_seal_mm", "outlet_dn", "height_adj_min_mm",
    "height_adj_max_mm", "installation_height_mm", "source_url", "source_path",
    "compatibility_evidence_type", "compatibility_confidence",
    "article_level_compatibility_found", "source_text_or_reason", "data_quality_status",
    "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view",
    "blocking_reason", "recommended_next_action", "production_status_note",
)

_VARIANTS = (
    ("9010.78.70", "without_sealing_sleeve", 685, 745, 130),
    ("9010.78.71", "without_sealing_sleeve", 785, 845, 130),
    ("9010.78.72", "without_sealing_sleeve", 885, 945, 130),
    ("9010.78.73", "without_sealing_sleeve", 985, 1045, 130),
    ("3018172", "with_attached_sealing_sleeve", 685, 745, 130),
    ("3018173", "with_attached_sealing_sleeve", 785, 845, 130),
    ("3018174", "with_attached_sealing_sleeve", 885, 945, 130),
    ("3018175", "with_attached_sealing_sleeve", 985, 1045, 130),
)


def _source_text(path: Path = PRODUCT_FIXTURE) -> str:
    if not path.exists():
        return ""
    soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    for tag in soup(("script", "style", "noscript")):
        tag.decompose()
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def _searchable_source_text(path: Path) -> tuple[str, str]:
    if path.suffix.lower() != ".pdf":
        return path.read_text(encoding="utf-8", errors="ignore"), "ok"
    try:
        reader = PdfReader(path)
        return "\n".join(page.extract_text() or "" for page in reader.pages), "ok"
    except Exception as exc:
        return "", f"unreadable_pdf:{type(exc).__name__}"


def inspect_source_roots(roots: Iterable[Path] = DEFAULT_SOURCE_ROOTS) -> pd.DataFrame:
    """Inventory B source files and explicit articles, including readable PDFs."""
    rows: list[dict[str, object]] = []
    for root in roots:
        if not root.exists():
            continue
        paths = (root,) if root.is_file() else root.rglob("*")
        for path in sorted(paths):
            if not path.is_file() or path.suffix.lower() not in {".html", ".htm", ".txt", ".md", ".csv", ".pdf"}:
                continue
            text, read_status = _searchable_source_text(path)
            if not BLINE_RE.search(text) and "aco_b" not in path.as_posix().lower():
                continue
            normalized = re.sub(r"\s+", " ", text).strip()
            rows.append({
                "source_path": str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path),
                "source_read_status": read_status,
                "bline_mentioned": bool(BLINE_RE.search(text)),
                "article_numbers": ",".join(sorted(set(ARTICLE_RE.findall(text)))),
                "explicit_integral_set_statement_found": bool(ALL_IN_ONE_RE.search(normalized)),
                "explicit_separate_body_to_grate_matrix_found": False,
            })
    return pd.DataFrame(rows)


def build_export_evidence_dataframe() -> pd.DataFrame:
    """Build eight explicit finished-set variants as non-production diagnostics."""
    text = _source_text()
    articles = set(ARTICLE_RE.findall(text))
    explicit_set = bool(ALL_IN_ONE_RE.search(text))
    rows: list[dict[str, object]] = []
    for article, condition, l1, l2, width in _VARIANTS:
        found = explicit_set and article in articles
        rows.append({
            "evidence_id": f"bline-complete-set-{article.replace('.', '-')}",
            "product_family": "showerdrain_b",
            "assembly_model": "integral_all_in_one_set",
            "product_article_number": article,
            "body_article_number": "",
            "grate_article_number": "",
            "variant_condition": condition,
            "length_l1_mm": l1,
            "length_l2_mm": l2,
            "width_mm": width,
            # Preserve the two source conditions; do not choose a default flow.
            "flow_rate_lps": "",
            "flow_rate_10mm_lps": 0.4,
            "flow_rate_20mm_lps": 0.46,
            "water_seal_mm": 30,
            "outlet_dn": "DN50",
            # The source gives a fixed installation height, not an adjustment range.
            "height_adj_min_mm": "",
            "height_adj_max_mm": "",
            "installation_height_mm": 80,
            "source_url": SOURCE_URL,
            "source_path": str(PRODUCT_FIXTURE.relative_to(REPO_ROOT)),
            "compatibility_evidence_type": "explicit_article_level_integral_complete_set" if found else "article_not_confirmed",
            "compatibility_confidence": "high" if found else "none",
            "article_level_compatibility_found": found,
            "source_text_or_reason": (
                "Source identifies ShowerDrain B as an all-in-one, ready-to-install channel, grating and gully set; "
                "the order table assigns this article to one length and sealing-sleeve condition. No separate grate article is stated."
                if found else "The expected B article was not confirmed in the checked-in product source."
            ),
            "data_quality_status": "explicit_finished_set_variant;no_separate_grate_article" if found else "blocked_missing_article_evidence",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": (
                "Diagnostic-first branch: do not promote B. Evidence describes an integral finished set, not a separately identified body-to-grate combination."
            ),
            "recommended_next_action": (
                "In a later dedicated branch, decide whether explicit all-in-one B articles should be promoted directly as products; do not generate base_x_grate assemblies."
            ),
            "production_status_note": "diagnostic/evidence-only; no Products, Comparison, BOM_Options, Final_Assemblies, Final_Set_Details, scoring, scenario, or customer-view change.",
        })
    return pd.DataFrame(rows, columns=EVIDENCE_COLUMNS)


def _bline_count(frame: pd.DataFrame) -> int:
    if frame is None or frame.empty:
        return 0
    mask = pd.Series(False, index=frame.index)
    for column in ("product_id", "product_name", "product_family", "parent_family", "option_family", "assembled_family"):
        if column in frame.columns:
            values = frame[column].fillna("").astype(str)
            mask |= values.str.contains(r"showerdrain[_ -]?b|showerdrain b|bline|b-line", case=False, regex=True)
    return int(mask.sum())


def workbook_inventory(path: str | Path) -> pd.DataFrame:
    """Count B-related rows in canonical sheets without modifying the workbook."""
    names = ("Products", "Components", "Candidates_All", "BOM_Options", "Final_Assemblies", "Final_Set_Details", "Article_Variants")
    with pd.ExcelFile(path, engine="openpyxl") as xls:
        return pd.DataFrame([
            {"sheet_name": name, "total_rows": len(df := pd.read_excel(xls, sheet_name=name)), "bline_rows": _bline_count(df)}
            for name in names if name in xls.sheet_names
        ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", help="Optional canonical workbook to inventory")
    parser.add_argument("--csv", help="Optional path for the evidence CSV")
    args = parser.parse_args(argv)
    evidence = build_export_evidence_dataframe()
    print(evidence.to_string(index=False))
    if args.workbook:
        print("\nWorkbook B-line inventory")
        print(workbook_inventory(args.workbook).to_string(index=False))
    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        evidence.to_csv(args.csv, index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
