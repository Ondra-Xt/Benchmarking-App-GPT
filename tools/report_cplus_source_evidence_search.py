from __future__ import annotations

import argparse
import re
from functools import lru_cache
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup
from pypdf import PdfReader

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "aco_cplus"
CATALOG_FIXTURE = DEFAULT_FIXTURE_DIR / "cplus_showerdrain_catalog_2025_cz.pdf"
CATALOG_SOURCE_URL = (
    "https://www.aco.cz/fileadmin/standard/aco.cz/04_Ke_stazeni/Ceniky/"
    "Dokumente/2025/ACO_ShowerDrain_katalog_2025_CZ.pdf"
)
CPLUS_FAMILY_URL = (
    "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/"
    "aco-showerdrain-cplus/"
)
PROTECTED_CPLUS_BASE_IDS = (
    "aco-showerdrain-cplus-standard-h92",
    "aco-showerdrain-cplus-low-h69",
)
ALLOWED_EVIDENCE_CLASSIFICATIONS = {
    "article_level_explicit",
    "article_level_table",
    "explicit_catalog_matrix",
    "explicit_manual_matrix",
    "explicit_technical_drawing_matrix",
    "page_level_cplus_family",
    "page_level_shared_c_cplus",
    "inferred_from_shared_c_grate_page",
    "insufficient",
    "missing",
}
ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{7})\b")
BASE_ARTICLE_RE = re.compile(r"\b9010\.85\.(?:2[0-4]|3[0-4]|4[0-4]|5[0-4])\b")
GRATE_ARTICLE_RE = re.compile(r"\b9010\.88\.(?:6[0-6]|6[7-9]|7[0-3]|8[1-9]|9[0-5])\b")
C_BODY_ARTICLE_RE = re.compile(r"\b9010\.85\.\d{2}\b")

BASE_ARTICLES_BY_ID_AND_LENGTH = {
    "aco-showerdrain-cplus-standard-h92": {
        685: ("9010.85.20", "9010.85.30"),
        785: ("9010.85.21", "9010.85.31"),
        885: ("9010.85.22", "9010.85.32"),
        985: ("9010.85.23", "9010.85.33"),
        1185: ("9010.85.24", "9010.85.34"),
    },
    "aco-showerdrain-cplus-low-h69": {
        685: ("9010.85.40", "9010.85.50"),
        785: ("9010.85.41", "9010.85.51"),
        885: ("9010.85.42", "9010.85.52"),
        985: ("9010.85.43", "9010.85.53"),
        1185: ("9010.85.44", "9010.85.54"),
    },
}
GRATE_LENGTH_BY_ARTICLE = {
    **{f"9010.88.{suffix:02d}": length for suffix, length in zip(range(60, 67), (585, 685, 785, 885, 985, 1085, 1185))},
    **{f"9010.88.{suffix:02d}": length for suffix, length in zip(range(67, 74), (585, 685, 785, 885, 985, 1085, 1185))},
    **{f"9010.88.{suffix:02d}": length for suffix, length in zip(range(81, 88), (585, 685, 785, 885, 985, 1085, 1185))},
    **{f"9010.88.{suffix:02d}": length for suffix, length in zip(range(88, 95), (585, 685, 785, 885, 985, 1085, 1185))},
    "9010.88.95": 1085,
}


@dataclass(frozen=True)
class ExplicitCompatibilityMapping:
    base_id: str
    base_article_numbers: tuple[str, ...]
    grate_article_number: str
    nominal_length_mm: int
    evidence_classification: str
    evidence_confidence: str
    source_path_or_url: str
    source_pages: str
    reason: str


@dataclass(frozen=True)
class SourceAuditRow:
    source_id: str
    source_type: str
    source_path_or_url: str
    fetch_or_parse_status: str
    relevant_text_snippet_or_table_summary: str
    detected_base_ids: str
    detected_base_article_numbers: str
    detected_grate_article_numbers: str
    evidence_classification: str
    evidence_confidence: str
    reason: str
    recommended_action: str


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


@lru_cache(maxsize=16)
def extract_pdf_pages(path: Path) -> tuple[str, ...]:
    return tuple(_clean(page.extract_text() or "") for page in PdfReader(path).pages)


def _html_text(path: Path) -> str:
    return _clean(BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "lxml").get_text(" "))



def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def explicit_catalog_mappings(path: Path = CATALOG_FIXTURE) -> tuple[ExplicitCompatibilityMapping, ...]:
    """Return only length-matched C+ body-to-grate mappings proven by the 2025 catalog."""
    if not path.exists():
        return ()
    pages = extract_pdf_pages(path)
    body_page = pages[24] if len(pages) > 24 else ""
    grate_page = pages[25] if len(pages) > 25 else ""
    body_is_explicit = (
        "Sprchové žlaby ACO ShowerDrain C+" in body_page
        and "Objednávková data, žlab" in body_page
        and bool(BASE_ARTICLE_RE.search(body_page))
    )
    grate_is_explicit = (
        "Sprchové žlaby ACO ShowerDrain C & C+" in grate_page
        and "Objednávková data, nerezové krycí rošty" in grate_page
        and bool(GRATE_ARTICLE_RE.search(grate_page))
    )
    if not (body_is_explicit and grate_is_explicit):
        return ()

    present_bases = set(BASE_ARTICLE_RE.findall(body_page))
    present_grates = set(GRATE_ARTICLE_RE.findall(grate_page))
    mappings: list[ExplicitCompatibilityMapping] = []
    for grate_article in sorted(present_grates):
        length = GRATE_LENGTH_BY_ARTICLE.get(grate_article)
        if length is None:
            continue
        for base_id, by_length in BASE_ARTICLES_BY_ID_AND_LENGTH.items():
            base_articles = tuple(article for article in by_length.get(length, ()) if article in present_bases)
            if not base_articles:
                continue
            mappings.append(ExplicitCompatibilityMapping(
                base_id=base_id,
                base_article_numbers=base_articles,
                grate_article_number=grate_article,
                nominal_length_mm=length,
                evidence_classification="explicit_catalog_matrix",
                evidence_confidence="high",
                source_path_or_url=f"{_display_path(path)} | {CATALOG_SOURCE_URL}",
                source_pages="25-26",
                reason=(
                    "Catalog page 25 lists C+ body articles by length and hydraulic variant; "
                    "page 26 is explicitly headed ShowerDrain C & C+ and lists grate articles by length. "
                    "Compatibility is limited to equal nominal lengths."
                ),
            ))
    return tuple(mappings)


def _audit_pdf(path: Path) -> SourceAuditRow:
    try:
        pages = extract_pdf_pages(path)
    except Exception as exc:
        return SourceAuditRow(path.stem, "pdf", str(path.relative_to(REPO_ROOT)), f"parse_error_{type(exc).__name__}", "", "", "", "", "missing", "none", "PDF text extraction failed.", "Repair or replace the fixture.")
    text = " ".join(pages)
    bases = sorted(set(BASE_ARTICLE_RE.findall(text)))
    grates = sorted(set(GRATE_ARTICLE_RE.findall(text)))
    mappings = explicit_catalog_mappings(path) if path.name == CATALOG_FIXTURE.name else ()
    if mappings:
        classification, confidence = "explicit_catalog_matrix", "high"
        summary = (
            "Pages 25-26: C+ body table lists standard H=92 articles 9010.85.20-.24/.30-.34 and "
            "low H=69 articles 9010.85.40-.44/.50-.54; the next table is headed "
            "'ShowerDrain C & C+' and lists stainless grate articles by nominal length."
        )
        reason = "The catalog explicitly scopes the grate article table to C and C+; equal-length body and grate rows produce article-level mappings."
        action = "Upgrade only length-matched catalog-proven rows; keep customer output disabled and do not generate assemblies in this pass."
    elif "ShowerDrain C+" in text and grates:
        classification, confidence = "insufficient", "low"
        summary = "C+ and grate articles occur in the document, but no parsed table explicitly scopes those grate rows to C+."
        reason = "Co-occurrence is not an article-level compatibility matrix under the strict evidence rule."
        action = "Keep inferred rows blocked; prefer the explicit 2025 C & C+ catalog table."
    elif "ShowerDrain C+" in text:
        classification, confidence = "page_level_cplus_family", "medium"
        summary = "Document mentions the C+ family but exposes no article-level C+ grate matrix."
        reason = "Family-level product information alone does not prove base-to-grate article compatibility."
        action = "Use only for family context."
    else:
        classification, confidence = "insufficient", "none"
        summary = "No explicit C+ base-to-grate table detected."
        reason = "The source does not connect protected C+ body variants to grate articles."
        action = "No evidence upgrade."
    return SourceAuditRow(
        path.stem, "catalog" if "catalog" in path.name else "pdf", str(path.relative_to(REPO_ROOT)), "ok",
        summary, ", ".join(PROTECTED_CPLUS_BASE_IDS if bases else ()), ", ".join(bases), ", ".join(grates),
        classification, confidence, reason, action,
    )


def _audit_html(path: Path) -> SourceAuditRow:
    try:
        text = _html_text(path)
    except Exception as exc:
        return SourceAuditRow(path.stem, "html", str(path.relative_to(REPO_ROOT)), f"parse_error_{type(exc).__name__}", "", "", "", "", "missing", "none", "HTML parsing failed.", "Repair or replace the fixture.")
    articles = sorted(set(ARTICLE_RE.findall(text)))
    bases = sorted(set(BASE_ARTICLE_RE.findall(text)))
    grates = sorted(set(GRATE_ARTICLE_RE.findall(text)))
    lower_name = path.name.lower()
    if "c_design_grates" in lower_name:
        classification, confidence = "inferred_from_shared_c_grate_page", "low"
        reason = "The design-grate page identifies ShowerDrain C, not C+; C-family grate pages alone are insufficient."
    elif "cplus" in lower_name:
        classification, confidence = "page_level_cplus_family", "medium"
        reason = "The C+ family page describes grate choice but does not link protected C+ body articles to specific grate article numbers."
    elif bases or C_BODY_ARTICLE_RE.search(text):
        classification, confidence = "insufficient", "low"
        reason = "A C-family body page is not a C+ grate compatibility source, and 9010.85.xx body rows are never treated as grates."
    else:
        classification, confidence = "insufficient", "none"
        reason = "No explicit C+ article-level base-to-grate relationship was detected."
    snippet = _clean(text[:500])
    return SourceAuditRow(
        path.stem, "html", str(path.relative_to(REPO_ROOT)), "ok", snippet,
        ", ".join(PROTECTED_CPLUS_BASE_IDS if "cplus" in lower_name else ()), ", ".join(bases), ", ".join(grates),
        classification, confidence, reason,
        "Use as context only; do not upgrade compatibility rows." if classification != "page_level_cplus_family" else "Retain as family-level corroboration only.",
    )


def build_source_audit(fixture_dir: Path = DEFAULT_FIXTURE_DIR) -> tuple[SourceAuditRow, ...]:
    if not fixture_dir.exists():
        return (SourceAuditRow("aco_cplus_fixture_directory", "fixture", str(fixture_dir), "missing", "", "", "", "", "missing", "none", "Fixture directory does not exist.", "Restore C+ source fixtures."),)
    rows = []
    for path in sorted(fixture_dir.iterdir()):
        if path.suffix.lower() == ".pdf":
            rows.append(_audit_pdf(path))
        elif path.suffix.lower() in {".html", ".htm"}:
            rows.append(_audit_html(path))
    return tuple(rows)


def source_audit_records(rows: Iterable[SourceAuditRow]) -> list[dict[str, str]]:
    return [asdict(row) for row in rows]


def print_audit(rows: tuple[SourceAuditRow, ...], mappings: tuple[ExplicitCompatibilityMapping, ...]) -> None:
    print("ACO ShowerDrain C+ source evidence audit")
    print(f"Explicit article-level compatibility found: {'YES' if mappings else 'NO'}")
    print(f"Explicit length-matched mappings found: {len(mappings)}")
    print("Customer-facing output enabled: NO")
    print("C+ production assemblies generated: NO")
    print("\nSources inspected:")
    for row in rows:
        print(f"- {row.source_id} [{row.fetch_or_parse_status}] {row.evidence_classification}/{row.evidence_confidence}")
        print(f"  source={row.source_path_or_url}")
        print(f"  summary={row.relevant_text_snippet_or_table_summary}")
        print(f"  reason={row.reason}")
    if mappings:
        print("\nExplicit compatibility source:")
        print(f"- {mappings[0].source_path_or_url}, pages {mappings[0].source_pages}")
        for base_id in PROTECTED_CPLUS_BASE_IDS:
            rows_for_base = [m for m in mappings if m.base_id == base_id]
            print(f"\n{base_id}:")
            by_length: dict[int, list[ExplicitCompatibilityMapping]] = {}
            for mapping in rows_for_base:
                by_length.setdefault(mapping.nominal_length_mm, []).append(mapping)
            for length, length_rows in sorted(by_length.items()):
                bases = ",".join(length_rows[0].base_article_numbers)
                grates = ",".join(row.grate_article_number for row in length_rows)
                print(f"- {length} mm base articles {bases} -> grate articles {grates}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit stored ACO C+ sources for explicit body-to-grate evidence.")
    parser.add_argument("--fixture-dir", type=Path, default=DEFAULT_FIXTURE_DIR)
    args = parser.parse_args(argv)
    rows = build_source_audit(args.fixture_dir)
    catalog_path = args.fixture_dir / CATALOG_FIXTURE.name
    mappings = explicit_catalog_mappings(catalog_path)
    print_audit(rows, mappings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
