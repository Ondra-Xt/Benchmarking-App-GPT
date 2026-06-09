from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOTS = (
    REPO_ROOT / "tests" / "fixtures",
    REPO_ROOT / "docs",
)
ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
EPLUS_RE = re.compile(r"(?:showerdrain[-\s]?e\+|showerdrain[-\s]?eplus|\beplus\b)", re.IGNORECASE)
BODY_RE = re.compile(r"(?:rinnenk(?:ö|oe)rper|ablaufk(?:ö|oe)rper|channel\s*body|drain\s*body|base)", re.IGNORECASE)
GRATE_RE = re.compile(r"(?:design[-\s]?rost|roste?|grate)", re.IGNORECASE)
EXPLICIT_LINK_RE = re.compile(r"(?:kompatibel|compatible|passend|geeignet|kombinierbar|->|→|×|\bx\b)", re.IGNORECASE)
TEXT_SUFFIXES = {".html", ".htm", ".txt", ".md", ".csv"}

COMPATIBILITY_EVIDENCE_TYPE = "no_explicit_article_level_matrix_found"
COMPATIBILITY_CONFIDENCE = "low"
BLOCKING_REASON = "Explicit article-level E+ base-to-grate compatibility is missing; no explicit article-level base-to-grate compatibility matrix was found."
RECOMMENDED_NEXT_ACTION = "collect explicit article-level E+ base-to-grate compatibility before production generation."
PRODUCTION_STATUS_NOTE = "diagnostic-only evidence; no Products/BOM_Options/Final_Assemblies/Final_Set_Details generation change."

EVIDENCE_COLUMNS = (
    "set_id", "product_family", "assembly_model", "body_id", "body_article_number",
    "body_source_url", "grate_id", "grate_article_number", "grate_source_url",
    "source_search_scope", "source_search_status", "compatibility_evidence_type",
    "compatibility_confidence", "article_level_compatibility_found", "safe_to_generate",
    "ready_for_benchmark", "ready_for_customer_view", "blocking_reason",
    "recommended_next_action", "production_status_note",
)


@dataclass(frozen=True)
class SourceSearchResult:
    source_path: str
    eplus_mentioned: bool
    body_articles: tuple[str, ...]
    grate_articles: tuple[str, ...]
    explicit_article_level_link_found: bool
    evidence_classification: str
    confidence: str
    reason: str


def inspect_source_text(text: str, source_path: str = "") -> SourceSearchResult:
    """Classify strict E+ evidence without treating family-level co-occurrence as compatibility."""
    normalized = re.sub(r"\s+", " ", text or " ").strip()
    eplus = bool(EPLUS_RE.search(normalized))
    body_articles: set[str] = set()
    grate_articles: set[str] = set()
    explicit = False
    for raw_line in re.split(r"[\r\n]+", text or ""):
        line = re.sub(r"\s+", " ", raw_line).strip()
        articles = set(ARTICLE_RE.findall(line))
        if not articles or not EPLUS_RE.search(line):
            continue
        if BODY_RE.search(line):
            body_articles.update(articles)
        if GRATE_RE.search(line):
            grate_articles.update(articles)
        if len(articles) >= 2 and BODY_RE.search(line) and GRATE_RE.search(line) and EXPLICIT_LINK_RE.search(line):
            explicit = True
    if explicit and body_articles and grate_articles:
        classification, confidence = "explicit_article_level_body_to_grate_link", "high"
        reason = "An E+ source row explicitly links body and grate article numbers."
    elif eplus and (body_articles or grate_articles):
        classification, confidence = "article_numbers_without_explicit_compatibility", "low"
        reason = "E+ article numbers occur without an explicit body-to-grate relationship."
    elif eplus:
        classification, confidence = "family_level_only", "low"
        reason = "The source mentions E+ but provides no explicit article-level body-to-grate relationship."
    else:
        classification, confidence = "no_eplus_evidence", "none"
        reason = "The source contains no E+ compatibility evidence."
    return SourceSearchResult(
        source_path=source_path,
        eplus_mentioned=eplus,
        body_articles=tuple(sorted(body_articles)),
        grate_articles=tuple(sorted(grate_articles)),
        explicit_article_level_link_found=explicit,
        evidence_classification=classification,
        confidence=confidence,
        reason=reason,
    )


def search_source_roots(roots: Iterable[Path] = DEFAULT_SOURCE_ROOTS) -> tuple[SourceSearchResult, ...]:
    results: list[SourceSearchResult] = []
    for root in roots:
        if not root.exists():
            continue
        paths = (root,) if root.is_file() else root.rglob("*")
        for path in sorted(paths):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            result = inspect_source_text(text, str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path))
            if result.eplus_mentioned:
                results.append(result)
    return tuple(results)


def build_export_evidence_dataframe(proposals: pd.DataFrame) -> pd.DataFrame:
    """Mirror E+ proposals into a diagnostic-only evidence sheet; never generate production rows."""
    proposals = pd.DataFrame() if proposals is None else proposals.copy()
    rows = []
    for _, proposal in proposals.iterrows():
        rows.append({
            "set_id": proposal.get("set_id", ""),
            "product_family": proposal.get("product_family", "showerdrain_eplus"),
            "assembly_model": proposal.get("assembly_model", "base_x_grate"),
            "body_id": proposal.get("body_id", ""),
            "body_article_number": proposal.get("body_article_number", ""),
            "body_source_url": proposal.get("body_source_url", ""),
            "grate_id": proposal.get("grate_id", ""),
            "grate_article_number": proposal.get("grate_article_number", ""),
            "grate_source_url": proposal.get("grate_source_url", ""),
            "source_search_scope": "checked-in fixtures/docs plus current E+ source-page diagnostics",
            "source_search_status": COMPATIBILITY_EVIDENCE_TYPE,
            "compatibility_evidence_type": COMPATIBILITY_EVIDENCE_TYPE,
            "compatibility_confidence": COMPATIBILITY_CONFIDENCE,
            "article_level_compatibility_found": False,
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": BLOCKING_REASON,
            "recommended_next_action": RECOMMENDED_NEXT_ACTION,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        })
    return pd.DataFrame(rows, columns=EVIDENCE_COLUMNS)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Search checked-in sources for explicit ACO ShowerDrain E+ article compatibility.")
    parser.add_argument("paths", nargs="*", type=Path)
    args = parser.parse_args(argv)
    results = search_source_roots(args.paths or DEFAULT_SOURCE_ROOTS)
    explicit = [row for row in results if row.explicit_article_level_link_found]
    print("ACO ShowerDrain E+ source evidence search")
    print(f"E+ sources inspected: {len(results)}")
    print(f"Explicit article-level compatibility found: {'YES' if explicit else 'NO'}")
    for row in results:
        print(f"- {row.source_path}: {row.evidence_classification}/{row.confidence} - {row.reason}")
    print("Production E+ generation enabled: NO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
