from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.connectors import tece

TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "water_seal_mm",
)
PRODUCTION_STATUS_NOTE = (
    "diagnostic-only TECE source inventory; no Products/Comparison/BOM/assembly generation change; "
    "TECE production promotion blocked pending article-level compatibility evidence and complete technical evidence"
)


@dataclass(frozen=True)
class TeceInventoryRow:
    product_id: str
    article_number: str
    product_family: str
    product_name: str
    product_url: str
    source_urls: str
    length_mm: Any
    length_delta_mm: Any
    flow_rate_lps: Any
    outlet_dn: Any
    height_adj_min_mm: Any
    height_adj_max_mm: Any
    water_seal_mm: Any
    evidence_sources: str
    missing_fields: str
    possible_assembly_model: str
    article_level_compatibility_evidence: bool
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    production_status_note: str


@dataclass(frozen=True)
class TeceInventoryReport:
    candidate_count: int
    article_numbers: list[str]
    source_urls: list[str]
    product_families: list[str]
    product_names: list[str]
    length_coverage: dict[str, int]
    technical_field_coverage: dict[str, int]
    evidence_source_counts: dict[str, int]
    missing_field_counts: dict[str, int]
    possible_assembly_model: str
    article_level_compatibility_evidence_exists: bool
    production_promotion_blocked: bool
    production_status_note: str
    seed_urls: list[str]
    discovery_debug: list[dict[str, Any]]
    rows: list[TeceInventoryRow]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _article_from_candidate(candidate: dict[str, Any]) -> str:
    value = _clean(candidate.get("product_id"))
    if value.startswith("tece-"):
        return value.split("tece-", 1)[1]
    return tece._extract_article_from_text(" ".join(_clean(candidate.get(k)) for k in ("product_url", "sources", "product_name"))) or ""


def _evidence_kinds(params: dict[str, Any], article: str) -> set[str]:
    kinds: set[str] = set()
    for label, _text, url in params.get("evidence") or []:
        joined = f"{label} {url}".lower()
        if "html" in joined:
            kinds.add("HTML")
        if ".pdf" in joined and "tcdb_" in joined:
            kinds.add("guessed tcdb PDF")
        elif ".pdf" in joined or "pdf" in joined:
            kinds.add("PDF datasheet")
    if article and not any("guessed tcdb PDF" == k for k in kinds):
        # The connector has a deterministic tcdb fallback URL; report it as guessed when used/available for audit.
        if any(tece._guess_tcdb_pdf(article) in _clean(item[2]) or tece._guess_tcdb_pdf(article) in _clean(item[1]) for item in (params.get("evidence") or [])):
            kinds.add("guessed tcdb PDF")
    return kinds or {"HTML"}


def _source_urls(candidate: dict[str, Any], params: dict[str, Any], article: str) -> list[str]:
    urls = []
    for key in ("product_url", "sources"):
        value = _clean(candidate.get(key))
        if value and value not in urls:
            urls.append(value)
    for _label, _text, url in params.get("evidence") or []:
        value = _clean(url)
        if value.startswith("http") and value not in urls:
            urls.append(value)
    if article:
        guessed = tece._guess_tcdb_pdf(article)
        if guessed in " ".join(urls) and guessed not in urls:
            urls.append(guessed)
    return urls


def build_report(target_length_mm: int = 1200, tolerance_mm: int = 100, *, max_candidates: int | None = None) -> TeceInventoryReport:
    candidates, debug = tece.discover_candidates(target_length_mm=target_length_mm, tolerance_mm=tolerance_mm)
    if max_candidates is not None:
        candidates = candidates[:max(0, int(max_candidates))]

    rows: list[TeceInventoryRow] = []
    for candidate in candidates:
        product_url = _clean(candidate.get("product_url"))
        params = tece.extract_parameters(product_url) if product_url else {"evidence": []}
        article = _article_from_candidate(candidate)
        missing = [field for field in TECHNICAL_FIELDS if _clean(params.get(field)) == ""]
        evidence_sources = sorted(_evidence_kinds(params, article))
        urls = _source_urls(candidate, params, article)
        has_article_evidence = bool(article) and any(article in url or article in _clean(candidate.get("product_name")) for url in urls)
        rows.append(TeceInventoryRow(
            product_id=_clean(candidate.get("product_id")),
            article_number=article,
            product_family=_clean(candidate.get("product_family")),
            product_name=_clean(candidate.get("product_name")),
            product_url=product_url,
            source_urls=" | ".join(urls),
            length_mm=target_length_mm + int(candidate.get("length_delta_mm") or 0) if _clean(candidate.get("length_delta_mm")) else "",
            length_delta_mm=candidate.get("length_delta_mm", ""),
            flow_rate_lps=params.get("flow_rate_lps"),
            outlet_dn=params.get("outlet_dn"),
            height_adj_min_mm=params.get("height_adj_min_mm"),
            height_adj_max_mm=params.get("height_adj_max_mm"),
            water_seal_mm=params.get("water_seal_mm"),
            evidence_sources=", ".join(evidence_sources),
            missing_fields=", ".join(missing),
            possible_assembly_model="drain body/channel candidate plus cover/grate/accessory assembly not proven at article level",
            article_level_compatibility_evidence=has_article_evidence and not missing,
            production_promotion_blocked=True,
            ready_for_benchmark=False,
            ready_for_customer_view=False,
            production_status_note=PRODUCTION_STATUS_NOTE,
        ))

    coverage = {field: sum(1 for row in rows if _clean(getattr(row, field)) != "") for field in TECHNICAL_FIELDS}
    missing_counts = {field: sum(1 for row in rows if field in row.missing_fields.split(", ")) for field in TECHNICAL_FIELDS}
    evidence_counts = {name: sum(1 for row in rows if name in row.evidence_sources) for name in ("HTML", "PDF datasheet", "guessed tcdb PDF")}
    return TeceInventoryReport(
        candidate_count=len(rows),
        article_numbers=sorted({row.article_number for row in rows if row.article_number}),
        source_urls=sorted({url for row in rows for url in row.source_urls.split(" | ") if url}),
        product_families=sorted({row.product_family for row in rows if row.product_family}),
        product_names=sorted({row.product_name for row in rows if row.product_name}),
        length_coverage={"with_length": sum(1 for row in rows if _clean(row.length_mm)), "missing_length": sum(1 for row in rows if not _clean(row.length_mm))},
        technical_field_coverage=coverage,
        evidence_source_counts=evidence_counts,
        missing_field_counts=missing_counts,
        possible_assembly_model="TECEdrain line/profile appears assembly-like; article-level body-to-cover compatibility evidence not established by this report",
        article_level_compatibility_evidence_exists=any(row.article_level_compatibility_evidence for row in rows),
        production_promotion_blocked=True,
        production_status_note=PRODUCTION_STATUS_NOTE,
        seed_urls=list(tece.PRODUKTDATEN_SEEDS),
        discovery_debug=debug,
        rows=rows,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only TECE source inventory and evidence audit.")
    parser.add_argument("--target-length-mm", type=int, default=1200)
    parser.add_argument("--tolerance-mm", type=int, default=100)
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = build_report(args.target_length_mm, args.tolerance_mm, max_candidates=args.max_candidates)
    payload = asdict(report)
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print("TECE source inventory (diagnostic-only)")
        print(f"candidate_count: {report.candidate_count}")
        print(f"article_numbers: {', '.join(report.article_numbers) or '(none)'}")
        print(f"technical_field_coverage: {json.dumps(report.technical_field_coverage, sort_keys=True)}")
        print(f"evidence_source_counts: {json.dumps(report.evidence_source_counts, sort_keys=True)}")
        print(f"production_promotion_blocked: {report.production_promotion_blocked}")
        print(report.production_status_note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
