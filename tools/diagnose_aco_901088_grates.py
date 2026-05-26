#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Set

from bs4 import BeautifulSoup

from src.connectors import aco

TARGET_ARTICLES = ["9010.88.61", "9010.88.62", "9010.88.63"]
TARGET_ARTICLE_DIGITS = {re.sub(r"\D", "", a) for a in TARGET_ARTICLES}
TARGET_PRODUCT_IDS = ["aco-90108861", "aco-90108862", "aco-90108863"]


@dataclass
class PageDiag:
    url: str
    reachable: bool
    status_code: int | None
    final_url: str
    family: str
    role: str
    cand_type: str
    contains_target_article_text: bool
    all_article_hits: List[str]
    table_detected: bool
    table_headers: List[str]
    parsed_pairs: List[Dict[str, Any]]
    parsed_diag_rows: List[Dict[str, Any]]
    emitted_rows_count_if_component_logic: int
    emitted_product_ids_if_component_logic: List[str]
    drop_reason: str


def _table_headers(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[str] = []
    for i, table in enumerate(soup.select("table"), start=1):
        heads = [aco._clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        if heads:
            out.append(f"table_{i}: " + " | ".join(heads[:8]))
    return out


def _component_emission_preview(url: str, title: str, family: str, role: str, pairs: List[Any]) -> tuple[int, List[str], str]:
    emitted: List[str] = []
    if not pairs:
        return 0, emitted, "no_article_pairs"

    if family not in {"showerdrain_mplus", "showerdrain_splus"}:
        return 0, emitted, "component_branch_supports_only_mplus_and_splus_article_rows"

    if family == "showerdrain_mplus":
        for _, _, article_digits in pairs:
            emitted.append(aco._stable_aco_id(url, family, aco._infer_mplus_role(url, title), title, article_digits))
        return len(emitted), emitted, "would_emit_mplus_components"

    if family == "showerdrain_splus":
        for _, article_no, article_digits in pairs:
            art_norm = article_no if "." in article_no else f"{article_digits[:4]}.{article_digits[4:6]}.{article_digits[6:8]}"
            if art_norm in aco.SPLUS_AMBIGUOUS_ARTICLES:
                continue
            role_s = "profile_channel" if art_norm in aco.SPLUS_PROFILE_ARTICLES else ("drain_body" if art_norm in aco.SPLUS_DRAIN_ARTICLES else "component")
            emitted.append(aco._stable_aco_id(url, family, role_s, title, article_digits))
        return len(emitted), emitted, "would_emit_splus_components"

    return 0, emitted, "unhandled_family"


def _discover_detail_pages() -> Set[str]:
    queue: List[str] = list(aco.SEED_PAGES)
    seen: Set[str] = set()
    detail: Set[str] = set()

    while queue and len(seen) < 250:
        page = queue.pop(0)
        if page in seen:
            continue
        seen.add(page)
        st, final, html, _ = aco._safe_get_text(page, timeout=35)
        if st != 200 or not html:
            continue

        final_c = aco._canonicalize_url(final)
        detail.add(final_c)
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = aco._abs(a.get("href") or "", final)
            if not aco._in_scope(cand):
                continue
            cand_c = aco._canonicalize_url(cand)
            if cand_c not in seen and cand_c not in queue:
                queue.append(cand_c)
            detail.add(cand_c)

    return detail


def _diagnose_page(url: str) -> PageDiag:
    st, final, html, _ = aco._safe_get_text(url, timeout=35)
    final_c = aco._canonicalize_url(final)
    if st != 200 or not html:
        return PageDiag(url, False, st, final_c, "unknown", "unknown", "unknown", False, [], False, [], [], [], 0, [], "unreachable")

    title = aco._extract_title(html, final_c)
    family = aco._detect_family(final_c, title)
    role, _role_reason = aco._classify_role(final_c, title, html, family)
    if family == "showerdrain_b" and aco._infer_b_role(final_c, title) == "complete_system":
        cand_type = "drain"
    elif aco._is_accessory_page(final_c, title):
        cand_type = "component"
    elif aco._looks_like_detail_drain_page(final_c, title, html):
        cand_type = "drain"
    elif family != "unknown":
        cand_type = "component"
    else:
        cand_type = "dropped_overview"

    flat = aco._main_flat_text_from_html(html)
    contains_target = any(a in flat for a in TARGET_ARTICLES)
    all_articles = sorted(set(m.group(0) for m in aco.ARTICLE_RE.finditer(flat)))
    pairs = aco._extract_pairs_from_table(html)
    diag_rows = aco._extract_article_row_diagnostics_from_table(html)
    emitted_count, emitted_ids, reason = _component_emission_preview(final_c, title, family, role, pairs)

    return PageDiag(
        url=url,
        reachable=True,
        status_code=st,
        final_url=final_c,
        family=family,
        role=role,
        cand_type=cand_type,
        contains_target_article_text=contains_target,
        all_article_hits=all_articles,
        table_detected=bool(pairs or diag_rows),
        table_headers=_table_headers(html),
        parsed_pairs=[{"l1_mm": l1, "article_no": a, "article_digits": d} for l1, a, d in pairs],
        parsed_diag_rows=diag_rows,
        emitted_rows_count_if_component_logic=emitted_count,
        emitted_product_ids_if_component_logic=emitted_ids,
        drop_reason=reason,
    )


def main() -> None:
    print("ACO 901088 grate diagnostics")
    print("=" * 80)

    detail_pages = _discover_detail_pages()
    c_pages = sorted([u for u in detail_pages if "aco-showerdrain-c" in u.lower()])
    print(f"Discovered in-scope detail pages: {len(detail_pages)}")
    print(f"Discovered ShowerDrain C related pages: {len(c_pages)}")

    targets = [u for u in c_pages if any(x in u.lower() for x in ["rost", "grate", "design", "abdeckung", "c/"])]
    if not targets:
        targets = c_pages

    hits = []
    for u in targets:
        d = _diagnose_page(u)
        if d.contains_target_article_text or any(r.get("article_digits") in TARGET_ARTICLE_DIGITS for r in d.parsed_diag_rows):
            hits.append(d)

    print(f"Candidate target pages containing 901088 indicators: {len(hits)}")
    for d in hits:
        print("-" * 80)
        print(f"target URL: {d.url}")
        print(f"page reachable: {'yes' if d.reachable else 'no'} (status={d.status_code}, final={d.final_url})")
        print(f"classified family/role/type: {d.family} / {d.role} / {d.cand_type}")
        print(f"article text present (9010.88.61/62/63): {'yes' if d.contains_target_article_text else 'no'}")
        parsed = sorted(set(r.get('article_no') for r in d.parsed_diag_rows if r.get('article_no')))
        print(f"parsed articles list: {parsed}")
        print(f"article table detected: {'yes' if d.table_detected else 'no'}")
        print(f"emitted rows count (component logic preview): {d.emitted_rows_count_if_component_logic}")
        print(f"emitted product ids (component logic preview): {d.emitted_product_ids_if_component_logic}")
        print(f"drop reason: {d.drop_reason}")

    print("-" * 80)
    print("Running full discover_candidates(1200,100) snapshot...")
    rows, debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    pids = {str(r.get("product_id") or "") for r in rows}
    present_target_ids = sorted([pid for pid in TARGET_PRODUCT_IDS if pid in pids])
    present_target_articles = sorted(
        {str(r.get("article_no")) for r in rows if re.sub(r"\D", "", str(r.get("article_no") or "")) in TARGET_ARTICLE_DIGITS}
    )
    print(f"discover_candidates rows: {len(rows)}")
    print(f"target product IDs present: {present_target_ids}")
    print(f"target article numbers present in emitted rows: {present_target_articles}")

    summary = [x for x in debug if x.get("method") == "summary"]
    if summary:
        print("debug summary:")
        print(json.dumps(summary[-1], indent=2, ensure_ascii=False))

    print("-" * 80)
    print("Root-cause verdict:")
    print("1) 901088 article numbers can appear in ShowerDrain C pages/tables.")
    print("2) Current discover_candidates component branch only emits article rows for mplus/splus families.")
    print("3) For ShowerDrain C pages classified as component, article rows are ignored and only page-level component IDs are emitted.")
    print("4) Therefore aco-90108861/62/63 are never emitted without changing production logic.")


if __name__ == "__main__":
    main()
