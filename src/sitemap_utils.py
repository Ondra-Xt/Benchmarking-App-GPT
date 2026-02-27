from __future__ import annotations

from typing import Iterable, List, Optional
import xml.etree.ElementTree as ET
import requests


def _get(url: str, headers: Optional[dict] = None, timeout: int = 30) -> tuple[int | None, str, str]:
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or "")
    except Exception:
        return None, url, ""


def _parse_sitemap_locs(xml_text: str) -> tuple[list[str], list[str]]:
    """
    Returns (sitemap_locs, url_locs)
    Supports <sitemapindex> and <urlset>.
    """
    if not xml_text:
        return [], []

    try:
        root = ET.fromstring(xml_text.encode("utf-8", errors="ignore"))
    except Exception:
        # některé servery vrací XML s BOM / divnými znaky
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            return [], []

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    sitemap_locs: list[str] = []
    url_locs: list[str] = []

    if root.tag.endswith("sitemapindex"):
        for sm in root.findall(f"{ns}sitemap"):
            loc = sm.find(f"{ns}loc")
            if loc is not None and loc.text:
                sitemap_locs.append(loc.text.strip())
    elif root.tag.endswith("urlset"):
        for u in root.findall(f"{ns}url"):
            loc = u.find(f"{ns}loc")
            if loc is not None and loc.text:
                url_locs.append(loc.text.strip())

    return sitemap_locs, url_locs


def fetch_urls_from_sitemaps(
    base_url: str,
    headers: Optional[dict] = None,
    contains_all: Iterable[str] = (),
    contains_any: Iterable[str] = (),
    max_sitemaps: int = 50,
    max_urls: int = 2000,
    timeout: int = 30,
) -> List[str]:
    """
    Very small sitemap crawler.
    - tries: /sitemap.xml, /sitemap_index.xml
    - follows sitemapindex => nested urlsets
    - filters URLs by contains_all / contains_any (case-insensitive)
    """
    base = (base_url or "").rstrip("/")
    candidates = [f"{base}/sitemap.xml", f"{base}/sitemap_index.xml", f"{base}/sitemapindex.xml"]

    all_need = [c.lower() for c in contains_all if c]
    any_need = [c.lower() for c in contains_any if c]

    def ok(u: str) -> bool:
        ul = (u or "").lower()
        if any_need and not any(k in ul for k in any_need):
            return False
        if any(k not in ul for k in all_need):
            return False
        return True

    seen_sitemaps: set[str] = set()
    queue: list[str] = []
    out: list[str] = []

    # pick first reachable sitemap root
    root_xml = ""
    for sm in candidates:
        sc, final, txt = _get(sm, headers=headers, timeout=timeout)
        if sc == 200 and txt:
            queue.append(final)
            root_xml = txt
            seen_sitemaps.add(final)
            break

    if not queue:
        return []

    # seed parse
    sm_locs, url_locs = _parse_sitemap_locs(root_xml)
    queue.extend([x for x in sm_locs if x and x not in seen_sitemaps])
    for u in url_locs:
        if ok(u):
            out.append(u)
            if len(out) >= max_urls:
                return out[:max_urls]

    # crawl nested sitemaps
    while queue and len(seen_sitemaps) < max_sitemaps and len(out) < max_urls:
        sm = queue.pop(0)
        if sm in seen_sitemaps:
            continue
        seen_sitemaps.add(sm)

        sc, final, txt = _get(sm, headers=headers, timeout=timeout)
        if sc != 200 or not txt:
            continue

        sm_locs, url_locs = _parse_sitemap_locs(txt)
        for nxt in sm_locs:
            if nxt and nxt not in seen_sitemaps and len(seen_sitemaps) + len(queue) < max_sitemaps:
                queue.append(nxt)

        for u in url_locs:
            if ok(u):
                out.append(u)
                if len(out) >= max_urls:
                    break

    # de-dup preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq[:max_urls]