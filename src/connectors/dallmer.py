# src/connectors/dallmer.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re
import json
import gzip
import csv
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

BASE_COM = "https://www.dallmer.com"
BASE_DE = "https://www.dallmer.de"

# “Relevant” keywords pro sprchové žlaby / systémy
KEYWORDS = [
    "shower-channel",
    "duschrinne",
    "ceraline",
    "dallflex",
    "cerawall",
    "cerafloor",
    "ceraniveau",
    "ceraframe",
]


def _project_root() -> Path:
    # .../src/connectors/dallmer.py -> parents[2] = project root (drain_benchmark_app)
    return Path(__file__).resolve().parents[2]


def _abs(href: str, base_url: str) -> str:
    return urljoin(base_url, href or "")


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _safe_get_bytes(url: str, timeout: int = 45) -> Tuple[Optional[int], str, bytes, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.content or b""), ""
    except Exception as e:
        return None, url, b"", f"{type(e).__name__}: {e}"


# ----------------------------
# Robust extraction of product URLs from HTML/JS/JSON
# ----------------------------

# absolute URL containing /produkte/..php
_RE_ABS_PROD = re.compile(r"https?://[^\s\"'>]+/[^\"'>]*/produkte/[^\"'>]+?\.php", re.IGNORECASE)
# relative URL containing /xx/produkte/..php
_RE_REL_PROD = re.compile(r"(/(?:[a-z]{2,3}|int)/produkte/[^\"'>]+?\.php)", re.IGNORECASE)
# escaped JSON form \/en\/produkte\/...php
_RE_ESC_PROD = re.compile(r"(?:\\?/)(?:[a-z]{2,3}|int)(?:\\?/)(?:produkte)(?:\\?/)[^\"'>]+?\.php", re.IGNORECASE)


def _extract_product_links_anywhere(html: str, base_url: str) -> List[str]:
    if not html:
        return []

    # normalize escaped slashes
    h_norm = html.replace("\\/", "/")

    links = set()

    # soup hrefs
    soup = BeautifulSoup(h_norm, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "/produkte/" in href.lower() and href.lower().endswith(".php"):
            links.add(_abs(href, base_url))

    # regex absolute
    for m in _RE_ABS_PROD.finditer(h_norm):
        links.add(m.group(0))

    # regex relative
    for m in _RE_REL_PROD.finditer(h_norm):
        links.add(_abs(m.group(1), base_url))

    # regex escaped (using original html, but de-escape)
    for m in _RE_ESC_PROD.finditer(html):
        u = m.group(0).replace("\\/", "/")
        if u.startswith("/"):
            links.add(_abs(u, base_url))

    return sorted(links)


# ----------------------------
# Robots / sitemap crawl (best-effort)
# ----------------------------

_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


def _robots_sitemaps(base_url: str) -> Tuple[List[str], Dict[str, Any]]:
    robots = base_url.rstrip("/") + "/robots.txt"
    st, final, txt, err = _safe_get_text(robots, timeout=25)
    dbg = {
        "site": "dallmer",
        "seed_url": robots,
        "status_code": st,
        "final_url": final,
        "error": err,
        "candidates_found": 0,
        "method": "robots",
        "is_index": None,
    }

    sitemaps: List[str] = []
    if st == 200 and txt:
        for line in txt.splitlines():
            if line.lower().startswith("sitemap:"):
                u = line.split(":", 1)[1].strip()
                if u.startswith("http"):
                    sitemaps.append(u)

    sitemaps = list(dict.fromkeys(sitemaps))
    dbg["candidates_found"] = len(sitemaps)
    return sitemaps, dbg


def _extract_sitemap_urls(payload: bytes) -> Tuple[List[str], bool]:
    if not payload:
        return [], False

    # gzip?
    if payload[:2] == b"\x1f\x8b":
        try:
            payload = gzip.decompress(payload)
        except Exception:
            return [], False

    txt = payload.decode("utf-8", errors="ignore").strip()
    if not txt:
        return [], False

    # plain list
    if "<" not in txt[:200] and "http" in txt:
        urls = [u.strip() for u in re.split(r"\s+", txt) if u.strip().startswith("http")]
        xmlish = sum(1 for u in urls if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(urls)))) if urls else False
        return urls, is_index

    locs = [m.group(1).strip() for m in _LOC_RE.finditer(txt) if m.group(1).strip()]
    if not locs:
        return [], False

    is_index = ("<sitemapindex" in txt.lower())
    if not is_index:
        xmlish = sum(1 for u in locs if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(locs))))

    return locs, is_index


def _crawl_sitemaps(start_sitemaps: List[str], max_sitemaps: int = 250, max_pages: int = 200000) -> Tuple[List[str], List[Dict[str, Any]]]:
    seen = set()
    queue = list(start_sitemaps)
    pages: List[str] = []
    debug: List[Dict[str, Any]] = []

    while queue and len(seen) < max_sitemaps and len(pages) < max_pages:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)

        st, final, body, err = _safe_get_bytes(sm, timeout=45)
        if st != 200 or not body:
            debug.append({
                "site": "dallmer",
                "seed_url": sm,
                "status_code": st,
                "final_url": final,
                "error": err,
                "candidates_found": 0,
                "method": "sitemap",
                "is_index": None,
            })
            continue

        urls, is_index = _extract_sitemap_urls(body)
        debug.append({
            "site": "dallmer",
            "seed_url": sm,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": len(urls),
            "method": "sitemap",
            "is_index": bool(is_index),
        })

        if not urls:
            continue

        if is_index:
            urls_sorted = sorted(
                urls,
                key=lambda u: (0 if any(k in u.lower() for k in ["prod", "product", "produkte", "article"]) else 1, u),
            )
            for u in urls_sorted:
                if u not in seen:
                    queue.append(u)
        else:
            pages.extend(urls)

    return list(dict.fromkeys(pages)), debug


# ----------------------------
# Local fallback seeds (project-only)
# ----------------------------

def _load_seed_file() -> Tuple[List[str], Optional[Dict[str, Any]]]:
    p = _project_root() / "data" / "seeds" / "dallmer_urls.txt"
    if not p.exists():
        return [], None
    urls = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("http"):
            urls.append(s)
    urls = list(dict.fromkeys(urls))
    dbg = {
        "site": "dallmer",
        "seed_url": str(p),
        "status_code": 200,
        "final_url": str(p),
        "error": "",
        "candidates_found": len(urls),
        "method": "local_seed_file",
        "is_index": None,
    }
    return urls, dbg


def _load_urls_from_previous_runs(limit_runs: int = 40) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    runs_dir = _project_root() / "data" / "runs"
    if not runs_dir.exists():
        return [], None

    runs = sorted([p for p in runs_dir.glob("update_*") if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)

    urls = []
    scanned = 0
    for run in runs[:limit_runs]:
        f = run / "outputs" / "products.csv"
        if not f.exists():
            continue
        scanned += 1
        try:
            with f.open("r", encoding="utf-8", errors="ignore", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    m = (row.get("manufacturer") or "").strip().lower()
                    u = (row.get("product_url") or row.get("sources") or "").strip()
                    if m == "dallmer" and u.startswith("http"):
                        urls.append(u)
        except Exception:
            continue

    urls = list(dict.fromkeys(urls))
    dbg = {
        "site": "dallmer",
        "seed_url": str(runs_dir),
        "status_code": 200 if scanned else None,
        "final_url": str(runs_dir),
        "error": "" if urls else "No Dallmer URLs in previous runs.",
        "candidates_found": len(urls),
        "method": "local_runs_cache",
        "is_index": None,
    }
    return urls, dbg


# ----------------------------
# Candidate classification (Products vs Components)
# ----------------------------

def _classify_candidate(url: str) -> str:
    ul = (url or "").lower()

    # components / příslušenství
    if any(k in ul for k in [
        "drain-body",
        "cover-plate",
        "installation-kit",
        "fire",
        "sound",
        "insulating",
        "collar",
        "pad",
        "adapter",
        "element",
        "accessor",
    ]):
        return "component"

    # žlaby / kanály
    if any(k in ul for k in ["shower-channel", "duschrinne"]):
        return "product"

    # default
    return "product"


# ----------------------------
# Discovery
# ----------------------------

def _length_from_url(u: str) -> Optional[int]:
    ul = (u or "").lower()
    m = re.search(r"-(\d{3,4})-mm", ul)
    if m:
        try:
            v = int(m.group(1))
            if 300 <= v <= 2000:
                return v
        except Exception:
            return None
    return None

# ============================================================
# SKU DEDUPLICATION HELPER
# ============================================================

def _dedupe_found_links_by_sku(found_links: List[str]):
    """
    Deduplicate Dallmer product URLs by 6-digit SKU.
    Preference:
        1) dallmer.com
        2) /en/produkte/
        3) /de/produkte/
        4) shorter URL
    Returns:
        deduped_links,
        debug_rows,
        sources_map (original sources per final URL)
    """

    import re

    debug_rows = []
    sources_map: Dict[str, str] = {}

    def _sku_from_url(u: str):
        m = re.search(r"/(\d{6})_", u)
        return m.group(1) if m else None

    def _rank(u: str):
        ul = u.lower()
        return (
            0 if "dallmer.com" in ul else 1,
            0 if "/en/produkte/" in ul else 1,
            0 if "/de/produkte/" in ul else 2,
            len(u),
        )

    by_sku: Dict[str, List[str]] = {}
    no_sku: List[str] = []

    for u in found_links:
        sku = _sku_from_url(u)
        if sku:
            by_sku.setdefault(sku, []).append(u)
        else:
            no_sku.append(u)

    deduped: List[str] = []

    removed_count = 0

    for sku, urls in by_sku.items():
        if len(urls) > 1:
            removed_count += len(urls) - 1
        best = sorted(urls, key=_rank)[0]
        deduped.append(best)
        sources_map[best] = " | ".join(urls)

    deduped.extend(no_sku)

    if removed_count > 0:
        debug_rows.append({
            "site": "dallmer",
            "seed_url": "sku_dedupe",
            "status_code": None,
            "final_url": None,
            "error": "",
            "candidates_found": len(deduped),
            "method": f"dedupe_removed_{removed_count}",
            "is_index": None,
        })

    return deduped, debug_rows, sources_map

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    debug: List[Dict[str, Any]] = []
    found_links: List[str] = []

    queries = [
        f"ceraline {want} dn",
        f"shower channel {want} dn",
        f"duschrinne {want} dn",
        "ceraline dn",
        "shower channel dn",
        "duschrinne dn",
    ]

    # (A) search on .com
    for q in queries:
        url = f"{BASE_COM}/en/search/?searchTerm={requests.utils.quote(q)}"
        st, final, html, err = _safe_get_text(url, timeout=35)
        links = _extract_product_links_anywhere(html, final) if st == 200 else []
        debug.append({
            "site": "dallmer",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": len(links),
            "method": "search_com",
            "is_index": None,
        })
        found_links.extend(links)

    # (B) search on .de
    for q in queries:
        url = f"{BASE_DE}/de/search/index.php?searchTerm={requests.utils.quote(q)}"
        st, final, html, err = _safe_get_text(url, timeout=35)
        links = _extract_product_links_anywhere(html, final) if st == 200 else []
        debug.append({
            "site": "dallmer",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": len(links),
            "method": "search_de",
            "is_index": None,
        })
        found_links.extend(links)

    found_links = list(dict.fromkeys(found_links))

    # (C) robots+sitemap fallback
    if len(found_links) < 5:
        sitemaps = []
        sms, dbg = _robots_sitemaps(BASE_COM)
        debug.append(dbg)
        sitemaps.extend(sms)

        sms, dbg = _robots_sitemaps(BASE_DE)
        debug.append(dbg)
        sitemaps.extend(sms)

        if not sitemaps:
            sitemaps = [
                BASE_COM + "/sitemap.xml",
                BASE_COM + "/sitemap.xml.gz",
                BASE_DE + "/sitemap.xml",
                BASE_DE + "/sitemap.xml.gz",
            ]

        urls, dbg_rows = _crawl_sitemaps(sitemaps, max_to_crawl=18)
        debug.extend(dbg_rows)

        for u in urls:
            if "/produkte/" in (u or "").lower() and u.endswith(".php"):
                found_links.append(u)

        found_links = list(dict.fromkeys(found_links))

    # (D) catalog pages fallback
    if len(found_links) < 5:
        catalog_pages = [
            BASE_COM + "/en/produkte/",
            BASE_DE + "/de/produkte/",
        ]

        for page in catalog_pages:
            st, final, html, err = _safe_get_text(page, timeout=35)
            links = _extract_product_links_anywhere(html, final) if st == 200 else []
            debug.append({
                "site": "dallmer",
                "seed_url": page,
                "status_code": st,
                "final_url": final,
                "error": err,
                "candidates_found": len(links),
                "method": "catalog",
                "is_index": None,
            })

            for u in links:
                if "/produkte/" in (u or "").lower() and u.endswith(".php"):
                    found_links.append(u)

        found_links = list(dict.fromkeys(found_links))

    # (E) previous runs fallback
    if len(found_links) < 5:
        urls, dbg = _load_urls_from_previous_runs()
        if dbg:
            debug.append(dbg)

        found_links.extend(urls)
        found_links = list(dict.fromkeys(found_links))

    # (F) SKU DEDUPE — MUSÍ být na stejné úrovni jako (C)(D)(E)
    sources_map: Dict[str, str] = {}
    found_links, dedupe_dbg, sources_map = _dedupe_found_links_by_sku(found_links)
    if dedupe_dbg:
        debug.extend(dedupe_dbg)

    # final filters + build
    out: List[Dict[str, Any]] = []

    for u in sorted(found_links):
        ul = u.lower()

        if "/produkte/" not in ul or not ul.endswith(".php"):
            continue

        if not any(k in ul for k in KEYWORDS):
            continue

        L = _length_from_url(u)
        if L is not None and not (min_len <= L <= max_len):
            continue

        ct = _classify_candidate(u)

        out.append({
            "manufacturer": "dallmer",
            "product_family": "Drain",
            "product_name": u.split("/")[-1].replace("-", " "),
            "product_url": u,
            "sources": sources_map.get(u, u),
            "candidate_type": ct,
            "complete_system": "yes" if ct == "product" else "component",
            "selected_length_mm": want,
            "length_mode": "url" if L is not None else "unknown",
            "length_delta_mm": (L - want) if L is not None else None,
        })

        if len(out) >= 700:
            break

    debug.append({
        "site": "dallmer",
        "seed_url": "search+sitemap+local",
        "status_code": 200 if out else None,
        "final_url": "search+sitemap+local",
        "error": "",
        "candidates_found": len(out),
        "method": "final",
        "is_index": None,
    })

    return out, debug
# ----------------------------
# Extraction helpers (smart height / DN / flow options)
# ----------------------------

def _extract_best_height_mm(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Smart height extraction:
    1) preferuje keyword + range A–B mm
    2) potom keyword + single
    3) potom fallback range kdekoliv
    Ochrana: hodnoty musí být <= 300 mm (aby se nechytaly délky 900–1200 mm).
    """
    if not text:
        return None, None, None

    t = " ".join(text.split())
    KEY = r"(?:Höheneinstellung|Höhenverstellung|höhenverstellbar|Adjustable height|Einbauhöhe|Bauhöhe|Installation height|Höhe|Height)"

    def ok(a: int, b: int) -> bool:
        return 1 <= a <= 300 and 1 <= b <= 300 and b >= a

    # 1) keyword + range
    for m in re.finditer(rf"{KEY}.{{0,80}}?(\d{{1,4}})\s*[-–]\s*(\d{{1,4}})\s*mm", t, flags=re.IGNORECASE):
        try:
            a = int(m.group(1))
            b = int(m.group(2))
            if ok(a, b):
                return a, b, m.group(0)
        except Exception:
            continue

    # 2) keyword + single
    for m in re.finditer(rf"{KEY}.{{0,50}}?(\d{{1,4}})\s*mm", t, flags=re.IGNORECASE):
        try:
            a = int(m.group(1))
            if ok(a, a):
                return a, a, m.group(0)
        except Exception:
            continue

    # 3) fallback range anywhere
    for m in re.finditer(r"(\d{1,4})\s*[-–]\s*(\d{1,4})\s*mm", t):
        try:
            a = int(m.group(1))
            b = int(m.group(2))
            if ok(a, b):
                return a, b, m.group(0)
        except Exception:
            continue

    return None, None, None


def _dns_from_text(text: str) -> List[str]:
    if not text:
        return []
    t = _clean_text(text)
    dns = set()

    for m in re.finditer(r"\bDN\s*0?(\d{2,3})\b", t, flags=re.IGNORECASE):
        dns.add(f"DN{m.group(1)}")
    for m in re.finditer(r"\bdn[-_\s]?(\d{2,3})\b", (text or "").lower()):
        dns.add(f"DN{m.group(1)}")

    return sorted(dns, key=lambda x: int(re.sub(r"\D", "", x) or "0"))


def _format_dn(dns: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not dns:
        return None, None, None
    dns = sorted(set(dns), key=lambda x: int(re.sub(r"\D", "", x) or "0"))

    if len(dns) == 1:
        dn = dns[0]
        return dn, dn, json.dumps([dn], ensure_ascii=False)

    display = "/".join(dns)  # DN40/DN50
    default = "DN50" if "DN50" in dns else dns[0]
    return display, default, json.dumps(dns, ensure_ascii=False)


def _extract_flow_options_json(text: str) -> Optional[str]:
    if not text:
        return None
    t = _clean_text(text)
    opts = []
    for m in re.finditer(r"([\d.,]+)\s*(l\s*/\s*s|l\s*/\s*sek|l\s*/\s*min|l\s*/\s*m|I\s*/\s*s)", t, flags=re.IGNORECASE):
        try:
            v = float(m.group(1).replace(",", "."))
            unit = m.group(2).lower().replace(" ", "")
            if "min" in unit or "/m" in unit:
                v = v / 60.0
            if 0.05 <= v <= 5.0:
                opts.append(round(v, 4))
        except Exception:
            pass
    opts = sorted(set(opts))
    return json.dumps(opts, ensure_ascii=False) if opts else None




def _guess_pdb_pdf_links(product_url: str) -> List[str]:
    m = re.search(r"/(\d{6})_", product_url or "")
    if not m:
        return []

    sku = m.group(1)
    ul = (product_url or "").lower()

    lang_variants: List[Tuple[str, str]] = []
    if "/en/" in ul:
        lang_variants = [("en", "EN")]
    elif "/de/" in ul:
        lang_variants = [("de", "DE")]
    else:
        lang_variants = [("en", "EN"), ("de", "DE")]

    bases = [BASE_COM]
    try:
        pu = urlparse(product_url or "")
        if pu.scheme and pu.netloc:
            bases.append(f"{pu.scheme}://{pu.netloc}")
    except Exception:
        pass

    out: List[str] = []
    for base in list(dict.fromkeys(bases)):
        b = base.rstrip("/")
        for lang, lang_up in lang_variants:
            out.append(f"{b}/default-wAssets/docs/{lang}/pdb/_{sku}_{lang_up}.pdf")
            out.append(f"{b}/default-wAssets/docs/{lang}/pdb/{sku}_{lang_up}.pdf")

    return list(dict.fromkeys(out))


def _find_pdf_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
    pdfs = []
    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        hl = href.lower()
        if any(x in hl for x in ["agb", "datenschutz", "privacy", "garantie", "warranty", "katalog", "catalog", "montage"]):
            continue
        if ".pdf" in hl:
            pdfs.append(_abs(href, base_url))
    return list(dict.fromkeys(pdfs))


# ----------------------------
# Public API
# ----------------------------

def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    # u Dallmeru BOM zatím neřešíme
    return []


def extract_parameters(product_url: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_raw_text": None,
        "flow_rate_unit": None,
        "flow_rate_status": None,
        "flow_rate_lps_options": None,  # JSON string

        "material_detail": None,
        "material_v4a": None,

        "din_en_1253_cert": None,
        "din_18534_compliance": None,

        "height_adj_min_mm": None,
        "height_adj_max_mm": None,

        "outlet_dn": None,              # DN50 nebo DN40/DN50
        "outlet_dn_default": None,
        "outlet_dn_options_json": None,

        "sealing_fleece_preassembled": None,
        "colours_count": None,

        "evidence": [],
    }

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))

    page_text = ""
    pdf_links: List[str] = []
    guessed_pdf_links: List[str] = []
    if st == 200 and html:
        soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
        page_text = soup.get_text(" ", strip=True) or ""
        pdf_links = _find_pdf_links(html, final)

    # DN
    dns = _dns_from_text(src + " " + page_text)
    dn_disp, dn_def, dn_json = _format_dn(dns)
    if dn_disp:
        res["outlet_dn"] = dn_disp
        res["outlet_dn_default"] = dn_def
        res["outlet_dn_options_json"] = dn_json
        res["evidence"].append(("Outlet DN options", dn_json, src))
        res["evidence"].append(("Outlet DN default", dn_def or "", src))

    # HTML parse
    if page_text:
        # flow best
        lps, raw_txt, unit, status = select_flow_rate(page_text)
        if lps is not None:
            res["flow_rate_lps"] = lps
            res["flow_rate_raw_text"] = raw_txt
            res["flow_rate_unit"] = unit
            res["flow_rate_status"] = status
            if raw_txt:
                res["evidence"].append(("Flow rate", raw_txt, final))

        # flow options JSON
        opts_json = _extract_flow_options_json(page_text)
        if opts_json and res["flow_rate_lps_options"] is None:
            res["flow_rate_lps_options"] = opts_json
            res["evidence"].append(("Flow rate options", opts_json, final))

        # smart height
        hmin, hmax, hsnip = _extract_best_height_mm(page_text)
        if hmin is not None and hmax is not None:
            res["height_adj_min_mm"] = hmin
            res["height_adj_max_mm"] = hmax
            if hsnip:
                res["evidence"].append(("Height adjustability", hsnip, final))

    # PDF only if needed (speed-up)
    need_pdf = (res.get("flow_rate_lps") is None) or (res.get("height_adj_min_mm") is None)
    if not pdf_links and (res.get("flow_rate_lps") is None or need_pdf):
        guessed_pdf_links = _guess_pdb_pdf_links(final)
        pdf_links = list(guessed_pdf_links)

    guessed_pdf_links_set = set(guessed_pdf_links)
    if need_pdf:
        for pdf_url in (pdf_links or [])[:3]:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            status_ok = str(pdf_status).lower().startswith("ok") or str(pdf_status).strip() == "200"
            if status_ok and pdf_url in guessed_pdf_links_set:
                res["evidence"].append(("PDF guess", pdf_url, product_url))
            if not pdf_text:
                continue

            if res.get("flow_rate_lps") is None:
                lps, raw_txt, unit, status = select_flow_rate(pdf_text)
                if lps is not None:
                    res["flow_rate_lps"] = lps
                    res["flow_rate_raw_text"] = raw_txt
                    res["flow_rate_unit"] = unit
                    res["flow_rate_status"] = status
                    if raw_txt:
                        res["evidence"].append(("Flow rate", raw_txt, pdf_url))

            if res.get("flow_rate_lps_options") is None:
                opts_json = _extract_flow_options_json(pdf_text)
                if opts_json:
                    res["flow_rate_lps_options"] = opts_json
                    res["evidence"].append(("Flow rate options", opts_json, pdf_url))

            if res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None:
                hmin, hmax, hsnip = _extract_best_height_mm(pdf_text)
                if hmin is not None and hmax is not None:
                    res["height_adj_min_mm"] = hmin
                    res["height_adj_max_mm"] = hmax
                    if hsnip:
                        res["evidence"].append(("Height adjustability", hsnip, pdf_url))

            if res.get("outlet_dn_options_json") is None:
                dns2 = _dns_from_text(pdf_text)
                dn_disp, dn_def, dn_json = _format_dn(dns2)
                if dn_disp:
                    res["outlet_dn"] = dn_disp
                    res["outlet_dn_default"] = dn_def
                    res["outlet_dn_options_json"] = dn_json
                    res["evidence"].append(("Outlet DN options", dn_json, pdf_url))
                    res["evidence"].append(("Outlet DN default", dn_def or "", pdf_url))

            if res.get("flow_rate_lps") is not None and res.get("height_adj_min_mm") is not None:
                break

    # default DN50
    if res.get("outlet_dn") is None:
        res["outlet_dn"] = "DN50"
        res["outlet_dn_default"] = "DN50"
        res["outlet_dn_options_json"] = json.dumps(["DN50"], ensure_ascii=False)
        res["evidence"].append(("Outlet DN options", res["outlet_dn_options_json"], src))
        res["evidence"].append(("Outlet DN default", "DN50", src))

    return res
