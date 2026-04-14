from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..pdf_text import extract_pdf_text_from_url

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

BASE = "https://www.viega.de"
DETAIL_SCOPE = "/de/produkte/Katalog/"
CATALOG_SEEDS = [
    f"{BASE}/de/produkte/entwaesserungstechnik/im-bad.html",
    f"{BASE}/de/produkte/entwaesserungstechnik/im-bad/duschwannengarnituren.html",
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen.html",
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe.html",
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen.html",
]
DETAIL_SEEDS = [
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Cleviva-Duschrinnen/Einbauhoehe-ab-95-mm/Advantix-Cleviva-Duschrinne-4981-10.html",
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen/Advantix-Duschrinnen-Einbauhoehe-ab-95/Advantix-Duschrinne-4983-10.html",
    f"{BASE}/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html",
]
CATEGORY_SEEDS = [
    *CATALOG_SEEDS,
]
SEED_FAMILY_HINTS: Dict[str, str] = {
    "advantix-duschrinnen": "advantix_line",
    "advantix-cleviva": "advantix_line",
    "advantix-vario": "advantix_line",
    "advantix-bodenablaeufe": "advantix_floor",
    "advantix-eckablaeufe": "advantix_corner",
    "duschwannengarnituren": "shower_tray",
    "tempoplex": "shower_tray",
    "domoplex": "shower_tray",
    "duoplex": "shower_tray",
    "varioplex": "shower_tray",
}

DETAIL_URL_RE = re.compile(r"-\d{4,5}-\d{2}\.html$", re.IGNORECASE)
ARTICLE_FROM_URL_RE = re.compile(r"-(\d{4,5}-\d{2})\.html(?:$|[?#])", re.IGNORECASE)
LENGTH_RE_LIST = [
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d)\.(\d{3})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})mm\b", re.IGNORECASE),
]
LENGTH_RANGE_RE = re.compile(r"\b(\d{3,4})\s*[-–]\s*(\d{3,4})\s*mm\b", re.IGNORECASE)
VARIABLE_LEN_RE = re.compile(r"\b(vario|variabel|stufenlos|kuerzbar|kürzbar|l\s*\d{3,4}\s*[-–]\s*\d{3,4})\b", re.IGNORECASE)
COMPONENT_KEYWORDS = (
    "zubehoer", "zubehör", "rost", "abdeckung", "einleger", "profil", "rahmen",
    "geruchverschluss", "geruchsverschluss", "ablaufbogen", "ablauf", "siphon",
    "montage", "werkzeug", "schallschutz", "dicht", "fliesen", "adapter",
)
COMPONENT_ONLY_RE = re.compile(
    r"grundk[öo]rper|geruchverschluss|rost|profil|einleger|rahmen|zubeh[öo]r|werkzeug|abdeckung",
    re.IGNORECASE,
)
UNRELATED_BRANCH_RE = re.compile(
    r"ersatzteile-fuer-advantix-systeme|ersatzteile-fuer-advantix-systeme-und-rueckstauverschluesse|rueckstauverschluesse|spuelen-ausguesse|waschtische-und-bidets|wcs-und-urinale|balkon--und-terrassenablaeufe|kueche|urinal|wc|ausguesse",
    re.IGNORECASE,
)
SPARE_TOKEN_RE = re.compile(
    r"dichtung|o-ring|glocke|stopfen|tauchrohr|siebeinsatz|schraubenset|sicherungsverschluss|sicherungsschraubenset|ersatzteilset|montageset|abdeckhaube|rosette|rohrf[üu]hrung|rohreinf[üu]hrung|verlaengerung|verlängerung|einsatz|klappe|handbetaetigung|akku|anschlussset|ersatzteil",
    re.IGNORECASE,
)
MEANINGFUL_ENTITY_RE = re.compile(
    r"duschrinne|bodenablauf|eckablauf|wandablauf|\bablauf\b|grundk[öo]rper|rost|profil|abdeckung",
    re.IGNORECASE,
)
RELEVANT_FAMILY_RE = re.compile(
    r"advantix-duschrinnen|advantix-cleviva|cleviva|advantix-vario|vario-wand|advantix-eckablaeufe|advantix-bodenablaeufe|tempoplex|tempoplex-plus|tempoplex-60|domoplex|duoplex|varioplex|duschwannengarnituren|ablaeufe-fuer-bade--und-duschwannen",
    re.IGNORECASE,
)

# strict DN parsing; only literal DN and allowed outlet sizes
DN_PAIR_RE = re.compile(r"\bDN\s*(\d{2,3})\s*/\s*(?:DN\s*)?(\d{2,3})\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\b(?:Nennweite\s*)?DN\s*(\d{2,3})\b", re.IGNORECASE)

FLOW_LPS_RE = re.compile(r"(?<!\d)(\d{1,2}(?:[\.,]\d{1,2})?)\s*l/s\b", re.IGNORECASE)
FLOW_REJECT_RE = re.compile(r"reduziert\s+um|reduziert|reduzieren|reduzierung", re.IGNORECASE)

HEIGHT_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm",
    re.IGNORECASE,
)
HEIGHT_SINGLE_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*mm",
    re.IGNORECASE,
)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:ö|oe)he|geruchsverschluss|water\s+seal", re.IGNORECASE)


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _main_flat_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main")
    if main is not None:
        return _clean_text(main.get_text(" ", strip=True) or "")
    for sel in ["header", "nav", "footer", "aside"]:
        for tag in soup.select(sel):
            tag.decompose()
    return _clean_text(soup.get_text(" ", strip=True) or "")


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    h1 = soup.select_one("h1")
    if h1:
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            return t
    t = soup.select_one("title")
    if t:
        x = _clean_text(t.get_text(" ", strip=True))
        if x:
            return x
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _product_id_from_url(url: str) -> str:
    m = ARTICLE_FROM_URL_RE.search(url or "")
    if m:
        return f"viega-{_digits_only(m.group(1))}"
    d = _digits_only((url or "").split("/")[-1])
    if len(d) >= 6:
        return f"viega-{d}"
    return f"viega-{abs(hash(url or ''))}"


def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


def _normalize_discovered_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0]


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        path = (p.path or "").lower()
        if not p.netloc.endswith("viega.de"):
            return False
        if not path.startswith("/de/produkte/"):
            return False
        return bool(re.search(r"entwaesserungstechnik|dusch|duschrinne|advantix|tempoplex|domoplex|duoplex|varioplex|ablauf", path, re.IGNORECASE))
    except Exception:
        return False


def _is_detail_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return _in_scope(url) and bool(DETAIL_URL_RE.search(p.path or ""))
    except Exception:
        return False


def _is_rost_component(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return "rost" in txt


def _has_component_keyword(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in COMPONENT_KEYWORDS)


def _classify_candidate(url: str, title: str, flat: str = "") -> str:
    u = (url or "").lower()
    t = f"{title} {flat}".lower()
    if "/zubehoer/" in u or "/zubehör/" in u or _has_component_keyword(f"{u} {t}"):
        return "component"
    if COMPONENT_ONLY_RE.search(t):
        return "component"
    if ("duschrinne" in t or "duschrinnen" in t) and not _has_component_keyword(t):
        return "drain"
    if "tempoplex" in t and not _has_component_keyword(t):
        return "drain"
    return "component"


def _extract_breadcrumb_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for sel in ["nav.breadcrumb", "[class*='breadcrumb']", "[aria-label*='Brotkrume']", "[aria-label*='breadcrumb']"]:
        node = soup.select_one(sel)
        if node:
            t = _clean_text(node.get_text(" ", strip=True))
            if t:
                return t
    return ""


def _derive_taxonomy(url: str, title: str, flat: str = "", breadcrumb: str = "", article_text: str = "") -> Tuple[str, str, str, str]:
    txt = f"{url} {title} {flat} {breadcrumb} {article_text}".lower()
    system_role = "complete_drain"
    drain_category = "unknown"

    is_tempoplex = bool(re.search(r"tempoplex|domoplex|duoplex|varioplex", txt))
    if re.search(r"zubeh[öo]r|werkzeug|rahmen|einleger|ersatzteil|montageset|sicherungsverschluss|dichtung|o-ring|glocke|stopfen|schraubenset|siebeinsatz|rohrf[üu]hrung|rohreinf[üu]hrung|akku|klappe|handbet[äa]tigung", txt):
        system_role = "accessory"
    elif re.search(r"rost|abdeckung", txt):
        system_role = "cover"
    elif re.search(r"profil", txt):
        system_role = "profile"
    elif re.search(r"duschrinne|bodenablauf|eckablauf|wandablauf", txt):
        system_role = "complete_drain"
    elif is_tempoplex and re.search(r"\bablauf\b", txt):
        system_role = "complete_drain"
    elif re.search(r"grundk[öo]rper|geruchverschluss|ablaufk[öo]rper|ablaufgeh[äa]use", txt):
        system_role = "base_set"
    elif re.search(r"ablauf(?!leistung)", txt) and not is_tempoplex:
        system_role = "base_set"

    if system_role == "accessory":
        drain_category = "accessory"
    elif re.search(r"tempoplex|domoplex|duoplex|varioplex|duschwanne", txt):
        drain_category = "shower_tray_drain"
    elif re.search(r"wandablauf|wandrinne|wandprofil|wand", txt):
        drain_category = "wall_channel"
    elif re.search(r"eckablauf", txt):
        drain_category = "corner_drain"
    elif re.search(r"bodenablauf", txt):
        drain_category = "floor_drain"
    elif re.search(r"punktablauf", txt):
        drain_category = "point_drain"
    elif re.search(r"duschrinne|advantix|cleviva|vario", txt):
        drain_category = "line_channel"
    elif re.search(r"\bablauf\b", txt) and re.search(r"advantix|entwaesserungstechnik|boden", txt):
        drain_category = "point_drain"

    cand_type = "drain" if (system_role == "complete_drain" and drain_category != "accessory") else "component"
    complete_system = "yes" if cand_type == "drain" else "component"
    return cand_type, drain_category, system_role, complete_system


def _infer_family(url: str, title: str = "") -> str:
    txt = f"{url} {title}".lower()
    for k, fam in SEED_FAMILY_HINTS.items():
        if k in txt:
            return fam
    if "advantix" in txt:
        return "advantix_other"
    return "other"


def _is_unrelated_branch(url: str, title: str = "") -> bool:
    return bool(UNRELATED_BRANCH_RE.search(f"{url} {title}"))


def _is_spare_part_like(url: str, title: str, flat: str, system_role: str) -> bool:
    txt = f"{url} {title} {flat}"
    if _is_unrelated_branch(url, title):
        return True
    if SPARE_TOKEN_RE.search(txt):
        return True
    if system_role == "accessory" and re.search(r"ersatzteil|ersatzteile|set\b", txt, re.IGNORECASE):
        return True
    return False


def _belongs_to_target_families(url: str, title: str, breadcrumb: str, drain_category: str) -> bool:
    txt = f"{url} {title} {breadcrumb}".lower()
    if drain_category in {"line_channel", "point_drain", "floor_drain", "corner_drain", "shower_tray_drain", "wall_channel"}:
        return True
    return bool(RELEVANT_FAMILY_RE.search(txt))


def _is_meaningful_system_entity(url: str, title: str, flat: str, system_role: str) -> bool:
    if system_role not in {"complete_drain", "base_set", "cover", "profile"}:
        return False
    txt = f"{url} {title} {flat}"
    return bool(MEANINGFUL_ENTITY_RE.search(txt))


def _extract_category_links_from_sortiment(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: Set[str] = set()

    # try explicit Sortiment section first
    sort_headers = soup.find_all(string=re.compile(r"sortiment", re.IGNORECASE))
    for sh in sort_headers:
        node = sh.parent
        container = node
        for _ in range(4):
            if container is None:
                break
            for a in container.select("a[href]"):
                u = _abs(a.get("href") or "", base_url)
                if _in_scope(u) and not _is_detail_url(u):
                    out.add(u)
            container = container.parent

    # fallback: all in-scope non-detail links from seed
    if not out:
        for a in soup.select("a[href]"):
            u = _abs(a.get("href") or "", base_url)
            if _in_scope(u) and not _is_detail_url(u):
                out.add(u)

    return out


def _crawl_category_pages(start_pages: Set[str], max_pages: int = 2000) -> Dict[str, Dict[str, Any]]:
    queue = list(start_pages)
    seen: Set[str] = set()
    details: Dict[str, Dict[str, Any]] = {}

    while queue and len(seen) < max_pages:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)

        st, final, html, _ = _safe_get_text(u, timeout=30)
        if st != 200 or not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            raw_href = a.get("href") or ""
            cand = _normalize_discovered_url(_abs(raw_href, final))
            if not _in_scope(cand):
                continue
            if _is_detail_url(cand):
                if cand not in details:
                    details[cand] = {
                        "raw_discovered_href": raw_href,
                        "normalized_detail_url": cand,
                        "href_source_page": final,
                        "was_synthetic_url": False,
                    }
            else:
                if cand not in seen and cand not in queue:
                    queue.append(cand)

    return details


def _extract_length_options(text: str) -> List[int]:
    out: List[int] = []
    seen = set()
    src = text or ""
    for rx in LENGTH_RE_LIST:
        for m in rx.finditer(src):
            try:
                if len(m.groups()) == 2:
                    v = int(m.group(1) + m.group(2))
                else:
                    v = int(m.group(1))
            except Exception:
                continue
            if 300 <= v <= 2500 and v not in seen:
                seen.add(v)
                out.append(v)
    return sorted(out)


def _resolve_length_from_text(flat: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    if VARIABLE_LEN_RE.search(flat or ""):
        m = VARIABLE_LEN_RE.search(flat or "")
        return None, (_snippet(flat, m.start(), m.end()) if m else "variable length"), "variable"
    opts = _extract_length_options(flat)
    if not opts:
        return None, None, None
    chosen = max(opts)
    m = re.search(rf"\b{chosen}\s*mm\b|\b{chosen}mm\b|\b{str(chosen)[:1]}\.{str(chosen)[1:]}\s*mm\b", flat, re.IGNORECASE)
    return chosen, (_snippet(flat, m.start(), m.end()) if m else f"{chosen} mm"), "fixed"


def _extract_pdf_candidates(html: str, base_url: str) -> List[Tuple[str, int]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[str, int]] = []
    seen = set()
    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        if ".pdf" not in href.lower():
            continue
        u = _abs(href, base_url)
        if u in seen:
            continue
        seen.add(u)
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        score = 0
        if "technische daten" in txt or "datenblatt" in txt:
            score += 4
        if "montage" in txt or "anleitung" in txt:
            score += 1
        out.append((u, score))
    out.sort(key=lambda x: x[1], reverse=True)
    return out


def _extract_dns_from_table(html: str) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html or "", "lxml")
    for table in soup.select("table"):
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        if not any("dn" in h or "nennweite" in h for h in headers):
            continue
        for tr in table.select("tr"):
            txt = _clean_text(tr.get_text(" ", strip=True))
            dns_row, _ = _extract_dns_from_text(txt)
            if dns_row:
                return dns_row, txt
    return [], None


def _parse_sections(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main") or soup
    out: Dict[str, str] = {"main_flat": _clean_text(main.get_text(" ", strip=True))}
    h1 = main.select_one("h1")
    if h1:
        out["title"] = _clean_text(h1.get_text(" ", strip=True))
    h2 = main.select_one("h2")
    if h2:
        out["subtitle"] = _clean_text(h2.get_text(" ", strip=True))
    return out


def _parse_article_table(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Dict[str, Any]] = []
    for table in soup.select("table"):
        header_cells = table.select("tr:first-child th, tr:first-child td")
        headers = [_clean_text(x.get_text(" ", strip=True)) for x in header_cells]
        norm = [h.lower() for h in headers]
        if not headers:
            continue
        if not any("artikel" in h or "art.-nr" in h or "art." in h for h in norm):
            continue
        for tr in table.select("tr")[1:]:
            vals = [_clean_text(x.get_text(" ", strip=True)) for x in tr.select("td,th")]
            if not vals:
                continue
            row: Dict[str, Any] = {headers[i] if i < len(headers) else f"col_{i}": vals[i] for i in range(len(vals))}
            row_txt = _clean_text(" ".join(vals))
            if not re.search(r"\d{4,5}[.\-]\d{2}", row_txt):
                continue
            row["_row_text"] = row_txt
            for i, h in enumerate(norm):
                if i >= len(vals):
                    continue
                v = vals[i]
                if h in {"l", "l mm", "länge", "laenge"} or h.startswith("l "):
                    m = re.search(r"(\d{3,4})", v)
                    if m:
                        row["length_mm"] = int(m.group(1))
                if "bh" in h:
                    m = re.search(r"(\d{2,3})(?:\s*[-–]\s*(\d{2,3}))?", v)
                    if m:
                        a, b = int(m.group(1)), int(m.group(2) or m.group(1))
                        row["hmin"], row["hmax"] = (a, b) if a <= b else (b, a)
                if "dn" in h:
                    dns, _ = _extract_dns_from_text(v)
                    if dns:
                        row["dns"] = dns
                if "ausführung" in h or "ausfuehrung" in h:
                    row["variant_label"] = v
                if "artikel" in h or "art." in h:
                    row["article_no"] = v
            fopts, _, _ = _extract_flow_from_ablaufleistung(row_txt)
            if fopts:
                row["flow_opts"] = fopts
            out.append(row)
    return out


def _select_article_row(rows: List[Dict[str, Any]], target_mm: int = 1200) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    with_len = [r for r in rows if isinstance(r.get("length_mm"), int)]
    if with_len:
        return sorted(with_len, key=lambda r: abs(int(r.get("length_mm", 0)) - target_mm))[0]
    return rows[0]


def _extract_flow_10_20(flat: str) -> Tuple[Optional[float], Optional[float]]:
    f10: Optional[float] = None
    f20: Optional[float] = None
    for m in re.finditer(r"anstauh[öo]he\s*10\s*mm[^\d]{0,20}(\d+(?:[\.,]\d+)?)(?:\s*[-–]\s*(\d+(?:[\.,]\d+)?))?\s*l/s", flat, re.IGNORECASE):
        vals = [float(m.group(1).replace(",", "."))]
        if m.group(2):
            vals.append(float(m.group(2).replace(",", ".")))
        f10 = max(vals)
    for m in re.finditer(r"anstauh[öo]he\s*20\s*mm[^\d]{0,20}(\d+(?:[\.,]\d+)?)(?:\s*[-–]\s*(\d+(?:[\.,]\d+)?))?\s*l/s", flat, re.IGNORECASE):
        vals = [float(m.group(1).replace(",", "."))]
        if m.group(2):
            vals.append(float(m.group(2).replace(",", ".")))
        f20 = max(vals)
    return f10, f20


def _extract_material_viega(flat: str) -> Tuple[Optional[str], Optional[str]]:
    src = flat or ""
    m = re.search(r"edelstahl\s*(1\.4301|1\.4404|1\.4571|304|316l?)?", src, re.IGNORECASE)
    if not m:
        return None, None
    token = (m.group(1) or "").lower()
    if token in {"1.4404", "1.4571", "316", "316l"} or re.search(r"\bv4a\b", src, re.IGNORECASE):
        return f"edelstahl {token}".strip(), "yes"
    if token in {"1.4301", "304"} or re.search(r"\bv2a\b", src, re.IGNORECASE):
        return f"edelstahl {token}".strip(), "no"
    return "edelstahl", None


def _extract_dns_from_text(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    first_span: Optional[Tuple[int, int]] = None

    for m in DN_PAIR_RE.finditer(flat):
        a = f"DN{m.group(1)}"
        b = f"DN{m.group(2)}"
        if a not in dns:
            dns.append(a)
        if b not in dns:
            dns.append(b)
        if first_span is None:
            first_span = (m.start(), m.end())

    for m in DN_SINGLE_RE.finditer(flat):
        dn = f"DN{m.group(1)}"
        if dn not in dns:
            dns.append(dn)
            if first_span is None:
                first_span = (m.start(), m.end())

    return sorted(dns), first_span


def _flow_value_if_valid(text: str, m: re.Match[str]) -> Optional[float]:
    try:
        v = float(m.group(1).replace(",", "."))
    except Exception:
        return None
    if v < 0.10 or v > 3.0:
        return None
    before = text[max(0, m.start() - 24):m.start()].lower()
    if FLOW_REJECT_RE.search(before):
        return None
    if re.search(r"\bum\s*$", before):
        return None
    # ignore leading negative values like "-0,8 l/s"
    lead = text[max(0, m.start() - 2):m.start()]
    if "-" in lead:
        return None
    return v


def _extract_flow_from_ablaufleistung(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]], bool]:
    vals: List[float] = []
    first_span: Optional[Tuple[int, int]] = None
    has_abl_snippet = False

    # snippets that contain BOTH "Ablaufleistung" and "l/s"
    for km in re.finditer(r"ablaufleistung", flat, re.IGNORECASE):
        lo = max(0, km.start() - 40)
        hi = min(len(flat), km.end() + 100)
        part = flat[lo:hi]
        if "l/s" not in part.lower():
            continue
        has_abl_snippet = True
        for m in FLOW_LPS_RE.finditer(part):
            prev_kw = part.lower().rfind("ablaufleistung", 0, m.start() + 1)
            if prev_kw < 0 or (m.start() - prev_kw) > 40:
                continue
            v = _flow_value_if_valid(part, m)
            if v is None:
                continue
            vals.append(v)
            if first_span is None:
                first_span = (lo + m.start(), lo + m.end())

    return sorted(set(vals)), first_span, has_abl_snippet


def _apply_text_extraction(res: Dict[str, Any], flat: str, src: str, html: str = "", flow_evidence_pdf: bool = False, dn_evidence_pdf: bool = False, parse_flow: bool = True) -> None:
    if not flat:
        return

    # DN: table first
    dns: List[str] = []
    dn_span: Optional[Tuple[int, int]] = None
    if html:
        dns_tab, tab_txt = _extract_dns_from_table(html)
        if dns_tab:
            dns = dns_tab
            # find table snippet occurrence in flat for evidence
            if tab_txt:
                idx = flat.lower().find(tab_txt.lower())
                if idx >= 0:
                    dn_span = (idx, min(len(flat), idx + len(tab_txt)))

    if not dns:
        dns, dn_span = _extract_dns_from_text(flat)

    if dns and res.get("outlet_dn") is None:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        if dn_span:
            dn_label = "Outlet DN (from PDF)" if dn_evidence_pdf else "Outlet DN"
            res["evidence"].append((dn_label, _snippet(flat, dn_span[0], dn_span[1]), src))

    # flow: strict Ablaufleistung-only parsing
    if parse_flow:
        flow_opts_abl, flow_span_abl, has_abl_snippet = _extract_flow_from_ablaufleistung(flat)
        if has_abl_snippet and flow_opts_abl:
            res["flow_rate_lps_options"] = json.dumps(flow_opts_abl, ensure_ascii=False)
            res["flow_rate_lps"] = max(flow_opts_abl)
            res["flow_rate_unit"] = "l/s"
            res["flow_rate_status"] = "ok"
            if flow_span_abl:
                label = "Flow rate (Ablaufleistung from PDF)" if flow_evidence_pdf else "Flow rate (Ablaufleistung)"
                res["evidence"].append((label, _snippet(flat, flow_span_abl[0], flow_span_abl[1]), src))

    # heights (never trap seal)
    h = HEIGHT_RE.search(flat)
    if h and not TRAP_SEAL_RE.search(flat[h.start():h.end()]):
        a = int(h.group(1))
        b = int(h.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Installation height (mm)", _snippet(flat, h.start(), h.end()), src))
    elif res.get("height_adj_min_mm") is None:
        hs = HEIGHT_SINGLE_RE.search(flat)
        if hs and not TRAP_SEAL_RE.search(flat[hs.start():hs.end()]):
            v = int(hs.group(1))
            if 20 <= v <= 300:
                res["height_adj_min_mm"] = v
                res["height_adj_max_mm"] = v
                res["evidence"].append(("Installation height (mm)", _snippet(flat, hs.start(), hs.end()), src))


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    _ = int(tolerance_mm)

    out: List[Dict[str, Any]] = []
    discovered_rows: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    discovered_meta: Dict[str, Dict[str, Any]] = {
        _normalize_discovered_url(u): {
            "raw_discovered_href": u,
            "normalized_detail_url": _normalize_discovered_url(u),
            "href_source_page": "detail_seed",
            "was_synthetic_url": False,
        }
        for u in DETAIL_SEEDS
    }
    accepted_urls: List[str] = []
    component_urls: List[str] = []
    product_urls: List[str] = []
    unknown_length_count = 0
    min_len = max(0, want - int(tolerance_mm))
    max_len = want + int(tolerance_mm)
    sample_candidates_by_family: Dict[str, List[str]] = {}
    counts_by_family: Dict[str, int] = {}
    counts_by_category: Dict[str, int] = {}
    counts_by_role: Dict[str, int] = {}
    dead_seed_urls: List[str] = []
    rejected_spare_parts: List[str] = []
    rejected_unrelated: List[str] = []
    rejected_overfiltered: List[str] = []
    sample_relevant_kept: List[str] = []
    sample_relevant_rejected: List[str] = []

    # Step 1: multiple seeds -> category links
    category_links: Set[str] = set(CATEGORY_SEEDS)
    for seed in CATALOG_SEEDS:
        st, final, html, err = _safe_get_text(seed, timeout=35)
        found = 0
        fam = _infer_family(seed, "")
        if st == 200 and html:
            links = _extract_category_links_from_sortiment(html, final)
            found = len(links)
            category_links.update(links)
        else:
            dead_seed_urls.append(seed)
        debug.append({"site": "viega", "seed_url": seed, "status_code": st, "final_url": final, "error": err, "candidates_found": found, "method": "seed_scope", "is_index": None, "discovery_seed_family": fam})

    # Step 2: category crawl -> detail links
    detail_links = _crawl_category_pages(category_links, max_pages=2000)
    for du, meta in detail_links.items():
        discovered_meta[du] = meta
    debug.append({"site": "viega", "seed_url": CATALOG_SEEDS[0], "status_code": 200 if detail_links else None, "final_url": CATALOG_SEEDS[0], "error": "" if detail_links else "No detail links from categories", "candidates_found": len(detail_links), "method": "category_crawl", "is_index": None})

    for url in sorted(discovered_meta):
        link_meta = discovered_meta.get(url, {})
        st, final, html, err = _safe_get_text(url, timeout=35)
        title = url.rstrip("/").split("/")[-1].replace("-", " ")
        length = None
        length_kind = None
        length_snip = None

        flat = ""
        if st == 200 and html:
            title = _extract_title(html, final)
            flat = _main_flat_text(html)
            length, length_snip, length_kind = _resolve_length_from_text(f"{title} {flat}")
            breadcrumb = _extract_breadcrumb_text(html)
            article_rows = _parse_article_table(html)
            article_text = " ".join(str(r.get("_row_text") or "") for r in article_rows[:30])
        else:
            breadcrumb = ""
            article_text = ""

        cand_type, drain_category, system_role, complete_system = _derive_taxonomy(url, title, flat, breadcrumb=breadcrumb, article_text=article_text)
        fam = _infer_family(url, title)

        # safeguard: /Zubehoer/ should never become products
        if ("/zubehoer/" in url.lower() or "/zubehör/" in url.lower()) and cand_type == "drain":
            cand_type = "component"

        # Apply length filter only for concrete fixed lengths (not unknown/variable)
        if length is not None and length_kind != "variable" and not (min_len <= length <= max_len):
            debug.append({"site": "viega", "seed_url": url, "status_code": st, "final_url": final, "error": "filtered_by_target_length", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        # For components (rost), append length range if present
        if cand_type == "component" and st == 200 and html:
            mrg = LENGTH_RANGE_RE.search(_main_flat_text(html))
            if mrg:
                title = f"{title} ({mrg.group(1)}–{mrg.group(2)} mm)"

        candidate_row = {
            "manufacturer": "viega",
            "product_id": _product_id_from_url(url),
            "product_family": "Advantix",
            "product_name": title if length is None else f"{title} ({length} mm)",
            "product_url": url,
            "sources": url,
            "candidate_type": cand_type,
            "complete_system": complete_system,
            "drain_category": drain_category,
            "system_role": system_role,
            "discovery_seed_family": fam,
            "selected_length_mm": want,
            "length_mode": "unknown" if length is None else ("variable" if length_kind == "variable" else "html"),
            "length_delta_mm": None if length is None else (length - want),
            "discovery_evidence": "Length (variable range)" if length_kind == "variable" else None,
            "raw_discovered_href": link_meta.get("raw_discovered_href"),
            "normalized_detail_url": link_meta.get("normalized_detail_url", url),
            "href_source_page": link_meta.get("href_source_page"),
            "was_synthetic_url": bool(link_meta.get("was_synthetic_url")),
        }
        discovered_rows.append(candidate_row)

        unrelated = _is_unrelated_branch(url, title)
        spare_like = _is_spare_part_like(url, title, flat, system_role)
        accepted_roles = {"complete_drain", "base_set", "cover", "profile"}
        in_target_families = _belongs_to_target_families(url, title, breadcrumb, drain_category)
        meaningful_entity = _is_meaningful_system_entity(url, title, flat, system_role)
        role_ok = system_role in accepted_roles
        is_accepted = role_ok and meaningful_entity and in_target_families and (not unrelated) and (not spare_like)

        if is_accepted:
            out.append(candidate_row)
            accepted_urls.append(url)
            if len(sample_relevant_kept) < 20:
                sample_relevant_kept.append(url)
            counts_by_family[fam] = counts_by_family.get(fam, 0) + 1
            counts_by_category[drain_category] = counts_by_category.get(drain_category, 0) + 1
            counts_by_role[system_role] = counts_by_role.get(system_role, 0) + 1
            sample_candidates_by_family.setdefault(fam, [])
            if len(sample_candidates_by_family[fam]) < 5:
                sample_candidates_by_family[fam].append(url)
            if cand_type == "component":
                component_urls.append(url)
            else:
                product_urls.append(url)
            if length is None:
                unknown_length_count += 1
        else:
            if unrelated:
                rejected_unrelated.append(url)
            elif spare_like:
                rejected_spare_parts.append(url)
            else:
                rejected_overfiltered.append(url)
                if (role_ok or meaningful_entity) and len(sample_relevant_rejected) < 20:
                    sample_relevant_rejected.append(url)

        debug.append({
            "site": "viega",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err if st != 200 else ("length_variable" if length_kind == "variable" else ("length_unknown" if length is None else "")),
            "candidates_found": 1,
            "method": "detail",
            "is_index": None,
            "raw_discovered_href": link_meta.get("raw_discovered_href"),
            "normalized_detail_url": link_meta.get("normalized_detail_url", url),
            "href_source_page": link_meta.get("href_source_page"),
            "was_synthetic_url": bool(link_meta.get("was_synthetic_url")),
        })

    # keep unique product_id to avoid duplicate IDs in exported Products/Components
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in out:
        pid = str(r.get("product_id") or "")
        if not pid:
            continue
        prev = dedup.get(pid)
        if prev is None:
            dedup[pid] = r
            continue
        prev_syn = bool(prev.get("was_synthetic_url"))
        cur_syn = bool(r.get("was_synthetic_url"))
        prev_source = str(prev.get("href_source_page") or "")
        cur_source = str(r.get("href_source_page") or "")
        if prev_syn and not cur_syn:
            dedup[pid] = r
            continue
        if prev_source == "detail_seed" and cur_source != "detail_seed":
            dedup[pid] = r
            continue
        if len(urlparse(str(r.get("product_url") or "")).path) > len(urlparse(str(prev.get("product_url") or "")).path):
            dedup[pid] = r

    debug.append({
        "site": "viega",
        "seed_url": CATALOG_SEEDS[0],
        "status_code": 200 if dedup else None,
        "final_url": CATALOG_SEEDS[0],
        "error": "" if dedup else "No accepted candidates.",
        "candidates_found": len(dedup),
        "method": "summary",
        "is_index": None,
        "final_count": len(dedup),
        "total_details": len(discovered_meta),
        "products_count": sum(1 for r in dedup.values() if str(r.get("candidate_type",""))=="drain"),
        "components_count": sum(1 for r in dedup.values() if str(r.get("candidate_type",""))=="component"),
        "unknown_length_count": sum(1 for r in dedup.values() if str(r.get("length_mode",""))=="unknown"),
        "variable_length_count": sum(1 for r in dedup.values() if str(r.get("length_mode",""))=="variable"),
        "sample_accepted_urls": json.dumps(accepted_urls[:10], ensure_ascii=False),
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_components_urls": json.dumps(component_urls[:10], ensure_ascii=False),
        "counts_by_drain_category": json.dumps(counts_by_category, ensure_ascii=False),
        "counts_by_system_role": json.dumps(counts_by_role, ensure_ascii=False),
        "counts_by_family": json.dumps(counts_by_family, ensure_ascii=False),
        "sample_candidates_by_family": json.dumps(sample_candidates_by_family, ensure_ascii=False),
        "canonical_seed_urls": json.dumps(CATALOG_SEEDS, ensure_ascii=False),
        "discovered_category_links": json.dumps(sorted(category_links)[:40], ensure_ascii=False),
        "discovered_detail_links": json.dumps(sorted(detail_links)[:40], ensure_ascii=False),
        "dead_seed_urls": json.dumps(dead_seed_urls, ensure_ascii=False),
        "accepted_product_links": json.dumps(product_urls[:40], ensure_ascii=False),
        "discovered_catalog_candidates_count": len(discovered_rows),
        "accepted_benchmark_candidates_count": len(dedup),
        "accepted_candidates_count": len(dedup),
        "rejected_spare_parts_count": len(rejected_spare_parts),
        "rejected_unrelated_branch_count": len(rejected_unrelated),
        "rejected_overfiltered_count": len(rejected_overfiltered),
        "sample_relevant_kept": json.dumps(sample_relevant_kept[:20], ensure_ascii=False),
        "sample_relevant_rejected": json.dumps(sample_relevant_rejected[:20], ensure_ascii=False),
        "sample_rejected_spare_parts": json.dumps(rejected_spare_parts[:20], ensure_ascii=False),
        "sample_rejected_unrelated": json.dumps(rejected_unrelated[:20], ensure_ascii=False),
    })
    return list(dedup.values()), debug


def extract_parameters(product_url: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_lps_10mm": None,
        "flow_rate_lps_20mm": None,
        "flow_rate_raw_text": None,
        "flow_rate_unit": None,
        "flow_rate_status": None,
        "flow_rate_lps_options": None,
        "material_detail": None,
        "material_v4a": None,
        "din_en_1253_cert": None,
        "din_18534_compliance": None,
        "height_adj_min_mm": None,
        "height_adj_max_mm": None,
        "outlet_dn": None,
        "outlet_dn_default": None,
        "outlet_dn_options_json": None,
        "sealing_fleece_preassembled": None,
        "colours_count": None,
        "resolved_length_mm": None,
        "pdf_url": None,
        "article_rows_json": None,
        "outlet_direction_hint": None,
        "outlet_selectable_hint": None,
        "evidence": [],
    }

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    sections = _parse_sections(html)
    flat = sections.get("main_flat") or _main_flat_text(html)

    pdfs = _extract_pdf_candidates(html, final)
    if pdfs:
        res["pdf_url"] = pdfs[0][0]
        res["evidence"].append(("PDF", pdfs[0][0], final))

    article_rows = _parse_article_table(html)
    if article_rows:
        res["article_rows_json"] = json.dumps(article_rows[:80], ensure_ascii=False)
        selected = _select_article_row(article_rows, target_mm=1200)
        if selected:
            res["evidence"].append(("Selected article row", str(selected.get("_row_text") or selected), final))
            if isinstance(selected.get("length_mm"), int):
                res["resolved_length_mm"] = int(selected["length_mm"])
            dns = selected.get("dns") or []
            if dns:
                res["outlet_dn"] = "/".join(dns)
                res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
                res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
            if isinstance(selected.get("hmin"), int) and isinstance(selected.get("hmax"), int):
                res["height_adj_min_mm"] = int(selected["hmin"])
                res["height_adj_max_mm"] = int(selected["hmax"])
            fopts = selected.get("flow_opts") or []
            if fopts:
                res["flow_rate_lps"] = max(fopts)
                res["flow_rate_unit"] = "l/s"
                res["flow_rate_status"] = "ok"
                res["flow_rate_raw_text"] = ", ".join(str(v).replace(".", ",") for v in fopts)
                res["flow_rate_lps_options"] = json.dumps(fopts, ensure_ascii=False)
        variants = sorted({str(r.get("variant_label") or "").strip() for r in article_rows if str(r.get("variant_label") or "").strip()})
        if variants:
            res["colours_count"] = len(variants)

    _flow_opts_html, _flow_span_html, has_abl_html = _extract_flow_from_ablaufleistung(flat)
    _apply_text_extraction(res, flat, final, html=html, parse_flow=(res.get("flow_rate_lps") is None))

    f10, f20 = _extract_flow_10_20(flat)
    if f10 is not None:
        res["flow_rate_lps_10mm"] = f10
        res["evidence"].append(("Flow 10 mm", str(f10), final))
    if f20 is not None:
        res["flow_rate_lps_20mm"] = f20
        res["evidence"].append(("Flow 20 mm", str(f20), final))
    if res.get("flow_rate_lps") is None and (f20 is not None or f10 is not None):
        vals = [v for v in [f10, f20] if v is not None]
        res["flow_rate_lps"] = f20 if f20 is not None else f10
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
        res["flow_rate_lps_options"] = json.dumps(sorted(set(vals)), ensure_ascii=False)
        res["flow_rate_raw_text"] = ", ".join(str(v).replace(".", ",") for v in sorted(set(vals)))

    length, len_snip, kind = _resolve_length_from_text(flat)
    if length is not None:
        res["resolved_length_mm"] = length
        res["evidence"].append(("Resolved length (mm)", len_snip or f"{length} mm", final))
    elif kind == "variable":
        res["evidence"].append(("Resolved length", "variable length", final))

    # accessory rost range evidence
    if _is_rost_component(src):
        mrg = LENGTH_RANGE_RE.search(flat)
        if mrg:
            res["evidence"].append(("Accessory length range (mm)", _snippet(flat, mrg.start(), mrg.end()), final))

    m_en = re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE)
    if m_en:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, m_en.start(), m_en.end()), final))

    mat_detail, mat_v4a = _extract_material_viega(flat)
    if mat_detail:
        res["material_detail"] = mat_detail
        res["material_v4a"] = mat_v4a
        res["evidence"].append(("Material", mat_detail, final))

    if re.search(r"abdichtungsmanschette[^\.\n]{0,80}(werkseitig|vormontiert)", flat, re.IGNORECASE):
        res["sealing_fleece_preassembled"] = "yes"
        res["evidence"].append(("Sealing interface", "Abdichtungsmanschette werkseitig vormontiert", final))

    if re.search(r"\bsenkrecht\b", flat, re.IGNORECASE):
        res["outlet_direction_hint"] = "vertical"
        res["evidence"].append(("Outlet clue", "senkrecht", final))
    if re.search(r"\bwaagerecht\b", flat, re.IGNORECASE):
        res["outlet_direction_hint"] = res.get("outlet_direction_hint") or "horizontal"
        res["evidence"].append(("Outlet clue", "waagerecht", final))
    if re.search(r"\bdrehbar\b|dn\s*40[^\.\n]{0,30}dn\s*50|dn40[^\.\n]{0,30}dn50", flat, re.IGNORECASE):
        res["outlet_selectable_hint"] = "yes"
        res["evidence"].append(("Outlet clue", "drehbar / DN40->DN50", final))

    need_pdf = any(res.get(k) is None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "resolved_length_mm"])
    if need_pdf:
        for pdf_url, _score in _extract_pdf_candidates(html, final)[:4]:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            if not pdf_text:
                continue
            flat_pdf = _clean_text(pdf_text)
            _apply_text_extraction(res, flat_pdf, pdf_url, flow_evidence_pdf=True, dn_evidence_pdf=True, parse_flow=(not has_abl_html))

            if res.get("resolved_length_mm") is None:
                plen, psnip, pkind = _resolve_length_from_text(flat_pdf)
                if plen is not None:
                    res["resolved_length_mm"] = plen
                    res["evidence"].append(("Resolved length (mm)", psnip or f"{plen} mm", pdf_url))
                elif pkind == "variable":
                    res["evidence"].append(("Resolved length", "variable length", pdf_url))

            if res.get("din_en_1253_cert") is None and re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat_pdf, re.IGNORECASE):
                res["din_en_1253_cert"] = "yes"

            done = all(res.get(k) is not None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "resolved_length_mm"])
            if done:
                break

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
