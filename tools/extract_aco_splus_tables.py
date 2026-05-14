from __future__ import annotations

import csv
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

URLS = {
    "splus_family": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/",
    "splus_drain_body": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/",
    "splus_profile": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/",
    "downloads": "https://www.aco-haustechnik.de/downloads/",
    "pdf_splus": "https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf",
    "pdf_line": "https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Badentwaesserung_Linie.pdf",
}

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
LEN_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
FLOW_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
WS_RE = re.compile(r"(?:sperrwasserh(?:ö|oe)he|geruchverschluss)[^\d]{0,20}(\d{2,3})\s*mm", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(\d{2})\b", re.IGNORECASE)
HEIGHT_RE = re.compile(r"(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm")
COMPAT_RE = re.compile(r"kompatibel|geeignet\s+f[üu]r|passend\s+zu|zu\s+aco\s+duschrinnenprofil\s+showerdrain\s*s\+", re.IGNORECASE)


@dataclass
class Row:
    source: str
    source_type: str
    page_ref: str
    source_refs: str = ""
    component_role: str = "unknown"
    article_no: str = ""
    product_name: str = ""
    length_mm: str = ""
    water_seal_mm: str = ""
    flow_rate_lps: str = ""
    flow_rate_10mm_lps: str = ""
    flow_rate_20mm_lps: str = ""
    outlet_dn: str = ""
    outlet_orientation: str = ""
    height_range_mm: str = ""
    compatibility_excerpt: str = ""
    flow_mapping_status: str = "unknown"
    evidence_quality: str = "diagnostic_only"
    assembly_ready: str = "source_only_family_level"
    notes: str = ""


def fetch(url: str) -> str:
    r = requests.get(url, timeout=40)
    r.raise_for_status()
    return r.text


def _classify_role(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ("ablaufk", "einzelablauf", "drain body")):
        return "drain_body"
    if any(k in t for k in ("rinnenprofil", "duschrinnenprofil", "profil", "rinnenk", "channel")):
        return "profile_channel"
    if any(k in t for k in ("rost", "abdeckung", "grate", "cover")):
        return "grate_cover"
    if any(k in t for k in ("zubeh", "accessory", "showerstep", "keil")):
        return "accessory"
    return "unknown"


def _role_from_context(source: str, text: str, current: str) -> str:
    s = (source or "").lower()
    x = (text or "").lower()
    if "ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus" in s:
        return "drain_body"
    if "aco-showerdrain-splus-duschrinnenprofil" in s:
        return "profile_channel"
    if re.search(r"9010\.51\.(20|21)", x):
        return "drain_body"
    if re.search(r"9010\.51\.(0[1-4]|4[1-4])", x):
        return "profile_channel"
    return current


def _determine_assembly_ready(row: Row) -> str:
    if not row.article_no:
        return "no_drain_article" if row.component_role == "drain_body" else "no_profile_article"
    if row.flow_mapping_status == "ambiguous":
        return "ambiguous_flow_mapping"
    if not row.compatibility_excerpt:
        return "no_article_to_article_compatibility"
    return "yes"


def _extract_lead_patterns(text: str, source: str, page_ref: str) -> List[Row]:
    rows: List[Row] = []
    txt = " ".join((text or "").split())
    for am in ARTICLE_RE.finditer(txt):
        art = am.group(0)
        lo = max(0, am.start() - 160)
        hi = min(len(txt), am.end() + 200)
        ctx = txt[lo:hi]
        role = _role_from_context(source, ctx, _classify_role(ctx))
        if role not in {"profile_channel", "drain_body"}:
            continue
        lm = LEN_RE.search(ctx)
        dn = next((f"DN{m.group(1)}" for m in DN_RE.finditer(ctx)), "")
        ws = next((m.group(1) for m in WS_RE.finditer(ctx)), "")
        h = next((f"{m.group(1)}-{m.group(2)}" for m in HEIGHT_RE.finditer(ctx)), "")
        flow = ""
        flow10 = ""
        flow20 = ""
        status = "unknown"
        m10 = re.search(r"10\s*mm[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", ctx, re.IGNORECASE)
        m20 = re.search(r"20\s*mm[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", ctx, re.IGNORECASE)
        if m10 or m20:
            status = "confirmed"
            if m10:
                flow10 = m10.group(1).replace(",", ".")
            if m20:
                flow20 = m20.group(1).replace(",", ".")
            vals = [v for v in (flow10, flow20) if v]
            if vals:
                flow = max(vals, key=lambda x: float(x))

        row = Row(
            source=source,
            source_type=("pdf" if source.lower().endswith(".pdf") else "html_page"),
            page_ref=page_ref,
            source_refs=page_ref,
            component_role=role,
            article_no=art,
            product_name="ACO ShowerDrain S+" if role == "profile_channel" else "ACO ShowerDrain S+ Ablaufkörper",
            length_mm=(lm.group(1) if lm else ""),
            water_seal_mm=ws,
            flow_rate_lps=flow,
            flow_rate_10mm_lps=flow10,
            flow_rate_20mm_lps=flow20,
            outlet_dn=dn,
            outlet_orientation=("horizontal" if re.search(r"waagerecht|horizontal", ctx, re.IGNORECASE) else ""),
            height_range_mm=h,
            compatibility_excerpt=("implicit_family_level" if COMPAT_RE.search(ctx) else ""),
            flow_mapping_status=status,
            evidence_quality=("medium_confidence_pdf_context" if source.lower().endswith(".pdf") else "diagnostic_only"),
            notes=f"lead-pattern extraction context: {ctx[:180]}",
        )
        row.assembly_ready = _determine_assembly_ready(row)
        rows.append(row)
    return rows


def _parse_flow_by_headers(headers: List[str], cells: List[str]) -> Tuple[str, str, str, str]:
    idx10 = idx20 = None
    for i, h in enumerate(headers):
        hh = h.lower()
        if "10" in hh and "mm" in hh and ("abfluss" in hh or "ablauf" in hh):
            idx10 = i
        if "20" in hh and "mm" in hh and ("abfluss" in hh or "ablauf" in hh):
            idx20 = i

    flow = ""
    flow10 = ""
    flow20 = ""
    status = "unknown"

    if idx10 is not None and idx10 < len(cells):
        m10 = FLOW_RE.search(cells[idx10])
        if m10:
            flow10 = m10.group(1).replace(",", ".")
    if idx20 is not None and idx20 < len(cells):
        m20 = FLOW_RE.search(cells[idx20])
        if m20:
            flow20 = m20.group(1).replace(",", ".")

    if flow10 or flow20:
        status = "confirmed"
        vals = [v for v in (flow10, flow20) if v]
        flow = max(vals, key=lambda x: float(x)) if vals else ""
        return flow, flow10, flow20, status

    # fallback generic flow (ambiguous mapping)
    joined = " ".join(cells)
    mg = FLOW_RE.search(joined)
    if mg:
        flow = mg.group(1).replace(",", ".")
        status = "ambiguous"
    return flow, flow10, flow20, status


def parse_html(url: str, html: str) -> List[Row]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Row] = []

    title = (soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
    title_role = _classify_role(title + " " + url)
    flat = " ".join(soup.get_text(" ", strip=True).split())
    dn_page = next((f"DN{m.group(1)}" for m in DN_RE.finditer(flat)), "")
    ws_page = next((m.group(1) for m in WS_RE.finditer(flat)), "")
    h_page = next((f"{m.group(1)}-{m.group(2)}" for m in HEIGHT_RE.finditer(flat)), "")
    compat = next((m.group(0) for m in COMPAT_RE.finditer(flat)), "")

    table_count = 0
    for t in soup.select("table"):
        table_count += 1
        trs = t.select("tr")
        if not trs:
            continue
        header_cells = [" ".join(c.get_text(" ", strip=True).split()) for c in trs[0].select("th,td")]
        for tr in trs[1:]:
            cell_texts = [" ".join(c.get_text(" ", strip=True).split()) for c in tr.select("th,td")]
            txt = " ".join(cell_texts)
            if not txt:
                continue
            am = ARTICLE_RE.search(txt)
            lm = LEN_RE.search(txt)
            flow, flow10, flow20, flow_status = _parse_flow_by_headers(header_cells, cell_texts)
            role = _role_from_context(url, txt, _classify_role(title + " " + txt + " " + url))
            dn_row = next((f"DN{m.group(1)}" for m in DN_RE.finditer(txt)), "") or dn_page
            ws_row = next((m.group(1) for m in WS_RE.finditer(txt)), "") or ws_page
            h_row = next((f"{m.group(1)}-{m.group(2)}" for m in HEIGHT_RE.finditer(txt)), "") or h_page
            row = Row(
                source=url,
                source_type="html_table",
                page_ref=f"table_{table_count}",
                source_refs=f"table_{table_count}",
                component_role=role,
                article_no=(am.group(0) if am else ""),
                product_name=title,
                length_mm=(lm.group(1) if lm else ""),
                water_seal_mm=ws_row,
                flow_rate_lps=flow,
                flow_rate_10mm_lps=flow10,
                flow_rate_20mm_lps=flow20,
                outlet_dn=dn_row,
                outlet_orientation=("horizontal" if "waagerecht" in flat.lower() else ""),
                height_range_mm=h_row,
                compatibility_excerpt=compat,
                flow_mapping_status=flow_status,
                evidence_quality="high_confidence_html_table",
                notes=("flow mapping from headers" if flow_status == "confirmed" else ""),
            )
            row.assembly_ready = _determine_assembly_ready(row)
            if row.article_no or row.length_mm:
                rows.append(row)

    rows.extend(_extract_lead_patterns(flat, url, "main_lead_scan"))

    if not rows:
        row = Row(
            source=url,
            source_type="html_page",
            page_ref="main",
            source_refs="main",
            component_role=title_role,
            product_name=title,
            water_seal_mm=ws_page,
            outlet_dn=dn_page,
            outlet_orientation=("horizontal" if "waagerecht" in flat.lower() else ""),
            height_range_mm=h_page,
            compatibility_excerpt=compat,
            evidence_quality="diagnostic_only",
            notes="no article table rows parsed",
        )
        row.assembly_ready = _determine_assembly_ready(row)
        rows.append(row)
    return rows


def parse_pdf(url: str, blob: bytes) -> List[Row]:
    rows: List[Row] = []
    extracted_any = False
    try:
        from pypdf import PdfReader  # type: ignore
        import io

        reader = PdfReader(io.BytesIO(blob))
        for i, page in enumerate(reader.pages, start=1):
            txt = " ".join((page.extract_text() or "").split())
            if not txt:
                continue
            page_ref = f"pdf_page_{i}"
            role = _classify_role(txt)
            compat = next((m.group(0) for m in COMPAT_RE.finditer(txt)), "")
            dn = next((f"DN{m.group(1)}" for m in DN_RE.finditer(txt)), "")
            ws = next((m.group(1) for m in WS_RE.finditer(txt)), "")
            h = next((f"{m.group(1)}-{m.group(2)}" for m in HEIGHT_RE.finditer(txt)), "")

            # extract article numbers with local context windows
            for am in ARTICLE_RE.finditer(txt):
                art = am.group(0)
                lo = max(0, am.start() - 120)
                hi = min(len(txt), am.end() + 140)
                ctx = txt[lo:hi]
                ctx_role = _classify_role(ctx)
                if ctx_role == "unknown" and re.search(r"9010\.51\.(0[1-4]|4[1-4])", art):
                    ctx_role = "profile_channel"
                if ctx_role == "unknown" and role != "unknown":
                    ctx_role = role
                if ctx_role != "profile_channel" and not re.search(r"profil|rinnenprofil|duschrinnenprofil|profilk[oö]rper|channel", ctx, re.IGNORECASE):
                    continue
                lm = LEN_RE.search(ctx)
                row = Row(
                    source=url,
                    source_type="pdf",
                    page_ref=page_ref,
                    source_refs=page_ref,
                    component_role="profile_channel" if ctx_role == "profile_channel" else ctx_role,
                    article_no=art,
                    product_name="ACO ShowerDrain S+ PDF extract",
                    length_mm=(lm.group(1) if lm else ""),
                    water_seal_mm=ws,
                    outlet_dn=dn,
                    height_range_mm=h,
                    compatibility_excerpt=compat,
                    flow_mapping_status="unknown",
                    evidence_quality="medium_confidence_pdf_context",
                    notes=f"pdf context: {ctx[:180]}",
                )
                row.assembly_ready = _determine_assembly_ready(row)
                rows.append(row)
                extracted_any = True

        if not extracted_any:
            rows.append(Row(
                source=url,
                source_type="pdf",
                page_ref="n/a",
                source_refs="n/a",
                notes="PDF parsed but no profile/channel article-number context found.",
                assembly_ready="source_only_family_level",
            ))
    except Exception as e:
        rows.append(Row(
            source=url,
            source_type="pdf",
            page_ref="n/a",
            source_refs="n/a",
            notes=f"PDF extraction unavailable: {type(e).__name__}: {e}",
            assembly_ready="source_only_family_level",
        ))
    return rows


def deduplicate(rows: List[Row]) -> List[Row]:
    key_map: Dict[Tuple[str, str, str, str, str, str, str], Row] = {}
    refs: Dict[Tuple[str, str, str, str, str, str, str], List[str]] = {}
    for r in rows:
        key = (
            r.source,
            r.article_no,
            r.product_name,
            r.water_seal_mm,
            r.outlet_dn,
            r.outlet_orientation,
            r.height_range_mm,
        )
        if key not in key_map:
            key_map[key] = r
            refs[key] = [r.page_ref]
        else:
            refs[key].append(r.page_ref)
            # merge best flow certainty
            if key_map[key].flow_mapping_status != "confirmed" and r.flow_mapping_status == "confirmed":
                key_map[key].flow_mapping_status = "confirmed"
                key_map[key].flow_rate_10mm_lps = r.flow_rate_10mm_lps or key_map[key].flow_rate_10mm_lps
                key_map[key].flow_rate_20mm_lps = r.flow_rate_20mm_lps or key_map[key].flow_rate_20mm_lps
                key_map[key].flow_rate_lps = r.flow_rate_lps or key_map[key].flow_rate_lps
    out = []
    for k, r in key_map.items():
        merged = sorted(set(refs[k]))
        if len(merged) > 1:
            suffix = f" merged_refs={','.join(merged)}"
            r.notes = (r.notes + suffix).strip()
        r.source_refs = ",".join(merged)
        r.assembly_ready = _determine_assembly_ready(r)
        out.append(r)
    return out


def write_outputs(rows: List[Row]) -> None:
    out_csv = Path("data/diagnostics/aco_splus_extracted_tables.csv")
    out_md = Path("data/diagnostics/aco_splus_extracted_tables.md")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    def _norm_article(a: str) -> str:
        m = ARTICLE_RE.search(str(a or ""))
        return m.group(0) if m else ""

    core_rows = [r for r in rows if r.source_type not in {"html_error", "pdf_error"}]
    confirmed = [r for r in core_rows if r.evidence_quality == "high_confidence_html_table"]
    unique_articles = sorted({x for x in (_norm_article(r.article_no) for r in core_rows) if x})
    profile_articles = sorted({x for x in (_norm_article(r.article_no) for r in confirmed if r.component_role == "profile_channel") if x})
    drain_articles = sorted({x for x in (_norm_article(r.article_no) for r in confirmed if r.component_role == "drain_body") if x})
    ambiguous_articles = sorted({x for x in (_norm_article(r.article_no) for r in core_rows if r.evidence_quality != "high_confidence_html_table") if x})
    flow_confirmed = any(r.flow_mapping_status == "confirmed" for r in confirmed if r.article_no)
    flow_ambiguous = any(r.flow_mapping_status == "ambiguous" for r in core_rows if r.article_no)
    compatibility_classification = "implicit_family_level" if (profile_articles and drain_articles) else "not_found"

    with out_md.open("w", encoding="utf-8") as f:
        f.write("# ACO S+ Extracted Tables (Diagnostic)\n\n")
        f.write("## Sources scanned\n")
        for k, v in URLS.items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Extraction summary\n")
        f.write(f"- Total deduplicated rows: {len(rows)}\n")
        f.write(f"- Unique article numbers: {', '.join(unique_articles) if unique_articles else '(none)'}\n")
        f.write(f"- confirmed_profile_articles: {', '.join(profile_articles) if profile_articles else 'no'}\n")
        f.write(f"- confirmed_drain_body_articles: {', '.join(drain_articles) if drain_articles else 'no'}\n")
        f.write(f"- ignored_or_ambiguous_articles: {', '.join(ambiguous_articles) if ambiguous_articles else '(none)'}\n")
        f.write(f"- compatibility_classification: {compatibility_classification}\n")
        f.write(f"- Flow mapping confirmed from headers: {'yes' if flow_confirmed else 'no'}\n")
        f.write(f"- Flow mapping ambiguous present: {'yes' if flow_ambiguous else 'no'}\n")
        f.write("\n## Conclusion\n")
        if compatibility_classification == "implicit_family_level" and profile_articles and drain_articles:
            f.write("partial\n")
            f.write("\n## Next recommendation\nProfile and drain-body articles are confirmed, but compatibility is implicit_family_level; do not create assembled products yet.\n")
        else:
            f.write("no safe assembled S+ implementation yet\n")
            f.write("\n## Next recommendation\nContinue official PDF/table extraction to find profile/channel article numbers and explicit article-to-article compatibility.\n")


def main() -> None:
    rows: List[Row] = []
    for key in ("splus_family", "splus_profile", "splus_drain_body", "downloads"):
        url = URLS[key]
        try:
            html = fetch(url)
            rows.extend(parse_html(url, html))
        except Exception as e:
            rows.append(Row(source=url, source_type="html_error", page_ref="n/a", source_refs="n/a", notes=f"fetch failed: {type(e).__name__}: {e}"))

    for key in ("pdf_splus", "pdf_line"):
        url = URLS[key]
        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            rows.extend(parse_pdf(url, r.content))
        except Exception as e:
            rows.append(Row(source=url, source_type="pdf_error", page_ref="n/a", source_refs="n/a", notes=f"fetch failed: {type(e).__name__}: {e}"))

    if not rows:
        rows = [Row(source="n/a", source_type="none", page_ref="n/a", source_refs="n/a", notes="no rows")]
    write_outputs(deduplicate(rows))


if __name__ == "__main__":
    main()
