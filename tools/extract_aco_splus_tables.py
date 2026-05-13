from __future__ import annotations

import csv
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup

URLS = {
    "splus_family": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/",
    "splus_drain_body": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/",
    "downloads": "https://www.aco-haustechnik.de/downloads/",
    "pdf_splus": "https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf",
    "pdf_line": "https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Badentwaesserung_Linie.pdf",
}

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
LEN_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
FLOW_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
FLOW10_RE = re.compile(r"10\s*mm[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
FLOW20_RE = re.compile(r"20\s*mm[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
WS_RE = re.compile(r"(?:sperrwasserh(?:ö|oe)he|geruchverschluss)[^\d]{0,20}(\d{2,3})\s*mm", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(\d{2})\b", re.IGNORECASE)
HEIGHT_RE = re.compile(r"(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm")
COMPAT_RE = re.compile(r"kompatibel|geeignet\s+f[üu]r|passend\s+zu|zu\s+aco\s+duschrinnenprofil\s+showerdrain\s*s\+", re.IGNORECASE)


@dataclass
class Row:
    source: str
    source_type: str
    page_ref: str
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
    notes: str = ""


def fetch(url: str) -> str:
    r = requests.get(url, timeout=40)
    r.raise_for_status()
    return r.text


def parse_html(url: str, html: str) -> List[Row]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Row] = []

    title = (soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
    flat = " ".join(soup.get_text(" ", strip=True).split())
    dn = next((f"DN{m.group(1)}" for m in DN_RE.finditer(flat)), "")
    ws = next((m.group(1) for m in WS_RE.finditer(flat)), "")
    f10 = next((m.group(1).replace(",", ".") for m in FLOW10_RE.finditer(flat)), "")
    f20 = next((m.group(1).replace(",", ".") for m in FLOW20_RE.finditer(flat)), "")
    flow_generic = next((m.group(1).replace(",", ".") for m in FLOW_RE.finditer(flat)), "")
    h = next((f"{m.group(1)}-{m.group(2)}" for m in HEIGHT_RE.finditer(flat)), "")
    compat = next((m.group(0) for m in COMPAT_RE.finditer(flat)), "")

    table_count = 0
    for t in soup.select("table"):
        table_count += 1
        for tr in t.select("tr"):
            txt = " ".join(tr.get_text(" ", strip=True).split())
            if not txt:
                continue
            am = ARTICLE_RE.search(txt)
            lm = LEN_RE.search(txt)
            row = Row(
                source=url,
                source_type="html_table",
                page_ref=f"table_{table_count}",
                article_no=(am.group(0) if am else ""),
                product_name=title,
                length_mm=(lm.group(1) if lm else ""),
                water_seal_mm=ws,
                flow_rate_lps=flow_generic,
                flow_rate_10mm_lps=f10,
                flow_rate_20mm_lps=f20,
                outlet_dn=dn,
                outlet_orientation=("horizontal" if "waagerecht" in flat.lower() else ""),
                height_range_mm=h,
                compatibility_excerpt=compat,
                notes=("compatibility wording present" if compat else ""),
            )
            if row.article_no or row.length_mm:
                rows.append(row)

    if not rows:
        rows.append(Row(
            source=url,
            source_type="html_page",
            page_ref="main",
            product_name=title,
            water_seal_mm=ws,
            flow_rate_lps=flow_generic,
            flow_rate_10mm_lps=f10,
            flow_rate_20mm_lps=f20,
            outlet_dn=dn,
            outlet_orientation=("horizontal" if "waagerecht" in flat.lower() else ""),
            height_range_mm=h,
            compatibility_excerpt=compat,
            notes="no article table rows parsed",
        ))
    return rows


def parse_pdf(url: str, blob: bytes) -> List[Row]:
    rows: List[Row] = []
    text_pages: List[str] = []
    try:
        import pdfplumber  # type: ignore

        with pdfplumber.open(Path("/tmp/aco_tmp.pdf")) as _:
            pass
    except Exception:
        pass

    # minimal fallback: only record that PDF was reached.
    rows.append(Row(
        source=url,
        source_type="pdf",
        page_ref="n/a",
        notes="PDF fetched; full table extraction requires optional local PDF parser (e.g. pdfplumber) in runtime environment.",
    ))
    return rows


def write_outputs(rows: List[Row]) -> None:
    out_csv = Path("data/diagnostics/aco_splus_extracted_tables.csv")
    out_md = Path("data/diagnostics/aco_splus_extracted_tables.md")
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    compat_rows = [r for r in rows if r.compatibility_excerpt]
    has_safe_matrix = any(r.article_no and r.compatibility_excerpt for r in rows)

    with out_md.open("w", encoding="utf-8") as f:
        f.write("# ACO S+ Extracted Tables (Diagnostic)\n\n")
        f.write("## Sources scanned\n")
        for k, v in URLS.items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Extracted rows\n")
        f.write(f"- Total rows: {len(rows)}\n")
        f.write(f"- Rows with article numbers: {sum(1 for r in rows if r.article_no)}\n")
        f.write(f"- Rows with compatibility wording: {len(compat_rows)}\n")
        f.write("\n## Conclusion\n")
        if has_safe_matrix:
            f.write("Potential article-level compatibility evidence exists; manual validation still required before production assembly.\n")
        else:
            f.write("no safe assembled S+ implementation yet\n")


def main() -> None:
    rows: List[Row] = []
    for key in ("splus_family", "splus_drain_body", "downloads"):
        url = URLS[key]
        try:
            html = fetch(url)
            rows.extend(parse_html(url, html))
        except Exception as e:
            rows.append(Row(source=url, source_type="html_error", page_ref="n/a", notes=f"fetch failed: {type(e).__name__}: {e}"))

    for key in ("pdf_splus", "pdf_line"):
        url = URLS[key]
        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            rows.extend(parse_pdf(url, r.content))
        except Exception as e:
            rows.append(Row(source=url, source_type="pdf_error", page_ref="n/a", notes=f"fetch failed: {type(e).__name__}: {e}"))

    if not rows:
        rows = [Row(source="n/a", source_type="none", page_ref="n/a", notes="no rows")]
    write_outputs(rows)


if __name__ == "__main__":
    main()
