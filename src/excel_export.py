# src/excel_export.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import json
import re

import pandas as pd
import openpyxl

_ILLEGAL_EXCEL_XML_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
_ILLEGAL_ESCAPED_UNICODE_RE = re.compile(r"\\u00(?:0[0-8BCEFbcef]|1[0-9A-Fa-f])")
_EXCEL_MAX_CELL_LEN = 32767


def _is_nan(x: Any) -> bool:
    try:
        return x != x  # NaN != NaN
    except Exception:
        return False


def _sanitize_excel_string(value: str) -> str:
    s = value or ""
    s = _ILLEGAL_EXCEL_XML_CHARS_RE.sub("", s)
    s = _ILLEGAL_ESCAPED_UNICODE_RE.sub("", s)
    if len(s) > _EXCEL_MAX_CELL_LEN:
        s = s[:_EXCEL_MAX_CELL_LEN]
    return s


def _to_excel_cell(v: Any) -> Any:
    """
    Převede hodnotu z DataFrame na hodnotu, kterou openpyxl umí uložit do buňky.
    - None/NaN -> None
    - list/tuple/set/dict -> JSON string
    - Path -> str
    - numpy scalar -> item()
    """
    if v is None or _is_nan(v):
        return None

    if hasattr(v, "item") and callable(getattr(v, "item")):
        try:
            v = v.item()
        except Exception:
            pass

    if isinstance(v, Path):
        return _sanitize_excel_string(str(v))

    if isinstance(v, (bytes, bytearray)):
        try:
            return _sanitize_excel_string(v.decode("utf-8", errors="ignore"))
        except Exception:
            return _sanitize_excel_string(str(v))

    if isinstance(v, (list, tuple, set, dict)):
        try:
            if isinstance(v, set):
                v = sorted(list(v))
            return _sanitize_excel_string(json.dumps(v, ensure_ascii=False))
        except Exception:
            return _sanitize_excel_string(str(v))

    if isinstance(v, str):
        return _sanitize_excel_string(v)

    return v




def _extract_source_checks(evidence_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "manufacturer", "source_id", "family", "source_url", "source_type", "status_code",
        "final_url", "content_hash_sha256", "content_length", "baseline_hash_sha256",
        "baseline_content_length", "hash_changed", "length_changed", "expected_terms_found",
        "expected_terms_missing", "new_source_candidate_count", "sample_new_source_candidates",
        "review_required", "review_reason", "checked_at", "extraction_mode", "fetch_error",
    ]
    if evidence_df.empty or "label" not in evidence_df.columns or "snippet" not in evidence_df.columns:
        return pd.DataFrame(columns=required_cols)
    rows = []
    for _, ev in evidence_df.iterrows():
        label = str(ev.get("label") or "")
        if not label.startswith("source_check:"):
            continue
        snippet = ev.get("snippet")
        try:
            payload = json.loads(snippet) if isinstance(snippet, str) else {}
        except Exception:
            payload = {}
        row = {k: payload.get(k, "") for k in required_cols}
        if not row.get("manufacturer"):
            row["manufacturer"] = str(ev.get("manufacturer") or "")
        if not row.get("source_id"):
            row["source_id"] = label.split(":", 1)[-1]
        rows.append(row)
    return pd.DataFrame(rows, columns=required_cols)


def _present(v: Any) -> bool:
    if v is None or _is_nan(v):
        return False
    s = str(v).strip().lower()
    return s not in {"", "nan", "none", "null", "unknown", "not_applicable"}


def _scoring_field_coverage(products_df: pd.DataFrame, comparison_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["manufacturer","product_id","product_name","candidate_type","complete_system","in_products","in_comparison",
            "has_flow_rate_lps","has_material_data","has_din_en_1253_data","has_din_en_18534_data",
            "has_height_adjustability_data","has_price_data","has_outlet_flexibility_data","has_sealing_fleece_data",
            "has_colour_count_data","present_scoring_fields","missing_scoring_fields","scoring_readiness_pct","scoring_readiness_note"]
    all_rows = pd.concat(
        [
            products_df.assign(in_products=True, in_comparison=False),
            comparison_df.assign(in_products=False, in_comparison=True),
        ],
        ignore_index=True,
        sort=False,
    )
    if all_rows.empty:
        return pd.DataFrame(columns=cols)
    all_rows = all_rows.drop(columns=["in_products", "in_comparison"], errors="ignore")
    all_rows = (
        all_rows.sort_values(by=["manufacturer", "product_id"])
        .groupby(["manufacturer", "product_id"], as_index=False)
        .agg(lambda s: next((x for x in s if _present(x)), s.iloc[0] if len(s) else ""))
    )
    # Preserve membership flags when a row is present in both Products and Comparison.
    in_products_map = products_df.groupby(["manufacturer", "product_id"]).size().reset_index(name="_n")
    in_comparison_map = comparison_df.groupby(["manufacturer", "product_id"]).size().reset_index(name="_n")
    all_rows = all_rows.merge(
        in_products_map.assign(in_products=True)[["manufacturer", "product_id", "in_products"]],
        on=["manufacturer", "product_id"],
        how="left",
    ).merge(
        in_comparison_map.assign(in_comparison=True)[["manufacturer", "product_id", "in_comparison"]],
        on=["manufacturer", "product_id"],
        how="left",
    )
    all_rows["in_products"] = all_rows["in_products"].fillna(False)
    all_rows["in_comparison"] = all_rows["in_comparison"].fillna(False)
    out = []
    groups = [
        ("has_flow_rate_lps", lambda r: (_present(r.get("flow_rate_lps")) and pd.to_numeric([r.get("flow_rate_lps")], errors="coerce")[0] > 0)),
        ("has_material_data", lambda r: any(_present(r.get(k)) for k in ("material_v4a","material_detail","material_class"))),
        ("has_din_en_1253_data", lambda r: any(_present(r.get(k)) for k in ("din_en_1253","certification_din_en_1253","din_en_1253_cert","certifications","certificate_text"))),
        ("has_din_en_18534_data", lambda r: any(_present(r.get(k)) for k in ("din_en_18534","certification_din_en_18534","din_18534_compliance","waterproofing_standard","certifications","certificate_text"))),
        ("has_height_adjustability_data", lambda r: pd.notna(pd.to_numeric(r.get("height_adj_min_mm"), errors="coerce")) and pd.notna(pd.to_numeric(r.get("height_adj_max_mm"), errors="coerce")) and float(pd.to_numeric(r.get("height_adj_max_mm"), errors="coerce")) > float(pd.to_numeric(r.get("height_adj_min_mm"), errors="coerce"))),
        ("has_price_data", lambda r: any(pd.notna(pd.to_numeric(r.get(k), errors="coerce")) for k in ("sales_price","sales_price_eur","price_eur","offer_price"))),
        ("has_outlet_flexibility_data", lambda r: any(_present(r.get(k)) for k in ("vertical_outlet_available","side_outlet_available","outlet_orientation","outlet_options"))),
        ("has_sealing_fleece_data", lambda r: any(_present(r.get(k)) for k in ("sealing_fleece_preassembled","sealing_fleece","waterproofing_fleece_preassembled"))),
        ("has_colour_count_data", lambda r: any(_present(r.get(k)) for k in ("colours_count","color_count","available_colours","available_colors","finish_count"))),
    ]
    for _, r in all_rows.iterrows():
        flags = {k: bool(fn(r)) for k, fn in groups}
        present = [k for k, v in flags.items() if v]
        missing = [k for k, v in flags.items() if not v]
        rec = {c: r.get(c, "") for c in ("manufacturer","product_id","product_name","candidate_type","complete_system")}
        rec.update({"in_products": bool(r.get("in_products", False)), "in_comparison": bool(r.get("in_comparison", False))})
        rec.update(flags)
        rec["present_scoring_fields"] = ",".join(present)
        rec["missing_scoring_fields"] = ",".join(missing)
        rec["scoring_readiness_pct"] = round((len(present) / len(groups)) * 100.0, 1)
        rec["scoring_readiness_note"] = "ready" if len(missing) == 0 else f"missing:{len(missing)}"
        out.append(rec)
    return pd.DataFrame(out, columns=cols)


def export_excel(
    template_path: str,
    out_path: str,
    cfg: Any,
    registry_df: Optional[pd.DataFrame] = None,
    products_df: Optional[pd.DataFrame] = None,
    comparison_df: Optional[pd.DataFrame] = None,
    excluded_df: Optional[pd.DataFrame] = None,
    evidence_df: Optional[pd.DataFrame] = None,
    bom_options_df: Optional[pd.DataFrame] = None,
    components_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Exportuje výsledky do XLSX.
    - Products: jen benchmarkované produkty (bez base_set)
    - Components: base_set produkty
    """
    wb = openpyxl.load_workbook(template_path)

    registry_df = pd.DataFrame() if registry_df is None else registry_df.copy()
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    excluded_df = pd.DataFrame() if excluded_df is None else excluded_df.copy()
    evidence_df = pd.DataFrame() if evidence_df is None else evidence_df.copy()
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()

    # --- AUTO split base_set -> Components ---
    if not products_df.empty and components_df.empty:
        df = products_df.copy()

        cand = df["candidate_type"].astype(str).str.lower() if "candidate_type" in df.columns else pd.Series([""] * len(df))
        comp = df["complete_system"].astype(str).str.lower() if "complete_system" in df.columns else pd.Series([""] * len(df))

        is_component = cand.isin(["base_set", "component"]) | comp.str.contains("component/base-set", na=False) | comp.str.contains("component", na=False)

        components_df = df[is_component].copy()
        products_df = df[~is_component].copy()

    def write_df(sheet_name: str, df: pd.DataFrame):
        # přepiš sheet
        if sheet_name in wb.sheetnames:
            ws_old = wb[sheet_name]
            wb.remove(ws_old)
        ws = wb.create_sheet(sheet_name)

        # hlavička
        ws.append([_sanitize_excel_string(str(c)) for c in df.columns])

        # řádky
        for _, row in df.iterrows():
            ws.append([_to_excel_cell(v) for v in row.tolist()])

    write_df("Candidates_All", registry_df)
    write_df("Products", products_df)
    write_df("Components", components_df)
    write_df("Comparison", comparison_df)
    write_df("Excluded", excluded_df)
    write_df("Evidence", evidence_df)
    write_df("BOM_Options", bom_options_df)
    write_df("Source_Checks", _extract_source_checks(evidence_df))
    write_df("Scoring_Field_Coverage", _scoring_field_coverage(products_df, comparison_df))

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)
