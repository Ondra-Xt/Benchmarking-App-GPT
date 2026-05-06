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

BENCHMARK_SCORING_KEYS = [
    "flow_rate_score",
    "material_v4a_score",
    "din_en_1253_score",
    "din_en_18534_score",
    "height_adjustability_score",
    "sales_price_score",
    "outlet_flexibility_score",
    "sealing_fleece_score",
    "colour_count_score",
]

DEFAULT_BENCHMARK_WEIGHTS_PCT = {
    "flow_rate_score": 25,
    "material_v4a_score": 15,
    "din_en_1253_score": 10,
    "din_en_18534_score": 10,
    "height_adjustability_score": 10,
    "sales_price_score": 15,
    "outlet_flexibility_score": 5,
    "sealing_fleece_score": 5,
    "colour_count_score": 5,
}

LEGACY_EQUIVALENCE_KEYS = [
    "length_mode_match",
    "selected_length_match",
    "length_delta_within_tolerance",
    "equiv_finish_set_requires_base",
    "equiv_complete_system_bonus",
]


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


def _cfg_to_dict(cfg: Any) -> dict:
    if cfg is None:
        return {}

    if hasattr(cfg, "to_dict") and callable(getattr(cfg, "to_dict")):
        try:
            d = cfg.to_dict()
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    if isinstance(cfg, dict):
        return dict(cfg)

    return {}


def _to_int_weight(value: Any, default: int) -> int:
    try:
        if value is None:
            return int(default)

        if isinstance(value, str) and value.strip() == "":
            return int(default)

        return int(round(float(value)))
    except Exception:
        return int(default)


def _extract_final_scoring_weights(cfg: Any) -> pd.DataFrame:
    """
    Exportuje hlavní Benchmark Scoring V2 váhy.

    Důležité:
    - Do tohoto sheetu nesmí pronikat legacy klíče typu equivalence_overall,
      flow_rate, din_18534, outlet_variants nebo sales_price.
    - sales_price_score má zůstat ve vahách s defaultem 15 a enabled=True.
      Pokud nejsou ceny v Comparison, vylučuje se až ve scoring denominatoru,
      ne v config/exportu.
    """
    cfg_dict = _cfg_to_dict(cfg)

    raw_weights = cfg_dict.get("final_weights_pct")
    if not isinstance(raw_weights, dict):
        raw_weights = {}

    raw_enabled = cfg_dict.get("final_enabled")
    if not isinstance(raw_enabled, dict):
        raw_enabled = {}

    rows = []

    for key in BENCHMARK_SCORING_KEYS:
        weight = _to_int_weight(raw_weights.get(key), DEFAULT_BENCHMARK_WEIGHTS_PCT[key])

        enabled = raw_enabled.get(key, True)
        if not isinstance(enabled, bool):
            enabled = str(enabled).strip().lower() not in {"false", "no", "0", "disabled"}

        note = "Configured benchmark scoring criterion."
        if key == "sales_price_score":
            note = (
                "Configured weight. Missing prices are handled dynamically in the "
                "effective scoring denominator."
            )

        rows.append(
            {
                "key": key,
                "weight_pct": weight,
                "enabled": bool(enabled),
                "scoring_model": "benchmark_scoring_v2",
                "note": note,
            }
        )

    return pd.DataFrame(
        rows,
        columns=["key", "weight_pct", "enabled", "scoring_model", "note"],
    )


def _extract_legacy_equivalence_weights(cfg: Any) -> pd.DataFrame:
    """
    Volitelný oddělený sheet pro staré equivalence váhy.

    Tyto hodnoty jsou pouze legacy/diagnostic a nemají ovlivňovat hlavní final_score.
    """
    cfg_dict = _cfg_to_dict(cfg)

    raw_weights = cfg_dict.get("equivalence_weights_pct")
    if not isinstance(raw_weights, dict):
        raw_weights = {}

    rows = []

    for key in LEGACY_EQUIVALENCE_KEYS:
        rows.append(
            {
                "key": key,
                "weight_pct": _to_int_weight(raw_weights.get(key), 0),
                "enabled": False,
                "scoring_model": "legacy_equivalence_diagnostic_only",
                "note": (
                    "Legacy equivalence weight retained for diagnostics only; "
                    "not used in benchmark final_score."
                ),
            }
        )

    return pd.DataFrame(
        rows,
        columns=["key", "weight_pct", "enabled", "scoring_model", "note"],
    )


def _extract_config_sheet() -> pd.DataFrame:
    """
    Exportuje auditovatelný Config sheet pro XLSX.

    Nahrazuje staré odkazy na Equivalence_Weights jako hlavní scoring konfiguraci.
    """
    rows = [
        {
            "key": "active_scoring_model",
            "value": "benchmark_scoring_v2",
            "note": "Main final_score uses Benchmark Scoring V2 criteria.",
        },
        {
            "key": "benchmark_scoring_weights_sheet",
            "value": "Final_Scoring_Weights",
            "note": "Active scoring weights used for final_score.",
        },
        {
            "key": "legacy_equivalence_weights_sheet",
            "value": "Legacy_Equivalence_Weights",
            "note": "Diagnostic only; disabled and not used in final_score.",
        },
        {
            "key": "sales_price_handling",
            "value": "weight_configured_but_excluded_when_no_prices_available",
            "note": (
                "sales_price_score keeps configured weight 15, but is excluded from "
                "the effective denominator when no Comparison prices exist."
            ),
        },
    ]

    return pd.DataFrame(rows, columns=["key", "value", "note"])


def _extract_source_checks(evidence_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "manufacturer",
        "source_id",
        "family",
        "source_url",
        "source_type",
        "status_code",
        "final_url",
        "content_hash_sha256",
        "content_length",
        "baseline_hash_sha256",
        "baseline_content_length",
        "hash_changed",
        "length_changed",
        "expected_terms_found",
        "expected_terms_missing",
        "new_source_candidate_count",
        "sample_new_source_candidates",
        "review_required",
        "review_reason",
        "checked_at",
        "extraction_mode",
        "fetch_error",
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

    Sheets:
    - Candidates_All
    - Products
    - Components
    - Comparison
    - Excluded
    - Evidence
    - BOM_Options
    - Source_Checks
    - Final_Scoring_Weights
    - Legacy_Equivalence_Weights
    - Config
    """
    wb = openpyxl.load_workbook(template_path)

    registry_df = pd.DataFrame() if registry_df is None else registry_df.copy()
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    excluded_df = pd.DataFrame() if excluded_df is None else excluded_df.copy()
    evidence_df = pd.DataFrame() if evidence_df is None else evidence_df.copy()
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()

    # Odstranit staré template listy, které už nemají být součástí hlavního scoringu.
    # Legacy equivalence data se exportuje explicitně do Legacy_Equivalence_Weights
    # s enabled=False.
    for obsolete_sheet in ["Equivalence_Weights"]:
        if obsolete_sheet in wb.sheetnames:
            ws_old = wb[obsolete_sheet]
            wb.remove(ws_old)

    # --- AUTO split base_set/component -> Components ---
    if not products_df.empty and components_df.empty:
        df = products_df.copy()

        if "candidate_type" in df.columns:
            cand = df["candidate_type"].astype(str).str.lower()
        else:
            cand = pd.Series([""] * len(df), index=df.index)

        if "complete_system" in df.columns:
            comp = df["complete_system"].astype(str).str.lower()
        else:
            comp = pd.Series([""] * len(df), index=df.index)

        is_component = (
            cand.isin(["base_set", "component"])
            | comp.str.contains("component/base-set", na=False)
            | comp.str.contains("component", na=False)
        )

        components_df = df[is_component].copy()
        products_df = df[~is_component].copy()

    def write_df(sheet_name: str, df: pd.DataFrame) -> None:
        # Přepiš sheet, aby v template nezůstávaly staré/hybridní hodnoty.
        if sheet_name in wb.sheetnames:
            ws_old = wb[sheet_name]
            wb.remove(ws_old)

        ws = wb.create_sheet(sheet_name)

        df = pd.DataFrame() if df is None else df.copy()

        # Hlavička
        ws.append([_sanitize_excel_string(str(c)) for c in df.columns])

        # Řádky
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

    # Scoring/config sheets.
    write_df("Final_Scoring_Weights", _extract_final_scoring_weights(cfg))
    write_df("Legacy_Equivalence_Weights", _extract_legacy_equivalence_weights(cfg))
    write_df("Config", _extract_config_sheet())

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)