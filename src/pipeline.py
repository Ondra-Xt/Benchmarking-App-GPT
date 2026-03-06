# src/pipeline.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional, Union
import re
import pandas as pd

from .config import WeightConfig
from .connectors import CONNECTORS
from .scoring import (
    compute_parameter_score,
    compute_equivalence_score,
    compute_system_score,
    compute_final_score,
)

# --- helpers -------------------------------------------------------------

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _make_product_id(manufacturer: str, url: str) -> str:
    """
    Stabilní ID (string) – žádné DataFrame->Series omyly.
    """
    m = _slug(manufacturer)
    u = (url or "").strip()
    # zkusti vytáhnout 8 číslic (Hansgrohe artikl) nebo 6 číslic (Dallmer SKU)
    m1 = re.search(r"(\d{8})(?!\d)", u)
    if m1:
        return f"{m}-{m1.group(1)}"
    m2 = re.search(r"/(\d{6})_", u)
    if m2:
        return f"{m}-{m2.group(1)}"
    return f"{m}-{abs(hash(u))}"


def _pick_connector(manufacturer: str, url: str):
    """
    Prefer manufacturer key, fallback to URL-based detection.
    """
    m = (manufacturer or "").strip().lower()
    if m in CONNECTORS:
        return CONNECTORS[m]

    u = (url or "").strip().lower()
    if "dallmer." in u:
        return CONNECTORS.get("dallmer")
    if "hansgrohe." in u:
        return CONNECTORS.get("hansgrohe")

    return None


# --- API pro app.py ------------------------------------------------------

def run_discovery(target_length_mm: int = 1200, tolerance_mm: int = 100) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Vrací:
      registry_df: kandidáti (sloupce: manufacturer, product_name, product_url, candidate_type, …)
      debug_df: diagnostika HTTP
    """
    all_rows: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []

    for key, connector in CONNECTORS.items():
        found, dbg = connector.discover_candidates(target_length_mm=target_length_mm, tolerance_mm=tolerance_mm)
        if dbg:
            debug_rows.extend(dbg)

        # found může být list dictů
        for r in (found or []):
            # sjednoť názvy
            manufacturer = r.get("manufacturer") or key
            product_url = r.get("product_url") or r.get("sources") or r.get("url")
            product_name = r.get("product_name") or r.get("product") or "unknown"

            row = dict(r)
            row["manufacturer"] = str(manufacturer).lower()
            row["product_url"] = str(product_url)
            row["product_name"] = str(product_name)

            # defaulty bez fillna(None)
            if "candidate_type" not in row or row["candidate_type"] in (None, ""):
                row["candidate_type"] = "product_detail"

            all_rows.append(row)

    registry_df = pd.DataFrame(all_rows)

    if registry_df.empty:
        return registry_df, pd.DataFrame(debug_rows)

    # product_id vždy scalar:
    registry_df["product_id"] = [
        _make_product_id(m, u) for m, u in zip(registry_df["manufacturer"], registry_df["product_url"])
    ]

    # pár jistých sloupců:
    for col, default_val in [
        ("product_family", "unknown"),
        ("available_lengths_mm", ""),
        ("selected_length_mm", target_length_mm),
        ("length_delta_mm", None),
        ("complete_system", "unknown"),
    ]:
        if col not in registry_df.columns:
            registry_df[col] = default_val
        else:
            # nefilluj None → jen když default_val není None
            if default_val is not None:
                registry_df[col] = registry_df[col].fillna(default_val)

    registry_df = registry_df.drop_duplicates(subset=["manufacturer", "product_url"]).reset_index(drop=True)

    return registry_df, pd.DataFrame(debug_rows)


def run_update(
    registry_df: pd.DataFrame,
    cfg: Union[WeightConfig, Dict[str, Any]],
    target_length_mm: Optional[int] = None,
    tolerance_mm: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Vrací:
      products_df, comparison_df, excluded_df, evidence_df, bom_options_df

    Pozn.: target_length_mm/tolerance_mm tu jsou kvůli app.py (ať to nepadá na unexpected kwarg).
    """
    if registry_df is None or registry_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, empty

    products_rows: List[Dict[str, Any]] = []
    comparison_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, Any]] = []
    bom_rows: List[Dict[str, Any]] = []

    for _, r in registry_df.iterrows():
        manufacturer = str(r.get("manufacturer", "")).lower()
        url = str(r.get("product_url", "")).strip()
        product_id = str(r.get("product_id", _make_product_id(manufacturer, url)))
        candidate_type = str(r.get("candidate_type", "product_detail"))

        connector = _pick_connector(manufacturer, url)
        if connector is None:
            excluded_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "product_url": url,
                "reason": "no_connector",
            })
            continue

        params = connector.extract_parameters(url) or {}
        # get_bom_options je volitelné
        options = []
        if hasattr(connector, "get_bom_options"):
            try:
                options = connector.get_bom_options(url, params=params) or []
            except TypeError:
                options = connector.get_bom_options(url) or []
            for opt in options:
                bom_rows.append({
                    "manufacturer": manufacturer,
                    "product_id": product_id,
                    **opt,
                })

        # evidence z konektoru: list(tuple(label, snippet, source))
        for ev in (params.get("evidence") or []):
            try:
                label, snippet, source = ev
            except Exception:
                continue
            evidence_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "label": str(label),
                "snippet": str(snippet),
                "source": str(source),
            })

        # scoring
        param_score, param_detail = compute_parameter_score(params, cfg)
        equiv_score = compute_equivalence_score({"candidate_type": candidate_type, **params}, cfg)
        system_score = compute_system_score(candidate_type, has_bom_options=bool(options))
        final_score = compute_final_score(param_score, system_score, equiv_score, cfg)

        prod_row = {
            "manufacturer": manufacturer,
            "product_id": product_id,
            "product_name": r.get("product_name"),
            "product_url": url,
            "candidate_type": candidate_type,

            # vytažené parametry:
            **{k: v for k, v in params.items() if k != "evidence"},

            # skóre:
            "param_score": param_score,
            "equiv_score": equiv_score,
            "system_score": system_score,
            "final_score": final_score,
        }
        products_rows.append(prod_row)

        comparison_rows.append({
            "manufacturer": manufacturer,
            "product_id": product_id,
            "product_name": r.get("product_name"),
            "product_url": url,
            "final_score": final_score,
            "param_score": param_score,
            "equiv_score": equiv_score,
            "system_score": system_score,
        })

        # detail pro debug (volitelné)
        for k, v in (param_detail or {}).items():
            evidence_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "label": f"Param detail: {k}",
                "snippet": str(v),
                "source": url,
            })

    products_df = pd.DataFrame(products_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    excluded_df = pd.DataFrame(excluded_rows)
    evidence_df = pd.DataFrame(evidence_rows)
    bom_options_df = pd.DataFrame(bom_rows)

    return products_df, comparison_df, excluded_df, evidence_df, bom_options_df