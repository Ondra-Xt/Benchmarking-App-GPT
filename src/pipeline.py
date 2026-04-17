# src/pipeline.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional, Union, Iterable, Set
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


def _normalize_manufacturer(value: Any) -> str:
    return str(value or "").strip().lower()


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
    # Viega article no. in URL tail, e.g. ...-4981-11.html -> 498111
    m3 = re.search(r"-(\d{3,5})-(\d{2})\.html(?:$|[?#])", u, re.IGNORECASE)
    if m3:
        return f"{m}-{m3.group(1)}{m3.group(2)}"
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


def _is_accessory_like(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in ("zubehoer", "zubehör", "rost", "abdeckung", "einleger", "profil", "rahmen", "siphon", "geruch"))


def _select_connector_keys(selected_connectors: Optional[Iterable[str]]) -> Set[str]:
    if not selected_connectors:
        return set(CONNECTORS.keys())
    picked = {str(x).strip().lower() for x in selected_connectors if str(x).strip()}
    valid = set(CONNECTORS.keys())
    out = picked & valid
    return out if out else valid


def _viega_family_hint(row: Dict[str, Any]) -> str:
    for key in ("family", "discovery_seed_family", "product_family"):
        v = str(row.get(key) or "").strip().lower()
        if v:
            return v
    txt = f"{row.get('product_url','')} {row.get('product_name','')}".lower()
    if "tempoplex" in txt:
        return "tempoplex"
    if "domoplex" in txt:
        return "domoplex"
    if "duoplex" in txt:
        return "duoplex"
    if "varioplex" in txt:
        return "varioplex"
    if "bodenablauf" in txt:
        return "advantix_floor"
    if "advantix" in txt:
        return "advantix_line"
    return "unknown"


def _viega_model_block(row: Dict[str, Any]) -> str:
    txt = f"{row.get('product_id','')} {row.get('product_url','')} {row.get('product_name','')}"
    m = re.search(r"(\d{4,5})[-\.]?(\d{2})", txt)
    if m:
        return m.group(1)
    m2 = re.search(r"(\d{4,5})", txt)
    return m2.group(1) if m2 else _slug(str(row.get("product_name") or "unknown"))


def _infer_viega_role(row: Dict[str, Any]) -> str:
    sr = str(row.get("system_role") or "").strip().lower()
    if sr:
        return sr
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    if any(k in txt for k in ("verstellfu", "dichtung", "o-ring", "glocke", "stopfen", "montageset", "schraubenset", "sicherungsverschluss", "siebeinsatz")):
        return "accessory"
    if any(k in txt for k in ("rost", "abdeckung", "verschlussplatte")):
        return "cover"
    if "profil" in txt:
        return "profile"
    if any(k in txt for k in ("grundkörper", "grundkoerper", "rinnenkörper", "rinnenkoerper", "ablaufkörper", "ablaufkoerper", "geruchverschluss")):
        return "base_set"
    if any(k in txt for k in ("duschrinne", "bodenablauf", "ablauf")):
        return "complete_drain"
    return "accessory"

# --- API pro app.py ------------------------------------------------------

def run_discovery(
    target_length_mm: int = 1200,
    tolerance_mm: int = 100,
    selected_connectors: Optional[Iterable[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Vrací:
      registry_df: kandidáti (sloupce: manufacturer, product_name, product_url, candidate_type, …)
      debug_df: diagnostika HTTP
    """
    all_rows: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []

    selected = _select_connector_keys(selected_connectors)
    for key, connector in CONNECTORS.items():
        if key not in selected:
            continue
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
            row["manufacturer"] = _normalize_manufacturer(manufacturer)
            row["product_url"] = str(product_url)
            row["product_name"] = str(product_name)

            # defaulty bez fillna(None)
            if "candidate_type" not in row or row["candidate_type"] in (None, ""):
                row["candidate_type"] = "product_detail"

            all_rows.append(row)

    registry_df = pd.DataFrame(all_rows)

    if registry_df.empty:
        return registry_df, pd.DataFrame(debug_rows)

    # product_id vždy scalar: preserve connector-provided IDs when present
    if "product_id" in registry_df.columns:
        pids = []
        for m, u, pid in zip(registry_df["manufacturer"], registry_df["product_url"], registry_df["product_id"]):
            if pd.isna(pid):
                pid_s = ""
            else:
                pid_s = str(pid).strip()
            if pid_s.lower() == "nan":
                pid_s = ""
            pids.append(pid_s if pid_s else _make_product_id(m, u))
        registry_df["product_id"] = pids
    else:
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

    registry_df = registry_df.drop_duplicates(subset=["manufacturer", "product_id"]).reset_index(drop=True)

    return registry_df, pd.DataFrame(debug_rows)


def run_update(
    registry_df: pd.DataFrame,
    cfg: Union[WeightConfig, Dict[str, Any]],
    target_length_mm: Optional[int] = None,
    tolerance_mm: Optional[int] = None,
    selected_connectors: Optional[Iterable[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Vrací:
      products_df, comparison_df, excluded_df, evidence_df, bom_options_df

    Pozn.: target_length_mm/tolerance_mm tu jsou kvůli app.py (ať to nepadá na unexpected kwarg).
    """
    if registry_df is None or registry_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, empty

    registry_df = registry_df.copy()
    if "manufacturer" in registry_df.columns:
        registry_df["manufacturer"] = registry_df["manufacturer"].map(_normalize_manufacturer)
    selected = _select_connector_keys(selected_connectors)
    if "manufacturer" in registry_df.columns:
        registry_df = registry_df[registry_df["manufacturer"].isin(selected)].reset_index(drop=True)
        if registry_df.empty:
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty

    products_rows: List[Dict[str, Any]] = []
    comparison_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, Any]] = []
    bom_rows: List[Dict[str, Any]] = []
    viega_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    viega_debug = {
        "complete_assembly_candidates_count": 0,
        "promoted_products_count": 0,
        "incomplete_assemblies_count": 0,
        "sample_promoted_products": [],
        "sample_incomplete_assemblies": [],
        "missing_required_parts_counts": {},
        "promotion_reason_counts": {},
        "rows_emitted_to_components_count": 0,
        "rows_emitted_to_products_count": 0,
        "rows_emitted_to_excluded_count": 0,
        "sample_emitted_components": [],
        "sample_emitted_products": [],
        "sample_emitted_excluded": [],
    }

    if "manufacturer" in registry_df.columns:
        for _, rv in registry_df[registry_df["manufacturer"] == "viega"].iterrows():
            rr = rv.to_dict()
            fam = _viega_family_hint(rr)
            block = _viega_model_block(rr)
            key = (fam, block)
            grp = viega_groups.setdefault(key, {"roles": set(), "product_ids": [], "urls": []})
            role = _infer_viega_role(rr)
            grp["roles"].add(role)
            grp["product_ids"].append(str(rr.get("product_id") or ""))
            grp["urls"].append(str(rr.get("product_url") or ""))

    for _, r in registry_df.iterrows():
        manufacturer = _normalize_manufacturer(r.get("manufacturer", ""))
        url = str(r.get("product_url", "")).strip()
        product_id = str(r.get("product_id", _make_product_id(manufacturer, url)))
        candidate_type = str(r.get("candidate_type", "product_detail"))
        complete_system = str(r.get("complete_system", "unknown")).strip().lower()
        excluded_reason = str(r.get("excluded_reason") or r.get("reason") or "").strip()

        if complete_system == "no":
            if manufacturer == "viega":
                viega_debug["rows_emitted_to_excluded_count"] += 1
                if len(viega_debug["sample_emitted_excluded"]) < 20:
                    viega_debug["sample_emitted_excluded"].append(url)
            excluded_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "product_name": r.get("product_name"),
                "product_url": url,
                "candidate_type": candidate_type,
                "complete_system": complete_system,
                "excluded_reason": excluded_reason or "complete_system_no",
            })
            continue

        connector = _pick_connector(manufacturer, url)
        if connector is None:
            if manufacturer == "viega":
                viega_debug["rows_emitted_to_excluded_count"] += 1
                if len(viega_debug["sample_emitted_excluded"]) < 20:
                    viega_debug["sample_emitted_excluded"].append(url)
            excluded_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "product_name": r.get("product_name"),
                "product_url": url,
                "candidate_type": candidate_type,
                "complete_system": complete_system,
                "excluded_reason": "no_connector",
            })
            continue

        params = connector.extract_parameters(url) or {}

        # ACO cleanup: drains without flow should be excluded
        if manufacturer == "aco" and candidate_type == "drain" and params.get("flow_rate_lps") in (None, ""):
            excluded_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "product_name": r.get("product_name"),
                "product_url": url,
                "candidate_type": candidate_type,
                "complete_system": complete_system,
                "excluded_reason": "missing_flow_after_html",
            })
            continue
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

        # Viega promotion-by-assembly: promote only complete assemblies with required parts
        promote_to_product = True
        promotion_reason = "default"
        missing_required_parts: List[str] = []
        matched_component_ids: List[str] = []
        if manufacturer == "viega":
            rowd = r.to_dict() if hasattr(r, "to_dict") else dict(r)
            fam = _viega_family_hint(rowd)
            block = _viega_model_block(rowd)
            role = _infer_viega_role(rowd)
            g = viega_groups.get((fam, block), {"roles": set(), "product_ids": []})
            roles = set(g.get("roles") or set())
            txt = f"{rowd.get('product_name','')} {url}".lower()
            non_promotable = role == "accessory" or any(k in txt for k in ("verstellfu", "dichtung", "o-ring", "glocke", "stopfen", "montageset", "schraubenset", "sicherungsverschluss", "siebeinsatz"))
            is_tray = fam in {"tempoplex", "tempoplex_plus", "tempoplex_60", "domoplex", "duoplex", "varioplex"}
            has_body = any(x in roles for x in {"complete_drain", "base_set"})
            has_top = any(x in roles for x in {"cover", "profile"}) or role == "complete_drain"
            missing_required_parts: List[str] = []
            if not has_body:
                missing_required_parts.append("body_or_base")
            if not is_tray and not has_top:
                missing_required_parts.append("top_element")
            if params.get("flow_rate_lps") in (None, ""):
                missing_required_parts.append("flow_rate_lps")
            if params.get("outlet_dn") in (None, "") and not is_tray:
                missing_required_parts.append("outlet_dn")

            promote = (role in {"complete_drain", "base_set"}) and (not non_promotable) and len(missing_required_parts) == 0
            reason = "promoted_complete_assembly" if promote else ("non_promotable_accessory" if non_promotable else "incomplete_assembly")
            promote_to_product = promote
            promotion_reason = reason
            matched_component_ids = list(g.get("product_ids") or [])
            viega_debug["promotion_reason_counts"][reason] = viega_debug["promotion_reason_counts"].get(reason, 0) + 1
            if promote:
                viega_debug["complete_assembly_candidates_count"] += 1
                viega_debug["promoted_products_count"] += 1
                if len(viega_debug["sample_promoted_products"]) < 20:
                    viega_debug["sample_promoted_products"].append(url)
                candidate_type = "drain"
                if len(viega_debug["sample_emitted_products"]) < 20:
                    viega_debug["sample_emitted_products"].append(url)
            else:
                viega_debug["incomplete_assemblies_count"] += 1
                if len(viega_debug["sample_incomplete_assemblies"]) < 20:
                    viega_debug["sample_incomplete_assemblies"].append(f"{url} missing={','.join(missing_required_parts) or reason}")
                for p in missing_required_parts:
                    viega_debug["missing_required_parts_counts"][p] = viega_debug["missing_required_parts_counts"].get(p, 0) + 1
                # keep in Components by not adding to products/comparison
                evidence_rows.append({
                    "manufacturer": manufacturer,
                    "product_id": product_id,
                    "label": "Viega promotion",
                    "snippet": f"promote_to_product=no reason={reason} missing_required_parts={missing_required_parts} matched_component_ids={g.get('product_ids')}",
                    "source": url,
                })
                candidate_type = "component"
                if len(viega_debug["sample_emitted_components"]) < 20:
                    viega_debug["sample_emitted_components"].append(url)

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
            "promote_to_product": "yes" if promote_to_product else "no",
            "promotion_reason": promotion_reason,
            "missing_required_parts": ",".join(missing_required_parts),
            "matched_component_ids": ",".join(str(x) for x in matched_component_ids),

            # vytažené parametry:
            **{k: v for k, v in params.items() if k != "evidence"},

            # skóre:
            "param_score": param_score,
            "equiv_score": equiv_score,
            "system_score": system_score,
            "final_score": final_score,
        }
        products_rows.append(prod_row)
        if manufacturer == "viega":
            if candidate_type == "drain":
                viega_debug["rows_emitted_to_products_count"] += 1
            else:
                viega_debug["rows_emitted_to_components_count"] += 1

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

    if any(str(x).strip().lower() == "viega" for x in registry_df.get("manufacturer", pd.Series(dtype=str)).tolist()):
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_complete_assembly_candidates_count",
            "snippet": str(viega_debug["complete_assembly_candidates_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_promoted_products_count",
            "snippet": str(viega_debug["promoted_products_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_incomplete_assemblies_count",
            "snippet": str(viega_debug["incomplete_assemblies_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_promoted_products",
            "snippet": str(viega_debug["sample_promoted_products"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_incomplete_assemblies",
            "snippet": str(viega_debug["sample_incomplete_assemblies"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "missing_required_parts_counts",
            "snippet": str(viega_debug["missing_required_parts_counts"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "promotion_reason_counts",
            "snippet": str(viega_debug["promotion_reason_counts"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_rows_emitted_to_components_count",
            "snippet": str(viega_debug["rows_emitted_to_components_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_rows_emitted_to_products_count",
            "snippet": str(viega_debug["rows_emitted_to_products_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_rows_emitted_to_excluded_count",
            "snippet": str(viega_debug["rows_emitted_to_excluded_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_emitted_components",
            "snippet": str(viega_debug["sample_emitted_components"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_emitted_products",
            "snippet": str(viega_debug["sample_emitted_products"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_emitted_excluded",
            "snippet": str(viega_debug["sample_emitted_excluded"][:10]),
            "source": "promotion_stage",
        })

    products_df = pd.DataFrame(products_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    excluded_df = pd.DataFrame(excluded_rows)
    evidence_df = pd.DataFrame(evidence_rows)
    bom_options_df = pd.DataFrame(bom_rows)

    return products_df, comparison_df, excluded_df, evidence_df, bom_options_df
