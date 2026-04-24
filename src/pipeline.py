# src/pipeline.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional, Union, Iterable, Set
import re
import json
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
VIEGA_EXPLICIT_DRAIN_BODY_OVERRIDE_IDS = {
    "viega-491420",
    "viega-498060",
    "viega-498061",
    "viega-498063",
    "viega-495120",
    "viega-495115",
    "viega-495515",
    "viega-495525",
    "viega-491411",
    "viega-491421",
}
VIEGA_TRAY_FAMILIES = {"tempoplex", "tempoplex_plus", "tempoplex_60", "domoplex", "duoplex", "varioplex"}
VIEGA_TRAY_KNOWN_BASE_TO_COVER_BLOCKS = {
    "tempoplex": {"6963": {"6964"}},
}
VIEGA_TEMPOPLEX_DETERMINISTIC_MODEL_PAIRS = {("6963.1", "6964.0")}
VIEGA_TRAY_KNOWN_INCOMPLETE_BASE_MODELS = {
    "tempoplex": {"6963.1"},
    "domoplex": {"6928.21"},
}
VIEGA_ERSATZ_OR_SERVICE_RE = re.compile(r"ersatzteil|ersatzteile|wartung|service|dichtung|o-ring|montageset|schraubenset", re.IGNORECASE)


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
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    model_txt = f"{row.get('product_id','')} {row.get('product_name','')} {row.get('product_url','')}".lower()
    target_drain_body_models = (
        "4914-20", "4980-60", "4980-61", "4980-63",
        "4951-20", "4951-15", "4955-15", "4955-25",
        "4914-11", "4914-21",
    )
    has_target_model = any(m in model_txt.replace(".", "-") for m in target_drain_body_models)
    has_negative_accessory = any(
        k in txt
        for k in (
            "verstellfu",
            "dichtung",
            "o-ring",
            "glocke",
            "stopfen",
            "montageset",
            "schraubenset",
            "sicherungsverschluss",
            "siebeinsatz",
            "reinigungshilfe",
            "reduzierstück",
            "reduzierstueck",
            "verbindungsstück",
            "verbindungsstueck",
            "tauchrohr",
        )
    )
    has_drain_body = any(
        k in txt
        for k in (
            "badablauf",
            "top-badablauf",
            "top badablauf",
            "topbadablauf",
            "top-bodenablauf",
            "top bodenablauf",
            "topbodenablauf",
            "bodenablauf",
            "duschwannenablauf",
            "grundkörper",
            "grundkoerper",
            "rinnenkörper",
            "rinnenkoerper",
            "ablaufkörper",
            "ablaufkoerper",
            "geruchverschluss",
        )
    )
    if sr and sr != "accessory":
        return sr
    if has_target_model:
        return "base_set"
    if has_negative_accessory and not has_drain_body:
        return "accessory"
    if has_drain_body:
        return "base_set"
    if any(k in txt for k in ("abdeckhaube", "abdeckung", "abdeckelement", "deckel", "top cover", "cover element")):
        return "cover"
    if any(k in txt for k in ("rost", "abdeckung", "verschlussplatte")):
        return "cover"
    if "profil" in txt:
        return "profile"
    if any(k in txt for k in ("duschrinne", "bodenablauf", "ablauf")):
        return "complete_drain"
    if sr:
        return sr
    return "accessory"


def _is_explicit_viega_drain_body_override(product_id: str, row: Dict[str, Any]) -> bool:
    pid = str(product_id or "").strip().lower()
    if pid in VIEGA_EXPLICIT_DRAIN_BODY_OVERRIDE_IDS:
        return True
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower().replace(".", "-")
    model_tokens = ("4914-20", "4980-60", "4980-61", "4980-63", "4951-20", "4951-15", "4955-15", "4955-25", "4914-11", "4914-21")
    return any(tok in txt for tok in model_tokens)


def _extract_model_token(text: str) -> str:
    m = re.search(r"(\d{4,5})[.\-](\d{1,2})", text)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return ""


def _is_tray_cover_compatible(base_row: Dict[str, Any], cover_row: Dict[str, Any]) -> bool:
    ok, _reason = _match_tray_cover(base_row, cover_row)
    return ok


def _match_tray_cover(base_row: Dict[str, Any], cover_row: Dict[str, Any]) -> Tuple[bool, str]:
    base_family = _viega_family_hint(base_row)
    cover_family = _viega_family_hint(cover_row)

    base_txt = f"{base_row.get('product_name','')} {base_row.get('product_url','')}".lower()
    cover_txt = f"{cover_row.get('product_name','')} {cover_row.get('product_url','')}".lower()
    base_model = _extract_model_token(base_txt)
    cover_model = _extract_model_token(cover_txt)

    if (base_model, cover_model) in VIEGA_TEMPOPLEX_DETERMINISTIC_MODEL_PAIRS:
        return True, "tempoplex_6963_1_to_6964_0_deterministic_map"

    if base_family != cover_family:
        return False, "family_mismatch"
    if base_family not in VIEGA_TRAY_FAMILIES:
        return False, "non_tray_family"

    base_block = (base_model.split(".", 1)[0] if base_model else _viega_model_block(base_row))
    cover_block = (cover_model.split(".", 1)[0] if cover_model else _viega_model_block(cover_row))
    known_for_family = VIEGA_TRAY_KNOWN_BASE_TO_COVER_BLOCKS.get(base_family, {})
    known_cover_blocks = known_for_family.get(base_block, set())
    if known_cover_blocks and cover_block in known_cover_blocks:
        return True, "known_block_map"

    if base_model and base_model in cover_txt:
        return True, "base_model_mentioned_in_cover"
    if cover_model and cover_model in base_txt:
        return True, "cover_model_mentioned_in_base"
    if base_block and re.search(rf"\b{re.escape(base_block)}(?:[.\-]\d{{1,2}})?\b", cover_txt):
        return True, "base_block_mentioned_in_cover"
    return False, "no_compatibility_signal"


def _is_rejected_ersatz_cover(row: Dict[str, Any]) -> bool:
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    return bool(VIEGA_ERSATZ_OR_SERVICE_RE.search(txt))


def _is_cover_top_element_text(row: Dict[str, Any]) -> bool:
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    return any(tok in txt for tok in ("abdeckhaube", "abdeckung", "abdeckelement", "cover"))


def _parse_cover_variants(params: Dict[str, Any], cover_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = params.get("article_rows_json")
    if not raw:
        return []
    try:
        rows = json.loads(raw) if isinstance(raw, str) else list(raw)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    family = _viega_family_hint(cover_row)
    cover_model = _extract_model_token(f"{cover_row.get('product_name','')} {cover_row.get('product_url','')}".lower()) or _viega_model_block(cover_row)
    compatible_base_model = "6963.1" if family in {"tempoplex", "tempoplex_plus", "tempoplex_60"} and cover_model == "6964.0" else ""
    params_txt = " ".join(
        str(params.get(k) or "")
        for k in ("outlet_dn", "flow_rate_raw_text", "article_rows_json")
    )
    for r in rows:
        if not isinstance(r, dict):
            continue
        article = str(r.get("article_no") or r.get("Artikel") or "").strip()
        if not article:
            continue
        article_norm = re.sub(r"\D", "", article)
        variant_raw = str(r.get("variant_label") or r.get("Ausführung") or r.get("_row_text") or "").strip()
        colour = ""
        mcol = re.search(r"(chrom|schwarz|weiß|weiss|edelstahl|gold|bronze|matt)", variant_raw, re.IGNORECASE)
        if mcol:
            colour = mcol.group(1)
        out.append({
            "cover_article_no": article,
            "cover_article_no_normalized": article_norm,
            "cover_finish_raw": variant_raw,
            "cover_colour": colour,
            "cover_variant_key": f"{cover_model}:{article}",
            "cover_model": cover_model,
            "parent_cover_model": cover_model,
            "compatible_family": family,
            "compatible_base_model": compatible_base_model,
            "diameter_mm": int(m_dia.group(1)) if (m_dia := re.search(r"(?:ø|o/|durchmesser)\s*=?\s*(\d{2,3})", f"{variant_raw} {r.get('_row_text','')}", re.IGNORECASE)) else 115 if cover_model == "6964.0" else None,
            "compatible_outlet_size": "D90" if re.search(r"\bd\s*90\b", f"{variant_raw} {r.get('_row_text','')} {params_txt}", re.IGNORECASE) else ("D90" if cover_model == "6964.0" else None),
            "raw_variant_text": str(r.get("_row_text") or variant_raw),
        })
    return out


def _is_known_or_signaled_incomplete_tray_base(row: Dict[str, Any]) -> bool:
    fam = _viega_family_hint(row)
    if fam not in VIEGA_TRAY_FAMILIES:
        return False
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    model = _extract_model_token(txt)
    known_models = VIEGA_TRAY_KNOWN_INCOMPLETE_BASE_MODELS.get(fam, set())
    if model and model in known_models:
        return True
    if any(sig in txt for sig in ("funktionseinheit", "ohne abdeckhaube", "requires top cover", "requires cover")):
        return True
    return False

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
        "false_positive_promotion_count": 0,
        "sample_demoted_to_components": [],
        "sample_drain_body_matches": [],
        "sample_accessory_matches": [],
        "sample_non_promotable_accessory": [],
        "sample_reclassified_base_sets": [],
        "explicit_override_applied_count": 0,
        "explicit_override_ids": [],
        "sample_overridden_drain_bodies": [],
        "tray_base_set_count": 0,
        "tray_cover_count": 0,
        "tray_pair_candidates_count": 0,
        "tray_complete_systems_created_count": 0,
        "sample_tray_pairings": [],
        "sample_unpaired_tray_base_sets": [],
        "sample_unpaired_tray_covers": [],
        "rejected_wrong_family_cover_count": 0,
        "rejected_ersatzteile_cover_count": 0,
        "tray_cover_variant_count": 0,
        "cover_variant_rows_parsed_count": 0,
        "tempoplex_pairing_fix_applied": 0,
        "paired_product_inheritance_applied_count": 0,
        "sample_paired_products_with_inherited_fields": [],
        "base_set_to_product_field_map": {},
        "cover_to_product_field_map": {},
        "inherited_flow_rate_count": 0,
        "inherited_outlet_dn_count": 0,
        "tempoplex_cover_variant_rows_parsed_count": 0,
        "domoplex_cover_variant_rows_parsed_count": 0,
        "tempoplex_plus_cover_variant_rows_parsed_count": 0,
        "tempoplex_products_created_from_cover_variants_count": 0,
        "tray_products_created_from_cover_variants_count": 0,
        "sample_cover_variant_rows": [],
        "sample_tempoplex_variant_pairings": [],
        "sample_unpaired_tempoplex_cover_variants": [],
        "sample_tray_variant_pairings": [],
        "sample_unpaired_tray_cover_variants": [],
    }
    tray_pairings_by_base_id: Dict[str, List[Dict[str, Any]]] = {}
    tray_pairing_reason_by_base_id: Dict[str, str] = {}
    tray_paired_cover_ids: Set[str] = set()
    cover_variants_by_cover_id: Dict[str, List[Dict[str, Any]]] = {}
    viega_params_by_id: Dict[str, Dict[str, Any]] = {}

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
        tray_rows = [rv.to_dict() for _, rv in registry_df[registry_df["manufacturer"] == "viega"].iterrows() if _viega_family_hint(rv.to_dict()) in VIEGA_TRAY_FAMILIES]
        tray_base_rows = []
        tray_cover_rows = []
        for row in tray_rows:
            role = _infer_viega_role(row)
            txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
            if role == "base_set" and ("ablauf" in txt or "funktionseinheit" in txt):
                tray_base_rows.append(row)
            elif role == "cover":
                tray_cover_rows.append(row)
        viega_debug["tray_base_set_count"] = len(tray_base_rows)
        viega_debug["tray_cover_count"] = len(tray_cover_rows)

        for b in tray_base_rows:
            bpid = str(b.get("product_id") or "")
            matches: List[Dict[str, Any]] = []
            for c in tray_cover_rows:
                cpid = str(c.get("product_id") or "")
                if not cpid or cpid == bpid:
                    continue
                is_ersatz = _is_rejected_ersatz_cover(c)
                is_match, match_reason = _match_tray_cover(b, c)
                ersatz_exception = is_ersatz and _is_cover_top_element_text(c) and is_match
                if is_ersatz and not ersatz_exception:
                    viega_debug["rejected_ersatzteile_cover_count"] += 1
                    continue
                if _viega_family_hint(b) != _viega_family_hint(c):
                    # deterministic Tempoplex map is allowed across tempoplex aliases
                    if not is_match:
                        viega_debug["rejected_wrong_family_cover_count"] += 1
                        continue
                if is_match:
                    if match_reason == "tempoplex_6963_1_to_6964_0_deterministic_map":
                        viega_debug["tempoplex_pairing_fix_applied"] += 1
                    matches.append(c)
                else:
                    viega_debug["rejected_wrong_family_cover_count"] += 1
                    continue
            if matches:
                tray_pairings_by_base_id[bpid] = matches
                tray_pairing_reason_by_base_id[bpid] = "compatible_cover_match"
                viega_debug["tray_pair_candidates_count"] += len(matches)
                for m in matches:
                    tray_paired_cover_ids.add(str(m.get("product_id") or ""))
                    if len(viega_debug["sample_tray_pairings"]) < 20:
                        viega_debug["sample_tray_pairings"].append(
                            f"{b.get('product_url')} + {m.get('product_url')}"
                        )
            elif len(viega_debug["sample_unpaired_tray_base_sets"]) < 20:
                viega_debug["sample_unpaired_tray_base_sets"].append(str(b.get("product_url") or ""))
        for c in tray_cover_rows:
            cpid = str(c.get("product_id") or "")
            if cpid not in tray_paired_cover_ids and len(viega_debug["sample_unpaired_tray_covers"]) < 20:
                viega_debug["sample_unpaired_tray_covers"].append(str(c.get("product_url") or ""))

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
        if manufacturer == "viega":
            viega_params_by_id[product_id] = {k: v for k, v in params.items() if k != "evidence"}

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
        tray_cover_variants: List[Dict[str, Any]] = []
        role = ""
        if manufacturer == "viega":
            rowd = r.to_dict() if hasattr(r, "to_dict") else dict(r)
            fam = _viega_family_hint(rowd)
            block = _viega_model_block(rowd)
            raw_role = str(rowd.get("system_role") or "").strip().lower()
            role = _infer_viega_role(rowd)
            explicit_override = _is_explicit_viega_drain_body_override(product_id, rowd)
            if explicit_override:
                role = "base_set"
                viega_debug["explicit_override_applied_count"] += 1
                if product_id not in viega_debug["explicit_override_ids"]:
                    viega_debug["explicit_override_ids"].append(product_id)
                if len(viega_debug["sample_overridden_drain_bodies"]) < 20:
                    viega_debug["sample_overridden_drain_bodies"].append(url)
            if raw_role == "accessory" and role == "base_set" and len(viega_debug["sample_reclassified_base_sets"]) < 20:
                viega_debug["sample_reclassified_base_sets"].append(url)
            if role == "base_set" and len(viega_debug["sample_drain_body_matches"]) < 20:
                viega_debug["sample_drain_body_matches"].append(url)
            if role == "accessory" and len(viega_debug["sample_accessory_matches"]) < 20:
                viega_debug["sample_accessory_matches"].append(url)
            g = viega_groups.get((fam, block), {"roles": set(), "product_ids": []})
            roles = set(g.get("roles") or set())
            txt = f"{rowd.get('product_name','')} {url}".lower()
            strong_accessory_item = role == "accessory" or any(k in txt for k in ("verstellfu", "dichtung", "o-ring", "glocke", "stopfen", "montageset", "schraubenset", "sicherungsverschluss", "siebeinsatz"))
            strong_accessory_item = strong_accessory_item or any(k in txt for k in ("reinigungshilfe", "reduzierstück", "reduzierstueck", "verbindungsstück", "verbindungsstueck", "tauchrohr", "montagekleber", "abdichtungsband"))
            # meaningful hydraulic bodies (base_set) are non-promoted due incomplete system context,
            # not because they are accessories.
            non_promotable = strong_accessory_item and (not explicit_override)
            if non_promotable and len(viega_debug["sample_non_promotable_accessory"]) < 20:
                viega_debug["sample_non_promotable_accessory"].append(url)
            standalone_subpart = any(
                k in txt
                for k in (
                    "geruchverschluss",
                    "grundkörper",
                    "grundkoerper",
                    "rinnenkörper",
                    "rinnenkoerper",
                    "ablaufkörper",
                    "ablaufkoerper",
                    "profil",
                    "rost",
                    "abdeckung",
                    "verschlussplatte",
                    "reduktion",
                    "reduzierstück",
                    "reduzierstueck",
                    "verbindungsstück",
                    "verbindungsstueck",
                )
            )
            is_tray = fam in {"tempoplex", "tempoplex_plus", "tempoplex_60", "domoplex", "duoplex", "varioplex"}
            tray_matches = tray_pairings_by_base_id.get(product_id, [])
            has_body = any(x in roles for x in {"complete_drain", "base_set"})
            has_top = any(x in roles for x in {"cover", "profile"}) or role == "complete_drain"
            if is_tray and role == "base_set":
                has_top = has_top or bool(tray_matches)
            missing_required_parts: List[str] = []
            if not has_body:
                missing_required_parts.append("body_or_base")
            if not has_top and (role in {"complete_drain", "base_set"}):
                missing_required_parts.append("top_element")
            if params.get("flow_rate_lps") in (None, ""):
                missing_required_parts.append("flow_rate_lps")
            if params.get("outlet_dn") in (None, "") and not is_tray:
                missing_required_parts.append("outlet_dn")
            if standalone_subpart:
                missing_required_parts.append("standalone_subpart_not_complete_product")

            tray_incomplete_signal = is_tray and role == "base_set" and (_is_known_or_signaled_incomplete_tray_base(rowd) or not tray_matches)
            promote = (role in {"complete_drain"}) and (not non_promotable) and (not explicit_override) and len(missing_required_parts) == 0
            if promote:
                reason = "promoted_complete_assembly"
            elif explicit_override:
                reason = "incomplete_assembly"
            elif tray_incomplete_signal:
                reason = "incomplete_assembly"
            elif role == "base_set" and not strong_accessory_item:
                reason = "incomplete_assembly"
            elif is_tray and role == "cover":
                reason = "incomplete_assembly"
            elif non_promotable:
                reason = "non_promotable_accessory"
            elif standalone_subpart:
                reason = "demoted_standalone_subpart"
            else:
                reason = "incomplete_assembly"
            promote_to_product = promote
            promotion_reason = reason
            matched_component_ids = list(g.get("product_ids") or [])
            if is_tray and role == "cover":
                tray_cover_variants = _parse_cover_variants(params, rowd)
                if tray_cover_variants:
                    cover_variants_by_cover_id[product_id] = tray_cover_variants
                    viega_debug["cover_variant_rows_parsed_count"] += len(tray_cover_variants)
                    viega_debug["tray_cover_variant_count"] += len(tray_cover_variants)
                    for var in tray_cover_variants:
                        if len(viega_debug["sample_cover_variant_rows"]) >= 20:
                            break
                        viega_debug["sample_cover_variant_rows"].append(
                            f"{var.get('cover_model')}|{var.get('cover_article_no')}|{var.get('cover_finish_raw')}"
                        )
                    if fam in {"tempoplex", "tempoplex_plus", "tempoplex_60"} and _extract_model_token(f"{rowd.get('product_name','')} {rowd.get('product_url','')}".lower()) == "6964.0":
                        viega_debug["tempoplex_cover_variant_rows_parsed_count"] += len(tray_cover_variants)
                    if fam == "domoplex":
                        viega_debug["domoplex_cover_variant_rows_parsed_count"] += len(tray_cover_variants)
                    if fam in {"tempoplex_plus", "tempoplex_60"}:
                        viega_debug["tempoplex_plus_cover_variant_rows_parsed_count"] += len(tray_cover_variants)
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
                if standalone_subpart:
                    viega_debug["false_positive_promotion_count"] += 1
                    if len(viega_debug["sample_demoted_to_components"]) < 20:
                        viega_debug["sample_demoted_to_components"].append(url)
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
            "pairing_reason": "",
            "why_not_product_reason": "" if promote_to_product else promotion_reason,

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

        if manufacturer == "viega" and tray_cover_variants:
            for idx, var in enumerate(tray_cover_variants, start=1):
                article_norm = str(var.get("cover_article_no_normalized") or "")
                var_id = f"{product_id}__{article_norm}" if article_norm else f"{product_id}__var_{idx}"
                var_name = f"{r.get('product_name')} [{var.get('cover_article_no')}]"
                products_rows.append({
                    "manufacturer": manufacturer,
                    "product_id": var_id,
                    "product_name": var_name,
                    "product_url": url,
                    "candidate_type": "component",
                    "promote_to_product": "no",
                    "promotion_reason": "cover_only_component",
                    "missing_required_parts": "",
                    "matched_component_ids": product_id,
                    "pairing_reason": "",
                    "why_not_product_reason": "cover_only_component",
                    "system_role": "cover",
                    "parent_cover_model": var.get("parent_cover_model"),
                    "cover_model": var.get("cover_model"),
                    "cover_article_no": var.get("cover_article_no"),
                    "cover_article_no_normalized": var.get("cover_article_no_normalized"),
                    "cover_finish_raw": var.get("cover_finish_raw"),
                    "cover_colour": var.get("cover_colour"),
                    "cover_variant_key": var.get("cover_variant_key"),
                    "compatible_family": var.get("compatible_family"),
                    "compatible_base_model": var.get("compatible_base_model"),
                    "diameter_mm": var.get("diameter_mm"),
                    "compatible_outlet_size": var.get("compatible_outlet_size"),
                    "source_url": url,
                    "source_page_title": r.get("product_name"),
                    "colours_count": len(tray_cover_variants),
                    "raw_variant_text": var.get("raw_variant_text"),
                })

    # synthetic tray complete-system products: base_set + compatible cover
    if "manufacturer" in registry_df.columns:
        registry_rows = [r.to_dict() for _, r in registry_df.iterrows()]
        by_id = {str(r.get("product_id") or ""): r for r in registry_rows}
        pre_existing_tempoplex_pair = False
        used_tempoplex_variant_articles: Set[str] = set()

        def _emit_paired_product(
            *,
            base_id: str,
            cover_id: str,
            base_name: str,
            cover_name: str,
            base_url: str,
            fam: str,
            pairing_reason: str,
            var: Optional[Dict[str, Any]] = None,
            product_id_suffix: str = "",
            cover_variant_total: int = 0,
        ) -> None:
            inherited_fields = {}
            base_params = viega_params_by_id.get(base_id, {})
            for k in ("flow_rate_lps", "outlet_dn", "flow_rate_raw_text", "material_detail", "din_en_1253_cert"):
                if base_params.get(k) not in (None, ""):
                    inherited_fields[k] = base_params.get(k)
                    viega_debug["base_set_to_product_field_map"][k] = viega_debug["base_set_to_product_field_map"].get(k, 0) + 1
            if inherited_fields.get("flow_rate_lps") not in (None, ""):
                viega_debug["inherited_flow_rate_count"] += 1
            if inherited_fields.get("outlet_dn") not in (None, ""):
                viega_debug["inherited_outlet_dn_count"] += 1
            if var:
                for k in ("cover_article_no", "cover_finish_raw", "cover_colour", "cover_variant_key", "compatible_base_model", "diameter_mm", "compatible_outlet_size"):
                    if var.get(k) not in (None, ""):
                        viega_debug["cover_to_product_field_map"][k] = viega_debug["cover_to_product_field_map"].get(k, 0) + 1
            param_score, _param_detail = compute_parameter_score(inherited_fields, cfg)
            equiv_score = compute_equivalence_score({"candidate_type": "drain", **inherited_fields}, cfg)
            system_score = compute_system_score("drain", has_bom_options=False)
            final_score = compute_final_score(param_score, system_score, equiv_score, cfg)

            if var and str(var.get("cover_article_no_normalized") or ""):
                full_id = f"{base_id}__{var.get('cover_article_no_normalized')}"
            else:
                full_id = f"{base_id}__{cover_id}{product_id_suffix}"
            pname = f"{base_name} + {cover_name}"
            if var and var.get("cover_article_no"):
                pname = f"{pname} [{var.get('cover_article_no')}]"
            row = {
                "manufacturer": "viega",
                "product_id": full_id,
                "product_name": pname,
                "product_url": base_url,
                "candidate_type": "drain",
                "promote_to_product": "yes",
                "promotion_reason": "tray_base_with_cover_pairing",
                "missing_required_parts": "",
                "matched_component_ids": ",".join([base_id, cover_id]),
                "pairing_reason": pairing_reason,
                "why_not_product_reason": "",
                "drain_category": "shower_tray_drain",
                "system_role": "complete_drain",
                "product_family": fam,
                "base_set_source_id": base_id,
                "cover_source_id": cover_id,
                **inherited_fields,
                "param_score": param_score,
                "equiv_score": equiv_score,
                "system_score": system_score,
                "final_score": final_score,
            }
            if var:
                row.update({
                    "cover_article_no": var.get("cover_article_no"),
                    "cover_article_no_normalized": var.get("cover_article_no_normalized"),
                    "cover_finish_raw": var.get("cover_finish_raw"),
                    "cover_colour": var.get("cover_colour"),
                    "cover_variant_key": var.get("cover_variant_key"),
                    "compatible_base_model": var.get("compatible_base_model"),
                    "diameter_mm": var.get("diameter_mm"),
                    "compatible_outlet_size": var.get("compatible_outlet_size"),
                })
            if cover_variant_total > 0:
                row["colours_count"] = cover_variant_total
            products_rows.append(row)
            comparison_rows.append({
                "manufacturer": "viega",
                "product_id": full_id,
                "product_name": pname,
                "product_url": base_url,
                "final_score": final_score,
                "param_score": param_score,
                "equiv_score": equiv_score,
                "system_score": system_score,
            })
            viega_debug["tray_complete_systems_created_count"] += 1
            viega_debug["paired_product_inheritance_applied_count"] += 1
            if len(viega_debug["sample_paired_products_with_inherited_fields"]) < 20:
                viega_debug["sample_paired_products_with_inherited_fields"].append(
                    f"{full_id}: inherited={sorted(inherited_fields.keys())}"
                )
            base_model = _extract_model_token(f"{base_name} {base_url}".lower())
            if base_model == "6963.1" and var and str(var.get("cover_article_no_normalized") or ""):
                viega_debug["tempoplex_products_created_from_cover_variants_count"] += 1
                used_tempoplex_variant_articles.add(str(var.get("cover_article_no_normalized")))
                if len(viega_debug["sample_tempoplex_variant_pairings"]) < 20:
                    viega_debug["sample_tempoplex_variant_pairings"].append(
                        f"{base_id}+{var.get('cover_article_no_normalized')}"
                    )
            if var:
                viega_debug["tray_products_created_from_cover_variants_count"] += 1
                if len(viega_debug["sample_tray_variant_pairings"]) < 20:
                    viega_debug["sample_tray_variant_pairings"].append(
                        f"{base_id}+{var.get('cover_article_no_normalized') or var.get('cover_article_no')}"
                    )

        for base_id, cover_rows in tray_pairings_by_base_id.items():
            base_row = by_id.get(base_id)
            if not base_row:
                continue
            base_name = str(base_row.get("product_name") or "")
            base_url = str(base_row.get("product_url") or "")
            fam = _viega_family_hint(base_row)
            for cover_row in cover_rows[:1]:
                cover_id = str(cover_row.get("product_id") or "")
                cover_name = str(cover_row.get("product_name") or "")
                base_model = _extract_model_token(f"{base_name} {base_url}".lower())
                cover_model = _extract_model_token(f"{cover_name} {cover_row.get('product_url','')}".lower())
                if (base_model, cover_model) in VIEGA_TEMPOPLEX_DETERMINISTIC_MODEL_PAIRS:
                    pre_existing_tempoplex_pair = True
                cover_variants = cover_variants_by_cover_id.get(cover_id, [])
                if cover_variants:
                    for var in cover_variants:
                        var_key = str(var.get("cover_variant_key") or var.get("cover_article_no") or cover_id)
                        _emit_paired_product(
                            base_id=base_id,
                            cover_id=cover_id,
                            base_name=base_name,
                            cover_name=cover_name,
                            base_url=base_url,
                            fam=fam,
                            pairing_reason=tray_pairing_reason_by_base_id.get(base_id, "compatible_cover_match"),
                            var=var,
                            product_id_suffix=f"__{var_key}",
                            cover_variant_total=len(cover_variants),
                        )
                else:
                    _emit_paired_product(
                        base_id=base_id,
                        cover_id=cover_id,
                        base_name=base_name,
                        cover_name=cover_name,
                        base_url=base_url,
                        fam=fam,
                        pairing_reason=tray_pairing_reason_by_base_id.get(base_id, "compatible_cover_match"),
                    )

        # final deterministic fallback: ensure Tempoplex 6963.1 + 6964.0 emits at least one paired product
        if not pre_existing_tempoplex_pair:
            base_candidates = [
                r for r in registry_rows
                if _extract_model_token(f"{r.get('product_name','')} {r.get('product_url','')}".lower()) == "6963.1"
                and _infer_viega_role(r) == "base_set"
            ]
            cover_candidates = [
                r for r in registry_rows
                if _extract_model_token(f"{r.get('product_name','')} {r.get('product_url','')}".lower()) == "6964.0"
                and _infer_viega_role(r) == "cover"
            ]
            if base_candidates and cover_candidates:
                b = base_candidates[0]
                c = cover_candidates[0]
                base_id = str(b.get("product_id") or "")
                cover_id = str(c.get("product_id") or "")
                _emit_paired_product(
                    base_id=base_id,
                    cover_id=cover_id,
                    base_name=str(b.get("product_name") or ""),
                    cover_name=str(c.get("product_name") or ""),
                    base_url=str(b.get("product_url") or ""),
                    fam="tempoplex",
                    pairing_reason="tempoplex_6963_1_to_6964_0_final_fallback",
                    product_id_suffix="__deterministic",
                )
                viega_debug["tempoplex_pairing_fix_applied"] += 1
                if len(viega_debug["sample_tray_pairings"]) < 20:
                    viega_debug["sample_tray_pairings"].append(f"{b.get('product_url')} + {c.get('product_url')}")

        for cover_id, variants in cover_variants_by_cover_id.items():
            cover_row = by_id.get(cover_id, {})
            cover_model = _extract_model_token(f"{cover_row.get('product_name','')} {cover_row.get('product_url','')}".lower())
            for var in variants:
                article_norm = str(var.get("cover_article_no_normalized") or "")
                if article_norm and article_norm not in used_tempoplex_variant_articles and cover_model == "6964.0":
                    if len(viega_debug["sample_unpaired_tempoplex_cover_variants"]) < 20:
                        viega_debug["sample_unpaired_tempoplex_cover_variants"].append(article_norm)
                if article_norm:
                    used_in_any_pair = any(
                        article_norm in str(x) for x in viega_debug["sample_tray_variant_pairings"]
                    )
                    if (not used_in_any_pair) and len(viega_debug["sample_unpaired_tray_cover_variants"]) < 20:
                        viega_debug["sample_unpaired_tray_cover_variants"].append(article_norm)

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
            "label": "explicit_override_applied_count",
            "snippet": str(viega_debug["explicit_override_applied_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "explicit_override_ids",
            "snippet": str(viega_debug["explicit_override_ids"][:20]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_overridden_drain_bodies",
            "snippet": str(viega_debug["sample_overridden_drain_bodies"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "viega_false_positive_promotion_count",
            "snippet": str(viega_debug["false_positive_promotion_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_demoted_to_components",
            "snippet": str(viega_debug["sample_demoted_to_components"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_demoted_standalone_subpart",
            "snippet": str(viega_debug["sample_demoted_to_components"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_drain_body_matches",
            "snippet": str(viega_debug["sample_drain_body_matches"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_accessory_matches",
            "snippet": str(viega_debug["sample_accessory_matches"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_non_promotable_accessory",
            "snippet": str(viega_debug["sample_non_promotable_accessory"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_reclassified_base_sets",
            "snippet": str(viega_debug["sample_reclassified_base_sets"][:10]),
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
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_base_set_count",
            "snippet": str(viega_debug["tray_base_set_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_cover_count",
            "snippet": str(viega_debug["tray_cover_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_cover_variant_count",
            "snippet": str(viega_debug["tray_cover_variant_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_pair_candidates_count",
            "snippet": str(viega_debug["tray_pair_candidates_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_complete_systems_created_count",
            "snippet": str(viega_debug["tray_complete_systems_created_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_tray_pairings",
            "snippet": str(viega_debug["sample_tray_pairings"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_unpaired_tray_base_sets",
            "snippet": str(viega_debug["sample_unpaired_tray_base_sets"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_unpaired_tray_covers",
            "snippet": str(viega_debug["sample_unpaired_tray_covers"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "rejected_wrong_family_cover_count",
            "snippet": str(viega_debug["rejected_wrong_family_cover_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "rejected_ersatzteile_cover_count",
            "snippet": str(viega_debug["rejected_ersatzteile_cover_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "cover_variant_rows_parsed_count",
            "snippet": str(viega_debug["cover_variant_rows_parsed_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_cover_variant_rows",
            "snippet": str(viega_debug["sample_cover_variant_rows"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tempoplex_pairing_fix_applied",
            "snippet": str(viega_debug["tempoplex_pairing_fix_applied"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "paired_product_inheritance_applied_count",
            "snippet": str(viega_debug["paired_product_inheritance_applied_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_paired_products_with_inherited_fields",
            "snippet": str(viega_debug["sample_paired_products_with_inherited_fields"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "base_set_to_product_field_map",
            "snippet": str(viega_debug["base_set_to_product_field_map"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "cover_to_product_field_map",
            "snippet": str(viega_debug["cover_to_product_field_map"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "inherited_flow_rate_count",
            "snippet": str(viega_debug["inherited_flow_rate_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "inherited_outlet_dn_count",
            "snippet": str(viega_debug["inherited_outlet_dn_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tempoplex_cover_variant_rows_parsed_count",
            "snippet": str(viega_debug["tempoplex_cover_variant_rows_parsed_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tempoplex_products_created_from_cover_variants_count",
            "snippet": str(viega_debug["tempoplex_products_created_from_cover_variants_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_tempoplex_variant_pairings",
            "snippet": str(viega_debug["sample_tempoplex_variant_pairings"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_unpaired_tempoplex_cover_variants",
            "snippet": str(viega_debug["sample_unpaired_tempoplex_cover_variants"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "domoplex_cover_variant_rows_parsed_count",
            "snippet": str(viega_debug["domoplex_cover_variant_rows_parsed_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tempoplex_plus_cover_variant_rows_parsed_count",
            "snippet": str(viega_debug["tempoplex_plus_cover_variant_rows_parsed_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "tray_products_created_from_cover_variants_count",
            "snippet": str(viega_debug["tray_products_created_from_cover_variants_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_tray_variant_pairings",
            "snippet": str(viega_debug["sample_tray_variant_pairings"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_unpaired_tray_cover_variants",
            "snippet": str(viega_debug["sample_unpaired_tray_cover_variants"][:10]),
            "source": "promotion_stage",
        })

    products_df = pd.DataFrame(products_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    excluded_df = pd.DataFrame(excluded_rows)
    evidence_df = pd.DataFrame(evidence_rows)
    bom_options_df = pd.DataFrame(bom_rows)

    return products_df, comparison_df, excluded_df, evidence_df, bom_options_df
