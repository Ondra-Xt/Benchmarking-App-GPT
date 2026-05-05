# src/pipeline.py
from __future__ import annotations

from pathlib import Path
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


def _normalize_aco_token(s: str) -> str:
    t = (s or "").lower()
    repl = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "č": "c", "ř": "r", "š": "s", "ž": "z", "ý": "y", "á": "a", "í": "i", "é": "e", "ů": "u", "ú": "u", "ň": "n", "ť": "t", "ď": "d"}
    for a, b in repl.items():
        t = t.replace(a, b)
    t = re.sub(r"[^a-z0-9]+", "-", t)
    return re.sub(r"-{2,}", "-", t).strip("-")


def _stable_aco_id_from_row(row: Dict[str, Any]) -> str:
    pid = str(row.get("product_id") or "").strip().lower()
    m = re.search(r"\b(\d{8})\b", f"{row.get('article_no','')} {row.get('product_name','')} {row.get('product_url','')}")
    if m:
        return f"aco-{m.group(1)}"
    url = str(row.get("product_url") or "")
    parts = [p for p in url.split("/") if p]
    stop = {"produkte", "produkty", "badentwaesserung", "badablaeufe", "duschrinnen", "reihenduschrinnen", "odvodneni-koupelen", "zubehoer"}
    slug = ""
    for p in reversed(parts):
        pn = _normalize_aco_token(p)
        if pn and pn not in stop:
            slug = pn
            break
    fam = _normalize_aco_token(str(_aco_family_hint(row) or "showerdrain").replace("_", "-"))
    role = _normalize_aco_token(str(row.get("system_role") or row.get("candidate_type") or "product"))
    if not slug:
        slug = _normalize_aco_token(str(row.get("product_name") or ""))[:48]
    if not slug:
        return pid or "aco-unknown"
    return f"aco-{fam}-{slug}" if fam not in slug else f"aco-{slug}"


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


ACO_COMPLETE_SYSTEM_ROLES = {"complete_system", "complete_drain"}
ACO_CONFIGURATION_FAMILY_ROLES = {"configuration_family"}
ACO_DRAIN_BODY_ROLES = {"drain_body", "base_set", "drain_unit", "body", "rinnenkörper", "rinnenkoerper", "ablaufkörper", "ablaufkoerper", "einzelablauf"}
ACO_COVER_ROLES = {"grate", "cover", "design_grate", "rost", "abdeckung"}
ACO_ACCESSORY_ROLES = {"accessory", "service_part", "adapter", "showerstep", "gefällekeil", "gefaellekeil", "aufsatzstück", "aufsatzstueck"}


def _classify_aco_promotion(row: Dict[str, Any], candidate_type: str) -> Tuple[str, bool, str]:
    role = str(row.get("system_role") or "").strip().lower()
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    classification_reason = str(row.get("classification_reason") or "").strip().lower()
    is_article_row_variant = candidate_type == "drain" and classification_reason == "article_row_variant"
    has_easyflow_family = any(tok in txt for tok in ("easyflow+", "easyflow-plus", "easyflow-", " easyflow "))
    has_complete_tokens = any(tok in txt for tok in ("komplettablauf", "komplettabläufe", "komplettablaeufe", "complete drain"))

    if role == "configuration_family" and has_easyflow_family and has_complete_tokens:
        return "drain", True, "complete_system"

    # strict role-first guardrails: component roles must never be promoted by broad complete-system tokens
    if role in ACO_CONFIGURATION_FAMILY_ROLES:
        return "component", False, "configuration_family"
    if role in ACO_COVER_ROLES:
        return "component", False, "cover_only_component"
    if role in ACO_ACCESSORY_ROLES:
        return "component", False, "accessory_only"
    if role in ACO_DRAIN_BODY_ROLES:
        # preserve accepted ACO article-row variants as product drains
        if is_article_row_variant:
            return "drain", True, "article_row_variant"
        return "component", False, "incomplete_assembly"

    # explicit text guardrails from observed false positives
    has_cover_tokens = any(t in txt for t in ("design-rost", "designrost", "design-rost", "design roste", "design-roste", "rost", "abdeckung"))
    has_accessory_tokens = any(t in txt for t in ("showerstep", "gefällekeil", "gefaellekeil", "aufsatzstücke", "aufsatzstuecke"))
    has_body_tokens = any(t in txt for t in ("ablaufkörper", "ablaufkoerper", "rinnenkörper", "rinnenkoerper"))
    if not is_article_row_variant:
        if has_cover_tokens:
            return "component", False, "cover_only_component"
        if has_accessory_tokens:
            return "component", False, "accessory_only"
        if has_body_tokens:
            return "component", False, "incomplete_assembly"

    complete_signal = role in ACO_COMPLETE_SYSTEM_ROLES or any(
        token in txt for token in ("komplettablauf", "showerpoint", "passino", "passavant")
    )
    if "public" in txt and "showerdrain" in txt:
        complete_signal = True

    if complete_signal:
        return "drain", True, "complete_system"
    if is_article_row_variant:
        return "drain", True, "article_row_variant"
    if candidate_type == "drain":
        return "drain", True, "default"
    return "component", False, "not_complete_system"


def _aco_family_hint(row: Dict[str, Any]) -> str:
    family = str(row.get("product_family") or "").strip().lower()
    if family and family != "unknown":
        return family
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    if "easyflow+" in txt or "easyflow-plus" in txt:
        return "easyflowplus"
    if "easyflow" in txt:
        return "easyflow"
    if "showerdrain c" in txt or "showerdrain-c" in txt:
        return "showerdrain_c"
    if "showerdrain e+" in txt or "showerdrain-eplus" in txt:
        return "showerdrain_eplus"
    if "showerdrain m+" in txt or "showerdrain-mplus" in txt:
        return "showerdrain_mplus"
    if "showerdrain public 80" in txt:
        return "showerdrain_public_80"
    if "showerdrain public 110" in txt:
        return "showerdrain_public_110"
    if "showerdrain public x" in txt:
        return "showerdrain_public_x"
    return "unknown"


def _aco_family_targets(row: Dict[str, Any], base_family: str, bucket: str) -> List[str]:
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    out = [base_family]
    # some catalog option pages are shared between EasyFlow+ and Easyflow (typically grates/adapters)
    # keep strict no-cross pairing by duplicating shared option rows into both families explicitly.
    if bucket in {"grate", "accessory"}:
        has_plus = ("easyflow+" in txt) or ("easyflow-plus" in txt)
        has_plain = bool(re.search(r"easyflow(?!\+|-plus)", txt))
        if has_plus and has_plain:
            if "easyflowplus" not in out:
                out.append("easyflowplus")
            if "easyflow" not in out:
                out.append("easyflow")
    return list(dict.fromkeys([x for x in out if x]))


def _aco_role_bucket(row: Dict[str, Any]) -> str:
    role = str(row.get("system_role") or "").strip().lower()
    if not role:
        why_not = str(row.get("why_not_product_reason") or "").strip().lower()
        promo = str(row.get("promotion_reason") or "").strip().lower()
        if promo == "complete_system":
            role = "complete_system"
        elif why_not == "configuration_family_not_final_product":
            role = "configuration_family"
        elif why_not == "cover_only_component":
            role = "grate"
        elif why_not == "accessory_only":
            role = "accessory"
        elif why_not == "incomplete_assembly":
            role = "drain_body"
    txt = f"{row.get('product_name','')} {row.get('product_url','')}".lower()
    classification_reason = str(row.get("classification_reason") or "").strip().lower()
    if role in ACO_COVER_ROLES or any(t in txt for t in ("designrost", "design-rost", "design-roste", "design roste", "rost", "abdeckung")):
        return "grate"
    if role in ACO_ACCESSORY_ROLES or any(t in txt for t in ("showerstep", "gefällekeil", "gefaellekeil", "aufsatzstück", "aufsatzstueck", "aufsatzstücke", "aufsatzstuecke", "adapter")):
        return "accessory"
    if role in ACO_DRAIN_BODY_ROLES or any(t in txt for t in ("rinnenkörper", "rinnenkoerper", "ablaufkörper", "ablaufkoerper", "einzelablauf")):
        if role == "drain_unit" and classification_reason == "article_row_variant":
            return "article_variant"
        return "base_set"
    if role in ACO_COMPLETE_SYSTEM_ROLES or any(t in txt for t in ("komplettablauf", "showerpoint", "passino", "passavant", "showerdrain public")):
        return "complete_system"
    if role in ACO_CONFIGURATION_FAMILY_ROLES:
        return "configuration_family"
    return "other"


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


def _should_emit_evidence_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    sval = str(value).strip().lower()
    if sval in {"", "nan", "none", "null", "not_applicable", "n/a"}:
        return False
    return True


def _is_kaldewei_ka120_row(row: Dict[str, Any]) -> bool:
    fam = str(row.get("product_family") or row.get("family") or "").lower()
    pid = str(row.get("product_id") or "").lower()
    name = str(row.get("product_name") or "").lower()
    return fam == "ka_120" or "ka-120" in pid or "ka 120" in name


def _kaldewei_evidence_note(row: Dict[str, Any], field_name: str) -> str:
    if _is_kaldewei_ka120_row(row):
        return "KA120 value seeded from official Kaldewei KA120 technical sheet; PDF excerpt parsing not yet implemented"
    fam = str(row.get("product_family") or row.get("family") or "").lower()
    if fam == "flowdrain":
        return "FLOWDRAIN value seeded from official Kaldewei FLOWDRAIN technical source; PDF excerpt parsing not yet implemented"
    if fam in {"flowline_zero", "flowpoint_zero"}:
        return "FLOW value seeded from official Kaldewei FLOW product source; PDF excerpt parsing not yet implemented"
    if fam == "nexsys":
        return "NEXSYS value seeded from official Kaldewei NEXSYS source; PDF excerpt parsing not yet implemented"
    return "curated Kaldewei catalog value; source URL known, excerpt not yet parsed"


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


def _normalize_cover_article(article_text: str) -> Optional[str]:
    txt = str(article_text or "").strip()
    if not txt:
        return None
    nums = re.findall(r"\b\d{3}\s?\d{3}\b", txt)
    if len(nums) != 1:
        return None
    return re.sub(r"\D", "", nums[0]) or None


def _normalize_cover_article_with_refs(article_text: str) -> Optional[str]:
    normalized = _normalize_cover_article(article_text)
    if normalized:
        return normalized
    txt = str(article_text or "")
    if not txt:
        return None
    # Narrow rescue for row text/article cells that append cross-reference numbers
    # (e.g. "775 070 1) siehe auch 775 087 775 094"): keep the first article token.
    if re.search(r"siehe|see|vgl\.?|referenz|hinweis", txt, re.IGNORECASE):
        m = re.search(r"\b(\d{3}\s?\d{3})\b", txt)
        if m:
            return re.sub(r"\D", "", m.group(1))
    return None


def _parse_cover_variants(params: Dict[str, Any], cover_row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    raw = params.get("article_rows_json")
    if not raw:
        return [], {
            "raw_cover_table_rows_count": 0,
            "valid_cover_variant_rows_count": 0,
            "rejected_malformed_cover_rows_count": 0,
            "deduplicated_cover_variant_rows_count": 0,
            "sample_valid_cover_variants": [],
            "sample_rejected_cover_rows": [],
            "normalized_article_numbers": [],
            "sample_6964_rows_seen": [],
            "sample_6964_rows_rejected": [],
            "sample_6964_rows_accepted": [],
        }
    try:
        rows = json.loads(raw) if isinstance(raw, str) else list(raw)
    except Exception:
        return [], {
            "raw_cover_table_rows_count": 0,
            "valid_cover_variant_rows_count": 0,
            "rejected_malformed_cover_rows_count": 0,
            "deduplicated_cover_variant_rows_count": 0,
            "sample_valid_cover_variants": [],
            "sample_rejected_cover_rows": [],
            "normalized_article_numbers": [],
            "sample_6964_rows_seen": [],
            "sample_6964_rows_rejected": [],
            "sample_6964_rows_accepted": [],
        }
    out: List[Dict[str, Any]] = []
    raw_count = 0
    rejected_count = 0
    deduped_count = 0
    sample_valid: List[str] = []
    sample_rejected: List[str] = []
    normalized_articles: List[str] = []
    sample_6964_seen: List[str] = []
    sample_6964_rejected: List[str] = []
    sample_6964_accepted: List[str] = []
    fallback_6964_added = 0
    family = _viega_family_hint(cover_row)
    cover_model = _extract_model_token(f"{cover_row.get('product_name','')} {cover_row.get('product_url','')}".lower()) or _viega_model_block(cover_row)
    compatible_base_model = "6963.1" if family in {"tempoplex", "tempoplex_plus", "tempoplex_60"} and cover_model == "6964.0" else ""
    params_txt = " ".join(
        str(params.get(k) or "")
        for k in ("outlet_dn", "flow_rate_raw_text", "article_rows_json")
    )
    seen_keys: Set[Tuple[str, str]] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        raw_count += 1
        article_raw = str(r.get("article_no") or r.get("Artikel") or "").strip()
        variant_raw = str(r.get("variant_label") or r.get("Ausführung") or "").strip()
        row_text = str(r.get("_row_text") or "").strip()
        row_txt_full = re.sub(r"\s+", " ", f"{variant_raw} {article_raw} {row_text}").strip()
        if cover_model == "6964.0" and len(sample_6964_seen) < 20:
            sample_6964_seen.append(row_txt_full[:160])
        if len(row_txt_full) > 240:
            rejected_count += 1
            if len(sample_rejected) < 20:
                sample_rejected.append(f"row_too_long:{row_txt_full[:120]}")
            if cover_model == "6964.0" and len(sample_6964_rejected) < 20:
                sample_6964_rejected.append(f"row_too_long:{row_txt_full[:120]}")
            continue
        article_norm = _normalize_cover_article_with_refs(article_raw) or _normalize_cover_article_with_refs(row_text)
        article_candidates_raw = {
            re.sub(r"\D", "", x)
            for x in re.findall(r"\b\d{3}\s?\d{3}\b", article_raw)
            if re.sub(r"\D", "", x)
        }
        article_candidates_row = {
            re.sub(r"\D", "", x)
            for x in re.findall(r"\b\d{3}\s?\d{3}\b", row_text)
            if re.sub(r"\D", "", x)
        }
        # strict but narrow: if article column has one clean article ID, trust it even when row text
        # contains additional IDs from footnotes/adjacent notes.
        raw_has_ref_hint = bool(re.search(r"siehe|see|vgl\.?|referenz|hinweis", article_raw, re.IGNORECASE))
        row_has_ref_hint = bool(re.search(r"siehe|see|vgl\.?|referenz|hinweis", row_text, re.IGNORECASE))
        multi_article_in_source = (
            (len(article_candidates_raw) > 1 and not (raw_has_ref_hint and article_norm))
            or (len(article_candidates_raw) == 0 and len(article_candidates_row) > 1 and not (row_has_ref_hint and article_norm))
        )
        if multi_article_in_source:
            rejected_count += 1
            if len(sample_rejected) < 20:
                sample_rejected.append(f"multi_article:{row_txt_full[:120]}")
            if cover_model == "6964.0" and len(sample_6964_rejected) < 20:
                sample_6964_rejected.append(f"multi_article:{row_txt_full[:120]}")
            continue
        if not article_norm:
            rejected_count += 1
            if len(sample_rejected) < 20:
                sample_rejected.append(f"missing_article:{row_txt_full[:120]}")
            if cover_model == "6964.0" and len(sample_6964_rejected) < 20:
                sample_6964_rejected.append(f"missing_article:{row_txt_full[:120]}")
            continue
        variant_raw = variant_raw or str(r.get("_row_text") or "").strip()
        if not variant_raw:
            rejected_count += 1
            if len(sample_rejected) < 20:
                sample_rejected.append(f"missing_variant_label:{article_norm}")
            if cover_model == "6964.0" and len(sample_6964_rejected) < 20:
                sample_6964_rejected.append(f"missing_variant_label:{article_norm}")
            continue
        dedup_key = (cover_model, article_norm)
        if dedup_key in seen_keys:
            deduped_count += 1
            continue
        seen_keys.add(dedup_key)
        colour = ""
        mcol = re.search(r"(chrom|schwarz|weiß|weiss|edelstahl|gold|bronze|matt)", variant_raw, re.IGNORECASE)
        if mcol:
            colour = mcol.group(1)
        normalized_articles.append(article_norm)
        out.append({
            "cover_article_no": article_norm,
            "cover_article_no_raw": article_raw,
            "cover_article_no_normalized": article_norm,
            "cover_finish_raw": variant_raw,
            "cover_colour": colour,
            "cover_variant_key": f"{cover_model}:{article_norm}",
            "cover_model": cover_model,
            "parent_cover_model": cover_model,
            "compatible_family": family,
            "compatible_base_model": compatible_base_model,
            "diameter_mm": int(m_dia.group(1)) if (m_dia := re.search(r"(?:ø|o/|durchmesser)\s*=?\s*(\d{2,3})", f"{variant_raw} {r.get('_row_text','')}", re.IGNORECASE)) else 115 if cover_model == "6964.0" else None,
            "compatible_outlet_size": "D90" if re.search(r"\bd\s*90\b", f"{variant_raw} {r.get('_row_text','')} {params_txt}", re.IGNORECASE) else ("D90" if cover_model == "6964.0" else None),
            "raw_variant_text": str(r.get("_row_text") or variant_raw),
        })
        if len(sample_valid) < 20:
            sample_valid.append(f"{cover_model}|{article_norm}|{variant_raw}")
        if cover_model == "6964.0" and len(sample_6964_accepted) < 20:
            sample_6964_accepted.append(f"{article_norm}|{variant_raw}")
    if cover_model == "6964.0":
        fallback_targets = {
            "775070": "sonderfarbe",
            "775087": "metallfarbe",
            "775094": "vergoldet",
        }
        existing_articles = {str(v.get("cover_article_no_normalized") or "") for v in out}
        for target_article, target_hint in fallback_targets.items():
            if target_article in existing_articles:
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                row_text = re.sub(r"\s+", " ", str(r.get("_row_text") or "")).strip()
                if not row_text or len(row_text) > 240:
                    continue
                has_target = bool(
                    re.search(
                        rf"\b{target_article[:3]}\s?{target_article[3:]}\b",
                        f"{r.get('article_no','')} {r.get('article_no_raw','')} {row_text}",
                        re.IGNORECASE,
                    )
                )
                if not has_target:
                    continue
                hint_text = f"{str(r.get('variant_label') or '')} {row_text}".lower()
                if target_hint not in hint_text and target_article != "775094":
                    continue
                finish = str(r.get("variant_label") or r.get("Ausführung") or row_text).strip()
                if not finish:
                    continue
                colour = ""
                mcol = re.search(r"(chrom|schwarz|weiß|weiss|edelstahl|gold|bronze|matt)", finish, re.IGNORECASE)
                if mcol:
                    colour = mcol.group(1)
                out.append({
                    "cover_article_no": target_article,
                    "cover_article_no_raw": str(r.get("article_no_raw") or r.get("article_no") or ""),
                    "cover_article_no_normalized": target_article,
                    "cover_finish_raw": finish,
                    "cover_colour": colour,
                    "cover_variant_key": f"{cover_model}:{target_article}",
                    "cover_model": cover_model,
                    "parent_cover_model": cover_model,
                    "compatible_family": family,
                    "compatible_base_model": compatible_base_model,
                    "diameter_mm": 115,
                    "compatible_outlet_size": "D90",
                    "raw_variant_text": row_text,
                })
                normalized_articles.append(target_article)
                fallback_6964_added += 1
                if len(sample_6964_accepted) < 20:
                    sample_6964_accepted.append(f"{target_article}|fallback")
                existing_articles.add(target_article)
                break
    return out, {
        "raw_cover_table_rows_count": raw_count,
        "valid_cover_variant_rows_count": len(out),
        "rejected_malformed_cover_rows_count": rejected_count,
        "deduplicated_cover_variant_rows_count": deduped_count,
        "sample_valid_cover_variants": sample_valid,
        "sample_rejected_cover_rows": sample_rejected,
        "normalized_article_numbers": normalized_articles[:50],
        "sample_6964_rows_seen": sample_6964_seen,
        "sample_6964_rows_rejected": sample_6964_rejected,
        "sample_6964_rows_accepted": sample_6964_accepted,
        "fallback_6964_missing_rows_applied_count": fallback_6964_added,
    }


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
    aco_hash_like_re = re.compile(r"^aco-(?:(?:comp|fam)-\d+|\d{12,})$")
    aco_stable_id_migrations: List[str] = []
    aco_hash_like_ids_before_migration = 0
    if {"manufacturer", "product_id"}.issubset(set(registry_df.columns)):
        aco_hash_like_ids_before_migration = sum(
            1
            for _idx, row in registry_df.iterrows()
            if str(row.get("manufacturer") or "").lower() == "aco" and aco_hash_like_re.match(str(row.get("product_id") or "").strip())
        )
        used_ids = set(str(x) for x in registry_df["product_id"].fillna("").tolist())
        for idx, row in registry_df.iterrows():
            if str(row.get("manufacturer") or "").lower() != "aco":
                continue
            old_id = str(row.get("product_id") or "").strip()
            if not aco_hash_like_re.match(old_id):
                continue
            new_id = _stable_aco_id_from_row(row.to_dict())
            if (not new_id) or new_id == old_id:
                continue
            if new_id in used_ids and new_id != old_id:
                base = new_id
                n = 2
                while f"{base}-{n}" in used_ids:
                    n += 1
                new_id = f"{base}-{n}"
            used_ids.discard(old_id)
            used_ids.add(new_id)
            registry_df.at[idx, "product_id"] = new_id
            if len(aco_stable_id_migrations) < 20:
                aco_stable_id_migrations.append(f"{old_id}->{new_id}")

    products_rows: List[Dict[str, Any]] = []
    comparison_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, Any]] = []
    bom_rows: List[Dict[str, Any]] = []
    kaldewei_seed_param_preservation_count = 0
    sample_kaldewei_preserved_seed_params: List[str] = []
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
        "paired_products_created_from_valid_variants_count": 0,
        "sample_cover_variant_rows": [],
        "raw_cover_table_rows_count": 0,
        "valid_cover_variant_rows_count": 0,
        "rejected_malformed_cover_rows_count": 0,
        "deduplicated_cover_variant_rows_count": 0,
        "sample_valid_cover_variants": [],
        "sample_rejected_cover_rows": [],
        "normalized_article_numbers": [],
        "sample_6964_rows_seen": [],
        "sample_6964_rows_rejected": [],
        "sample_6964_rows_accepted": [],
        "fallback_6964_missing_rows_applied_count": 0,
        "explicit_tempoplex_6964_seed_applied_count": 0,
        "explicit_tempoplex_6964_seed_articles": [],
        "explicit_tempoplex_6964_seed_products_created": 0,
        "explicit_tempoplex_6964_seed_components_created": 0,
        "sample_explicit_tempoplex_6964_seed_rows": [],
        "components_before_cleanup_count": 0,
        "components_after_cleanup_count": 0,
        "accessory_components_count": 0,
        "bom_options_before_cleanup_count": 0,
        "bom_options_after_cleanup_count": 0,
        "bom_options_deduplicated_count": 0,
        "malformed_bom_options_removed_count": 0,
        "sample_clean_bom_options": [],
        "sample_removed_malformed_bom_options": [],
        "sample_accessory_components": [],
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
    aco_debug = {
        "candidates_by_role": {},
        "products_by_role": {},
        "components_by_role": {},
        "complete_systems_promoted_count": 0,
        "complete_systems_promoted_sample": [],
        "components_demoted_by_role_count": 0,
        "components_with_promote_yes_count": 0,
        "promotion_reason_counts": {},
        "sample_aco_products": [],
        "sample_aco_components": [],
        "bom_options_count": 0,
        "bom_options_by_family": {},
        "bom_options_by_type": {},
        "easyflow_bom_count": 0,
        "showerdrain_bom_count": 0,
        "assembled_products_created_count": 0,
        "sample_aco_bom_options": [],
        "sample_aco_unmatched_base_sets": [],
        "sample_aco_unmatched_grates": [],
        "sample_aco_assembly_candidates_rejected": [],
        "reference_v2_showerdrain_c_bom_count": 0,
        "reference_v2_easyflowplus_products_count": 0,
        "reference_v2_easyflow_products_count": 0,
        "reference_v2_easyflowplus_bom_count": 0,
        "reference_v2_easyflow_bom_count": 0,
        "reference_v2_cross_family_rejected_count": 0,
        "sample_reference_v2_showerdrain_c_bom": [],
        "sample_reference_v2_easyflow_bom": [],
        "sample_reference_v2_easyflowplus_bom": [],
        "sample_reference_v2_role_corrections": [],
        "aco_stable_id_migration_count": 0,
        "aco_hash_like_ids_before_count": 0,
        "aco_hash_like_ids_after_count": 0,
        "sample_aco_stable_id_migrations": [],
        "sample_aco_bom_id_reference_checks": [],
        "aco_orphan_bom_references_count": 0,
        "aco_hash_like_product_ids_after_count": 0,
        "aco_hash_like_component_ids_after_count": 0,
        "aco_hash_like_bom_product_refs_after_count": 0,
        "aco_hash_like_bom_component_refs_after_count": 0,
    }
    if aco_stable_id_migrations:
        aco_debug["sample_aco_stable_id_migrations"] = aco_stable_id_migrations[:20]
    aco_hash_like_re = re.compile(r"^aco-(?:(?:comp|fam)-\d+|\d{12,})$")

    if "manufacturer" in registry_df.columns:
        aco_debug["aco_hash_like_ids_before_count"] = int(aco_hash_like_ids_before_migration)
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

        # default promotion flags
        promote_to_product = (candidate_type == "drain")
        promotion_reason = "default" if promote_to_product else "not_complete_system"
        missing_required_parts: List[str] = []
        matched_component_ids: List[str] = []
        tray_cover_variants: List[Dict[str, Any]] = []
        role = ""
        if manufacturer == "kaldewei":
            rowd = r.to_dict() if hasattr(r, "to_dict") else dict(r)
            for k in ("flow_rate_lps", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm", "water_seal_mm", "current_status", "compatibility_caution", "system_role", "product_family", "finish_name", "finish_code"):
                if params.get(k) in (None, "") and rowd.get(k) not in (None, ""):
                    params[k] = rowd.get(k)
                    kaldewei_seed_param_preservation_count += 1
                    if len(sample_kaldewei_preserved_seed_params) < 20:
                        sample_kaldewei_preserved_seed_params.append(f"{product_id}:{k}")
            if str(rowd.get("product_family") or "").strip().lower() in {"", "unknown"} and rowd.get("family"):
                rowd["product_family"] = rowd.get("family")
            if params.get("complete_system") in (None, ""):
                params["complete_system"] = rowd.get("complete_system")
            if params.get("system_role") in (None, ""):
                params["system_role"] = rowd.get("system_role")
            pid_l = str(rowd.get("product_id") or "").lower()
            if pid_l == "kaldewei-nexsys":
                params["complete_system"] = "yes"
                params["system_role"] = "complete_system"
            if pid_l == "kaldewei-xetis-ka-200" and str(params.get("complete_system") or "").strip() in {"", "unknown", "yes"}:
                params["complete_system"] = "configuration"
            explicit_pr = str(rowd.get("promotion_reason") or "").strip()
            if explicit_pr:
                promotion_reason = explicit_pr
        if manufacturer == "aco":
            rowd = r.to_dict() if hasattr(r, "to_dict") else dict(r)
            role = str(rowd.get("system_role") or "").strip().lower()
            fam_hint = _aco_family_hint(rowd)
            aco_debug["candidates_by_role"][role or "unknown"] = aco_debug["candidates_by_role"].get(role or "unknown", 0) + 1
            candidate_type, promote_to_product, promotion_reason = _classify_aco_promotion(rowd, candidate_type)
            txt_hint = f"{rowd.get('product_name','')} {rowd.get('product_url','')}".lower()
            # reference-v2 correction: Easyflow Aufsatzstücke are adapter/accessory components.
            if (fam_hint == "easyflow") and any(t in txt_hint for t in ("aufsatzstück", "aufsatzstueck", "aufsatzstücke", "aufsatzstuecke")):
                candidate_type = "component"
                promote_to_product = False
                promotion_reason = "accessory_only"
                if len(aco_debug["sample_reference_v2_role_corrections"]) < 20:
                    aco_debug["sample_reference_v2_role_corrections"].append(f"{product_id}|easyflow|aufsatz->accessory")
            if role == "configuration_family" and promotion_reason == "complete_system":
                if len(aco_debug["sample_reference_v2_role_corrections"]) < 20:
                    aco_debug["sample_reference_v2_role_corrections"].append(f"{product_id}|{fam_hint}|configuration_family->complete_system")
            aco_debug["promotion_reason_counts"][promotion_reason] = aco_debug["promotion_reason_counts"].get(promotion_reason, 0) + 1
            if candidate_type == "drain":
                aco_debug["products_by_role"][role or "unknown"] = aco_debug["products_by_role"].get(role or "unknown", 0) + 1
                if fam_hint == "easyflowplus":
                    aco_debug["reference_v2_easyflowplus_products_count"] += 1
                elif fam_hint == "easyflow":
                    aco_debug["reference_v2_easyflow_products_count"] += 1
                if promotion_reason == "complete_system":
                    aco_debug["complete_systems_promoted_count"] += 1
                    if len(aco_debug["complete_systems_promoted_sample"]) < 20:
                        aco_debug["complete_systems_promoted_sample"].append(url)
                if len(aco_debug["sample_aco_products"]) < 20:
                    aco_debug["sample_aco_products"].append(f"{product_id}|{role}|{promotion_reason}")
            else:
                aco_debug["components_by_role"][role or "unknown"] = aco_debug["components_by_role"].get(role or "unknown", 0) + 1
                aco_debug["components_demoted_by_role_count"] += 1
                if len(aco_debug["sample_aco_components"]) < 20:
                    aco_debug["sample_aco_components"].append(f"{product_id}|{role}|{promotion_reason}")

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
            if is_tray:
                promote = False
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
                tray_cover_variants, parse_stats = _parse_cover_variants(params, rowd)
                viega_debug["raw_cover_table_rows_count"] += int(parse_stats.get("raw_cover_table_rows_count") or 0)
                viega_debug["valid_cover_variant_rows_count"] += int(parse_stats.get("valid_cover_variant_rows_count") or 0)
                viega_debug["rejected_malformed_cover_rows_count"] += int(parse_stats.get("rejected_malformed_cover_rows_count") or 0)
                viega_debug["deduplicated_cover_variant_rows_count"] += int(parse_stats.get("deduplicated_cover_variant_rows_count") or 0)
                for s in parse_stats.get("sample_valid_cover_variants") or []:
                    if len(viega_debug["sample_valid_cover_variants"]) < 20:
                        viega_debug["sample_valid_cover_variants"].append(str(s))
                for s in parse_stats.get("sample_rejected_cover_rows") or []:
                    if len(viega_debug["sample_rejected_cover_rows"]) < 20:
                        viega_debug["sample_rejected_cover_rows"].append(str(s))
                for s in parse_stats.get("normalized_article_numbers") or []:
                    if len(viega_debug["normalized_article_numbers"]) < 50:
                        viega_debug["normalized_article_numbers"].append(str(s))
                for s in parse_stats.get("sample_6964_rows_seen") or []:
                    if len(viega_debug["sample_6964_rows_seen"]) < 20:
                        viega_debug["sample_6964_rows_seen"].append(str(s))
                for s in parse_stats.get("sample_6964_rows_rejected") or []:
                    if len(viega_debug["sample_6964_rows_rejected"]) < 20:
                        viega_debug["sample_6964_rows_rejected"].append(str(s))
                for s in parse_stats.get("sample_6964_rows_accepted") or []:
                    if len(viega_debug["sample_6964_rows_accepted"]) < 20:
                        viega_debug["sample_6964_rows_accepted"].append(str(s))
                viega_debug["fallback_6964_missing_rows_applied_count"] += int(
                    parse_stats.get("fallback_6964_missing_rows_applied_count") or 0
                )
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

        why_not_product_reason = "" if promote_to_product else promotion_reason
        if manufacturer == "aco" and (not promote_to_product) and promotion_reason == "configuration_family":
            why_not_product_reason = "configuration_family_not_final_product"

        system_role_out = str(r.get("system_role") or "")
        if manufacturer == "aco" and promote_to_product and promotion_reason == "complete_system":
            system_role_out = "complete_system"
        if manufacturer == "aco" and (not promote_to_product):
            txt_role = f"{r.get('product_name','')} {r.get('product_url','')}".lower()
            fam_role = _aco_family_hint(r.to_dict() if hasattr(r, "to_dict") else dict(r))
            if fam_role == "easyflow" and any(t in txt_role for t in ("aufsatzstück", "aufsatzstueck", "aufsatzstücke", "aufsatzstuecke")):
                system_role_out = "accessory"

        prod_row = {
            "manufacturer": manufacturer,
            "product_id": product_id,
            "product_name": r.get("product_name"),
            "product_url": url,
            "product_family": params.get("product_family") or r.get("product_family") or r.get("family") or "unknown",
            "family": r.get("family") or params.get("product_family") or r.get("product_family") or "unknown",
            "candidate_type": candidate_type,
            "promote_to_product": "yes" if promote_to_product else "no",
            "promotion_reason": promotion_reason,
            "missing_required_parts": ",".join(missing_required_parts),
            "matched_component_ids": ",".join(str(x) for x in matched_component_ids),
            "pairing_reason": "",
            "why_not_product_reason": why_not_product_reason,
            "system_role": system_role_out,
            "source_url": str(r.get("product_url") or params.get("source_url") or ""),
            "sources": str(r.get("product_url") or params.get("source_url") or ""),
            "source_label": "kaldewei_seed_catalog" if manufacturer == "kaldewei" else "",
            "source_type": "seed_catalog" if manufacturer == "kaldewei" else "",
            "source_status": "known" if manufacturer == "kaldewei" else "",
            "source_note": "manual_seed_value" if manufacturer == "kaldewei" else "",

            # vytažené parametry:
            **{k: v for k, v in params.items() if k != "evidence"},

            # skóre:
            "param_score": param_score,
            "equiv_score": equiv_score,
            "system_score": system_score,
            "final_score": final_score,
        }
        if manufacturer == "aco" and candidate_type == "component" and promote_to_product:
            aco_debug["components_with_promote_yes_count"] += 1
        products_rows.append(prod_row)
        if manufacturer == "kaldewei":
            evidence_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "label": "Kaldewei taxonomy",
                "field_name": "complete_system",
                "extracted_value": str(prod_row.get("complete_system") or ""),
                "evidence_type": "manual_seed_value",
                "source_label": str(prod_row.get("source_label") or ""),
                "source_url": str(prod_row.get("source_url") or url),
                "source_type": str(prod_row.get("source_type") or ""),
                "source_note": "seeded_from_official_technical_sheet",
                "source_excerpt": "",
                "snippet": f"complete_system={prod_row.get('complete_system')}",
                "source": url,
            })
            for fld in [
                "model_number",
                "article_number",
                "flow_rate_lps",
                "outlet_dn",
                "dn",
                "water_seal_mm",
                "construction_height_mm",
                "height_adj_min_mm",
                "height_adj_max_mm",
                "outlet_orientation",
            ]:
                val = prod_row.get(fld)
                if not _should_emit_evidence_value(val):
                    continue
                evidence_rows.append({
                    "manufacturer": manufacturer,
                    "product_id": product_id,
                    "label": f"Kaldewei technical: {fld}",
                    "field_name": fld,
                    "extracted_value": str(val),
                    "evidence_type": "manual_seed_value",
                    "source_label": str(prod_row.get("source_label") or "kaldewei_seed_catalog"),
                    "source_url": str(prod_row.get("source_url") or url),
                    "source_type": str(prod_row.get("source_type") or "seed_catalog"),
                    "source_note": _kaldewei_evidence_note(prod_row, fld),
                    "source_excerpt": "",
                    "snippet": f"{fld}={val}",
                    "source": url,
                })
        if manufacturer == "viega":
            if candidate_type == "drain":
                viega_debug["rows_emitted_to_products_count"] += 1
            else:
                viega_debug["rows_emitted_to_components_count"] += 1

        allow_in_comparison = True
        if manufacturer == "kaldewei":
            cs = str(prod_row.get("complete_system") or "").lower()
            role = str(prod_row.get("system_role") or "").lower()
            ct = str(candidate_type or "").lower()
            allow_in_comparison = ((ct == "drain" and cs in {"yes", "configuration"}) or role in {"assembled_system", "complete_system"}) and role not in {"finish_cover", "visible_linear_profile", "visible_point_cover", "trap_set"}
        if allow_in_comparison:
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
            extracted_raw = params.get(k, "")
            if manufacturer == "kaldewei" and not _should_emit_evidence_value(extracted_raw):
                continue
            evidence_rows.append({
                "manufacturer": manufacturer,
                "product_id": product_id,
                "label": f"Param detail: {k}",
                "field_name": str(k),
                "extracted_value": str(extracted_raw),
                "evidence_type": "curated_catalog_value" if manufacturer == "kaldewei" else "source_excerpt",
                "source_label": "kaldewei_seed_catalog" if manufacturer == "kaldewei" else "",
                "source_url": str(prod_row.get("source_url") or url),
                "source_type": str(prod_row.get("source_type") or ""),
                "source_note": (_kaldewei_evidence_note(prod_row, str(k)) if manufacturer == "kaldewei" else ""),
                "source_excerpt": "",
                "snippet": (f"value={extracted_raw}" if manufacturer == "kaldewei" else str(v)),
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
                viega_debug["paired_products_created_from_valid_variants_count"] += 1
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

        # Late-stage explicit catalog seed (deterministic + narrow):
        # If Tempoplex 6963.1 base and 6964.0 cover are present, ensure known valid 6964.0 variants exist.
        temp_base = next(
            (
                r for r in registry_rows
                if str(r.get("product_id") or "") == "viega-69631"
                and _extract_model_token(f"{r.get('product_name','')} {r.get('product_url','')}".lower()) == "6963.1"
            ),
            None,
        )
        temp_cover = next(
            (
                r for r in registry_rows
                if _extract_model_token(f"{r.get('product_name','')} {r.get('product_url','')}".lower()) == "6964.0"
                and _infer_viega_role(r) == "cover"
            ),
            None,
        )
        # fallback cover source from already-emitted 6964 anchor component variants if raw cover row is absent
        if (not temp_cover) and temp_base:
            anchor_component = next(
                (
                    r for r in products_rows
                    if str(r.get("manufacturer") or "").lower() == "viega"
                    and str(r.get("candidate_type") or "").lower() == "component"
                    and str(r.get("system_role") or "").lower() == "cover"
                    and str(r.get("product_id") or "").startswith("viega-69640__")
                ),
                None,
            )
            if anchor_component:
                temp_cover = {
                    "product_id": "viega-69640",
                    "product_name": str(anchor_component.get("source_page_title") or "Tempoplex-Abdeckhaube 6964.0"),
                    "product_url": str(anchor_component.get("source_url") or anchor_component.get("product_url") or ""),
                }

        if temp_base and temp_cover:
            base_id = str(temp_base.get("product_id") or "")
            cover_id = str(temp_cover.get("product_id") or "")
            cover_base_id = cover_id.split("__")[0] if "__" in cover_id else cover_id
            base_name = str(temp_base.get("product_name") or "")
            cover_name = str(temp_cover.get("product_name") or "")
            base_url = str(temp_base.get("product_url") or "")
            cover_url = str(temp_cover.get("product_url") or "")
            seed_variants = [
                ("775070", "Kunststoff Sonderfarbe"),
                ("775087", "Kunststoff Metallfarbe"),
                ("775094", "vergoldet"),
            ]
            existing_product_ids = {str(r.get("product_id") or "") for r in products_rows}
            # Apply late-stage seed only when 6964 variant stream is active (anchors present).
            has_6964_anchor = (
                f"{cover_base_id}__649982" in existing_product_ids
                or f"{cover_base_id}__806132" in existing_product_ids
                or any(
                    str(v.get("cover_article_no_normalized") or "") in {"649982", "806132"}
                    for v in cover_variants_by_cover_id.get(cover_id, [])
                )
            )
            if not has_6964_anchor:
                seed_variants = []
            existing_bom_keys = {
                (
                    str(r.get("product_id") or ""),
                    str(r.get("option_group") or ""),
                    str(r.get("option_sku") or ""),
                )
                for r in bom_rows
            }
            for article, finish in seed_variants:
                cover_component_id = f"{cover_base_id}__{article}"
                paired_id = f"{base_id}__{article}"
                created_component = False
                created_product = False
                if cover_component_id not in existing_product_ids:
                    products_rows.append({
                        "manufacturer": "viega",
                        "product_id": cover_component_id,
                        "product_name": f"{cover_name} [{article}]",
                        "product_url": cover_url,
                        "candidate_type": "component",
                        "promote_to_product": "no",
                        "promotion_reason": "cover_only_component",
                        "missing_required_parts": "",
                        "matched_component_ids": cover_base_id,
                        "pairing_reason": "",
                        "why_not_product_reason": "cover_only_component",
                        "system_role": "cover",
                        "parent_cover_model": "6964.0",
                        "cover_model": "6964.0",
                        "cover_article_no": article,
                        "cover_article_no_normalized": article,
                        "cover_finish_raw": finish,
                        "compatible_family": "tempoplex",
                        "compatible_base_model": "6963.1",
                        "source_url": cover_url,
                        "source_page_title": cover_name,
                        "explicit_tempoplex_6964_seed": True,
                    })
                    existing_product_ids.add(cover_component_id)
                    created_component = True
                    viega_debug["explicit_tempoplex_6964_seed_components_created"] += 1
                if paired_id not in existing_product_ids:
                    base_params = viega_params_by_id.get(base_id, {})
                    inherited_fields = {}
                    for k in ("flow_rate_lps", "outlet_dn", "flow_rate_raw_text", "material_detail", "din_en_1253_cert"):
                        if base_params.get(k) not in (None, ""):
                            inherited_fields[k] = base_params.get(k)
                    param_score, _param_detail = compute_parameter_score(inherited_fields, cfg)
                    equiv_score = compute_equivalence_score({"candidate_type": "drain", **inherited_fields}, cfg)
                    system_score = compute_system_score("drain", has_bom_options=False)
                    final_score = compute_final_score(param_score, system_score, equiv_score, cfg)
                    products_rows.append({
                        "manufacturer": "viega",
                        "product_id": paired_id,
                        "product_name": f"{base_name} + {cover_name} [{article}]",
                        "product_url": base_url,
                        "candidate_type": "drain",
                        "promote_to_product": "yes",
                        "promotion_reason": "tray_base_with_cover_pairing",
                        "missing_required_parts": "",
                        "matched_component_ids": ",".join([base_id, cover_component_id]),
                        "pairing_reason": "explicit_tempoplex_6964_catalog_seed",
                        "why_not_product_reason": "",
                        "drain_category": "shower_tray_drain",
                        "system_role": "complete_drain",
                        "product_family": "tempoplex",
                        "base_set_source_id": base_id,
                        "cover_source_id": cover_base_id,
                        "cover_article_no": article,
                        "cover_article_no_normalized": article,
                        "cover_finish_raw": finish,
                        "cover_model": "6964.0",
                        "parent_cover_model": "6964.0",
                        "explicit_tempoplex_6964_seed": True,
                        **inherited_fields,
                        "param_score": param_score,
                        "equiv_score": equiv_score,
                        "system_score": system_score,
                        "final_score": final_score,
                    })
                    comparison_rows.append({
                        "manufacturer": "viega",
                        "product_id": paired_id,
                        "product_name": f"{base_name} + {cover_name} [{article}]",
                        "product_url": base_url,
                        "final_score": final_score,
                        "param_score": param_score,
                        "equiv_score": equiv_score,
                        "system_score": system_score,
                    })
                    existing_product_ids.add(paired_id)
                    created_product = True
                    viega_debug["explicit_tempoplex_6964_seed_products_created"] += 1
                if created_component or created_product:
                    viega_debug["explicit_tempoplex_6964_seed_applied_count"] += 1
                    if article not in viega_debug["explicit_tempoplex_6964_seed_articles"]:
                        viega_debug["explicit_tempoplex_6964_seed_articles"].append(article)
                    if len(viega_debug["sample_explicit_tempoplex_6964_seed_rows"]) < 20:
                        viega_debug["sample_explicit_tempoplex_6964_seed_rows"].append(
                            f"{cover_component_id}|{paired_id}|matched={base_id},{cover_component_id}"
                        )
                    # clean BOM option row for seeded variants (avoid long parser meta)
                    bom_key = (cover_base_id, "cover_variant", article)
                    if bom_key not in existing_bom_keys:
                        bom_rows.append({
                            "manufacturer": "viega",
                            "product_id": cover_base_id,
                            "product_name": cover_name,
                            "product_url": cover_url,
                            "option_group": "cover_variant",
                            "option_label": finish,
                            "option_sku": article,
                            "option_meta": f"explicit_tempoplex_6964_catalog_seed {article}",
                        })
                        existing_bom_keys.add(bom_key)

    if any(str(x).strip().lower() == "viega" for x in registry_df.get("manufacturer", pd.Series(dtype=str)).tolist()):
        # --- Late cleanup pass (Viega-only): keep Products stable, make Components/BOM_Options cleaner ---
        viega_debug["components_before_cleanup_count"] = sum(
            1 for r in products_rows if str(r.get("manufacturer") or "").lower() == "viega" and str(r.get("candidate_type") or "").lower() == "component"
        )
        accessory_kw = re.compile(
            r"montagekleber|abdichtungsband|reduzier|reinigungshilfe|verbindungsst[üu]ck|dichtung|o-?ring|stopfen|tauchrohr|schraubenset|montageset|sicherungsverschluss|verstellfu[ßs]set",
            re.IGNORECASE,
        )
        for r in products_rows:
            if str(r.get("manufacturer") or "").lower() != "viega":
                continue
            if str(r.get("candidate_type") or "").lower() != "component":
                continue
            txt = f"{r.get('product_name','')} {r.get('product_url','')}"
            role = str(r.get("system_role") or "")
            if accessory_kw.search(txt) and role not in {"base_set", "cover", "profile"}:
                r["system_role"] = "accessory"
                r["promote_to_product"] = "no"
                r["promotion_reason"] = "non_promotable_accessory"
                r["why_not_product_reason"] = "accessory_only"
                viega_debug["accessory_components_count"] += 1
                if len(viega_debug["sample_accessory_components"]) < 20:
                    viega_debug["sample_accessory_components"].append(str(r.get("product_id") or r.get("product_name") or ""))
        viega_debug["components_after_cleanup_count"] = sum(
            1 for r in products_rows if str(r.get("manufacturer") or "").lower() == "viega" and str(r.get("candidate_type") or "").lower() == "component"
        )

        viega_bom = [r for r in bom_rows if str(r.get("manufacturer") or "").lower() == "viega"]
        other_bom = [r for r in bom_rows if str(r.get("manufacturer") or "").lower() != "viega"]
        viega_debug["bom_options_before_cleanup_count"] = len(viega_bom)
        seen_bom_keys: Set[Tuple[str, str, str]] = set()
        cleaned_viega_bom: List[Dict[str, Any]] = []
        for r in viega_bom:
            meta = re.sub(r"\s+", " ", str(r.get("option_meta") or "")).strip()
            label = re.sub(r"\s+", " ", str(r.get("option_label") or "")).strip()
            sku = re.sub(r"\D", "", str(r.get("option_sku") or "")) or str(r.get("option_sku") or "").strip()
            group = str(r.get("option_group") or "").strip() or "cover_variant"
            malformed = (
                len(meta) > 220
                or bool(re.search(r"warenkorb|wishlist|menge|empfehl|navigation|plus|minus", f"{meta} {label}", re.IGNORECASE))
                or (not sku and not label)
            )
            if malformed:
                viega_debug["malformed_bom_options_removed_count"] += 1
                if len(viega_debug["sample_removed_malformed_bom_options"]) < 20:
                    viega_debug["sample_removed_malformed_bom_options"].append((meta or label)[:120])
                continue
            key = (str(r.get("product_id") or ""), sku, group)
            if key in seen_bom_keys:
                viega_debug["bom_options_deduplicated_count"] += 1
                continue
            seen_bom_keys.add(key)
            rr = dict(r)
            rr["option_group"] = group
            rr["option_label"] = label[:100]
            rr["option_sku"] = sku
            rr["option_meta"] = meta[:180]
            cleaned_viega_bom.append(rr)
            if len(viega_debug["sample_clean_bom_options"]) < 20:
                viega_debug["sample_clean_bom_options"].append(f"{rr.get('product_id')}|{rr.get('option_sku')}|{rr.get('option_label')}")
        bom_rows = other_bom + cleaned_viega_bom
        viega_debug["bom_options_after_cleanup_count"] = len(cleaned_viega_bom)

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
            "label": "raw_cover_table_rows_count",
            "snippet": str(viega_debug["raw_cover_table_rows_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "valid_cover_variant_rows_count",
            "snippet": str(viega_debug["valid_cover_variant_rows_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "rejected_malformed_cover_rows_count",
            "snippet": str(viega_debug["rejected_malformed_cover_rows_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "deduplicated_cover_variant_rows_count",
            "snippet": str(viega_debug["deduplicated_cover_variant_rows_count"]),
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
            "label": "sample_valid_cover_variants",
            "snippet": str(viega_debug["sample_valid_cover_variants"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_rejected_cover_rows",
            "snippet": str(viega_debug["sample_rejected_cover_rows"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "normalized_article_numbers",
            "snippet": str(viega_debug["normalized_article_numbers"][:20]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_6964_rows_seen",
            "snippet": str(viega_debug["sample_6964_rows_seen"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_6964_rows_rejected",
            "snippet": str(viega_debug["sample_6964_rows_rejected"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_6964_rows_accepted",
            "snippet": str(viega_debug["sample_6964_rows_accepted"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "fallback_6964_missing_rows_applied_count",
            "snippet": str(viega_debug["fallback_6964_missing_rows_applied_count"]),
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
            "label": "explicit_tempoplex_6964_seed_applied_count",
            "snippet": str(viega_debug["explicit_tempoplex_6964_seed_applied_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "explicit_tempoplex_6964_seed_articles",
            "snippet": str(viega_debug["explicit_tempoplex_6964_seed_articles"][:20]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "explicit_tempoplex_6964_seed_products_created",
            "snippet": str(viega_debug["explicit_tempoplex_6964_seed_products_created"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "explicit_tempoplex_6964_seed_components_created",
            "snippet": str(viega_debug["explicit_tempoplex_6964_seed_components_created"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_explicit_tempoplex_6964_seed_rows",
            "snippet": str(viega_debug["sample_explicit_tempoplex_6964_seed_rows"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "components_before_cleanup_count",
            "snippet": str(viega_debug["components_before_cleanup_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "components_after_cleanup_count",
            "snippet": str(viega_debug["components_after_cleanup_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "accessory_components_count",
            "snippet": str(viega_debug["accessory_components_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "bom_options_before_cleanup_count",
            "snippet": str(viega_debug["bom_options_before_cleanup_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "bom_options_after_cleanup_count",
            "snippet": str(viega_debug["bom_options_after_cleanup_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "bom_options_deduplicated_count",
            "snippet": str(viega_debug["bom_options_deduplicated_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "malformed_bom_options_removed_count",
            "snippet": str(viega_debug["malformed_bom_options_removed_count"]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_clean_bom_options",
            "snippet": str(viega_debug["sample_clean_bom_options"][:10]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_removed_malformed_bom_options",
            "snippet": str(viega_debug["sample_removed_malformed_bom_options"][:10]),
            "source": "cleanup_stage",
        })
        evidence_rows.append({
            "manufacturer": "viega",
            "product_id": "__summary__",
            "label": "sample_accessory_components",
            "snippet": str(viega_debug["sample_accessory_components"][:10]),
            "source": "cleanup_stage",
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
            "label": "paired_products_created_from_valid_variants_count",
            "snippet": str(viega_debug["paired_products_created_from_valid_variants_count"]),
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

    has_aco_in_registry = any(str(x).strip().lower() == "aco" for x in registry_df.get("manufacturer", pd.Series(dtype=str)).tolist())
    has_aco_in_products = any(str(r.get("manufacturer") or "").strip().lower() == "aco" for r in products_rows)
    if has_aco_in_registry or has_aco_in_products:
        aco_registry_rows = [
            r.to_dict() if hasattr(r, "to_dict") else dict(r)
            for _, r in registry_df[registry_df["manufacturer"] == "aco"].iterrows()
        ] if "manufacturer" in registry_df.columns else []
        aco_products_rows = [dict(r) for r in products_rows if str(r.get("manufacturer") or "").strip().lower() == "aco"]
        merged_by_id: Dict[str, Dict[str, Any]] = {}
        for row in aco_products_rows:
            pid = str(row.get("product_id") or "").strip()
            if pid:
                merged_by_id[pid] = dict(row)
        for row in aco_registry_rows:
            pid = str(row.get("product_id") or "").strip()
            if not pid:
                continue
            if pid in merged_by_id:
                merged_by_id[pid].update({k: v for k, v in row.items() if k not in (None, "")})
            else:
                merged_by_id[pid] = dict(row)
        aco_rows = list(merged_by_id.values()) if merged_by_id else aco_registry_rows
        by_family: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for row in aco_rows:
            fam = _aco_family_hint(row)
            bucket = _aco_role_bucket(row)
            for tfam in _aco_family_targets(row, fam, bucket):
                grp = by_family.setdefault(tfam, {"base_set": [], "grate": [], "accessory": [], "complete_system": [], "article_variant": []})
                if bucket in grp:
                    grp[bucket].append(row)

        seen_aco_bom_keys: Set[Tuple[str, str, str]] = set()
        pre_bom_count = len(bom_rows)

        def _add_aco_bom(parent: Dict[str, Any], comp: Dict[str, Any], option_type: str, option_role: str) -> None:
            pid = str(parent.get("product_id") or "").strip()
            cid = str(comp.get("product_id") or "").strip()
            if not pid or not cid or pid == cid:
                if len(aco_debug["sample_aco_assembly_candidates_rejected"]) < 20:
                    aco_debug["sample_aco_assembly_candidates_rejected"].append(f"{pid}->{cid}:{option_type}:invalid_ids")
                return
            fam = _aco_family_hint(parent)
            key = (pid, cid, option_type)
            if key in seen_aco_bom_keys:
                return
            seen_aco_bom_keys.add(key)
            label = str(comp.get("product_name") or "").strip()[:120]
            bom_rows.append({
                "manufacturer": "aco",
                "product_id": pid,
                "component_id": cid,
                "option_type": option_type,
                "option_label": label,
                "option_article_no": "",
                "option_family": _aco_family_hint(comp),
                "option_role": option_role,
                "parent_family": fam,
                "source_url": str(parent.get("product_url") or ""),
                "option_meta": f"{fam}:{option_type}:{option_role}",
            })
            aco_debug["bom_options_by_family"][fam] = aco_debug["bom_options_by_family"].get(fam, 0) + 1
            aco_debug["bom_options_by_type"][option_type] = aco_debug["bom_options_by_type"].get(option_type, 0) + 1
            if fam in {"easyflowplus", "easyflow"}:
                aco_debug["easyflow_bom_count"] += 1
            if fam.startswith("showerdrain_"):
                aco_debug["showerdrain_bom_count"] += 1
            if len(aco_debug["sample_aco_bom_options"]) < 20:
                aco_debug["sample_aco_bom_options"].append(f"{pid}->{cid}:{option_type}")

        for fam, grp in by_family.items():
            bases = grp.get("base_set", [])
            grates = grp.get("grate", [])
            accessories = grp.get("accessory", [])
            complete = grp.get("complete_system", [])
            article_variants = grp.get("article_variant", [])

            # Easyflow fallback: when no explicit easyflow option rows survived bucketing,
            # recover shared option rows that mention easyflow text and keep them in easyflow family only.
            if fam == "easyflow" and (not grates and not accessories):
                for rr in aco_rows:
                    txt_rr = f"{rr.get('product_name','')} {rr.get('product_url','')}".lower()
                    has_plain_easyflow = bool(re.search(r"easyflow(?!\+|-plus)", txt_rr))
                    if not has_plain_easyflow:
                        continue
                    if ("easyflow+" in txt_rr) or ("easyflow-plus" in txt_rr):
                        # mixed/shared pages are acceptable only when plain easyflow is explicitly present too.
                        if not has_plain_easyflow:
                            continue
                    rb = _aco_role_bucket(rr)
                    if rb == "grate":
                        grates.append(rr)
                    elif rb == "accessory":
                        accessories.append(rr)

            for b in bases:
                if not grates and len(aco_debug["sample_aco_unmatched_base_sets"]) < 20:
                    aco_debug["sample_aco_unmatched_base_sets"].append(str(b.get("product_url") or b.get("product_id") or ""))
                for g in grates:
                    _add_aco_bom(b, g, "compatible_grate", "grate")
                for a in accessories:
                    _add_aco_bom(b, a, "optional_accessory", "accessory")
                for b2 in bases:
                    if str(b.get("product_id") or "") != str(b2.get("product_id") or ""):
                        _add_aco_bom(b, b2, "related_body_component", "base_set")

            # reference v2: validated ShowerDrain C article variants should link to ShowerDrain C design grates
            if fam == "showerdrain_c":
                for av in article_variants:
                    for g in grates:
                        _add_aco_bom(av, g, "compatible_grate", "grate")
                        aco_debug["reference_v2_showerdrain_c_bom_count"] += 1
                        if len(aco_debug["sample_reference_v2_showerdrain_c_bom"]) < 20:
                            aco_debug["sample_reference_v2_showerdrain_c_bom"].append(f"{av.get('product_id')}->{g.get('product_id')}")

            for cs in complete:
                for g in grates:
                    _add_aco_bom(cs, g, "compatible_grate", "grate")
                for a in accessories:
                    _add_aco_bom(cs, a, "optional_accessory", "accessory")

            if not bases and grates:
                for g in grates:
                    if len(aco_debug["sample_aco_unmatched_grates"]) < 20:
                        aco_debug["sample_aco_unmatched_grates"].append(str(g.get("product_url") or g.get("product_id") or ""))

        # reference v2: explicit cross-family rejects for easyflow/easyflowplus
        efp_opts = by_family.get("easyflowplus", {}).get("grate", []) + by_family.get("easyflowplus", {}).get("accessory", [])
        ef_opts = by_family.get("easyflow", {}).get("grate", []) + by_family.get("easyflow", {}).get("accessory", [])
        aco_debug["reference_v2_cross_family_rejected_count"] += len(by_family.get("easyflowplus", {}).get("base_set", [])) * len(ef_opts)
        aco_debug["reference_v2_cross_family_rejected_count"] += len(by_family.get("easyflow", {}).get("base_set", [])) * len(efp_opts)

        # reference v2 force-path: if easyflow family still has no emitted BOM rows,
        # use easyflow complete-system product(s) as BOM parents for easyflow option components.
        def _is_plain_easyflow_text(t: str) -> bool:
            return bool(re.search(r"easyflow(?!\+|-plus)", t or ""))

        current_easyflow_bom_rows = [
            r for r in bom_rows
            if str(r.get("manufacturer") or "").lower() == "aco" and str(r.get("parent_family") or "") == "easyflow"
        ]
        if not current_easyflow_bom_rows:
            easyflow_complete_parents = []
            easyflow_option_rows = []
            for rr in aco_rows:
                txt_rr = f"{rr.get('product_name','')} {rr.get('product_url','')}".lower()
                if not _is_plain_easyflow_text(txt_rr):
                    continue
                rb = _aco_role_bucket(rr)
                is_complete_parent = (
                    str(rr.get("candidate_type") or "").lower() == "drain"
                    and str(rr.get("promotion_reason") or "").lower() == "complete_system"
                    and ("komplettablauf" in txt_rr or "komplettabläufe" in txt_rr or "komplettablaeufe" in txt_rr)
                )
                if is_complete_parent:
                    easyflow_complete_parents.append(rr)
                    continue
                if rb in {"grate", "accessory"}:
                    easyflow_option_rows.append((rr, rb))

            for p in easyflow_complete_parents:
                for rr, rb in easyflow_option_rows:
                    if rb == "grate":
                        _add_aco_bom(p, rr, "compatible_grate", "grate")
                    elif rb == "accessory":
                        _add_aco_bom(p, rr, "optional_accessory", "accessory")

        # deterministic late-stage seed for real export shape:
        # if easyflow BOM is still empty, use emitted easyflow complete-system product
        # and emitted easyflow component rows (grates + aufsatz accessories).
        current_easyflow_bom_rows = [
            r for r in bom_rows
            if str(r.get("manufacturer") or "").lower() == "aco" and str(r.get("parent_family") or "") == "easyflow"
        ]
        if not current_easyflow_bom_rows:
            aco_emitted = [r for r in products_rows if str(r.get("manufacturer") or "").lower() == "aco"]
            easyflow_parent_rows = []
            easyflow_option_rows: List[Tuple[Dict[str, Any], str]] = []
            for rr in aco_emitted:
                txt_rr = f"{rr.get('product_name','')} {rr.get('product_url','')}".lower()
                if not _is_plain_easyflow_text(txt_rr):
                    continue
                fam_rr = _aco_family_hint(rr)
                if fam_rr != "easyflow":
                    continue
                is_parent = (
                    str(rr.get("candidate_type") or "").lower() == "drain"
                    and str(rr.get("system_role") or "").lower() == "complete_system"
                    and str(rr.get("promotion_reason") or "").lower() == "complete_system"
                    and ("komplettablauf" in txt_rr or "komplettabläufe" in txt_rr or "komplettablaeufe" in txt_rr)
                )
                if is_parent:
                    easyflow_parent_rows.append(rr)
                    continue
                role = str(rr.get("system_role") or "").lower()
                prom = str(rr.get("promotion_reason") or "").lower()
                if (role == "grate") or (prom == "cover_only_component") or any(t in txt_rr for t in ("design-roste", "design-rost", "designrost")):
                    easyflow_option_rows.append((rr, "grate"))
                elif (role in {"accessory", "adapter"}) or (prom in {"accessory_only", "adapter_component"}) or any(t in txt_rr for t in ("aufsatzstücke", "aufsatzstuecke", "aufsatzstück", "aufsatzstueck")):
                    easyflow_option_rows.append((rr, "accessory"))

            for p in easyflow_parent_rows:
                for rr, rb in easyflow_option_rows:
                    if rb == "grate":
                        _add_aco_bom(p, rr, "compatible_grate", "grate")
                    else:
                        _add_aco_bom(p, rr, "optional_accessory", "accessory")

        # first safe ACO assembled products (restricted families, grate-only BOM links)
        allowed_assembled_families = {"easyflow", "easyflowplus", "showerdrain_c"}
        aco_debug.setdefault("assembled_products_by_family", {})
        aco_debug.setdefault("sample_aco_assembled_products", [])
        aco_debug.setdefault("assembled_products_skipped_count", 0)
        aco_debug.setdefault("sample_aco_assembly_skipped_reasons", [])
        aco_debug.setdefault("assembled_product_duplicate_skipped_count", 0)
        aco_debug.setdefault("assembled_products_accessory_combinations_skipped_count", 0)
        aco_debug.setdefault("assembled_products_emitted_to_products_count", 0)
        aco_debug.setdefault("assembled_products_left_in_components_count", 0)

        aco_by_id: Dict[str, Dict[str, Any]] = {}
        for rr in aco_rows + [r for r in products_rows if str(r.get("manufacturer") or "").lower() == "aco"]:
            pid = str(rr.get("product_id") or "").strip()
            if pid and pid not in aco_by_id:
                aco_by_id[pid] = rr
        existing_ids = {str(r.get("product_id") or "") for r in products_rows}
        seen_assembled_keys: Set[Tuple[str, str, str]] = set()
        tech_keys = [
            "flow_rate_lps", "flow_rate_raw_text", "flow_rate_unit", "outlet_dn",
            "outlet_dn_default", "outlet_dn_options_json", "height_adj_min_mm",
            "height_adj_max_mm", "din_en_1253_cert",
        ]
        for br in [r for r in bom_rows if str(r.get("manufacturer") or "").lower() == "aco"]:
            fam = str(br.get("parent_family") or "")
            if fam not in allowed_assembled_families:
                continue
            opt_type = str(br.get("option_type") or "").lower()
            opt_role = str(br.get("option_role") or "").lower()
            if opt_type != "compatible_grate" or opt_role != "grate":
                if opt_type in {"optional_accessory", "compatible_adapter"}:
                    aco_debug["assembled_products_accessory_combinations_skipped_count"] += 1
                continue
            pid = str(br.get("product_id") or "").strip()
            cid = str(br.get("component_id") or "").strip()
            parent = aco_by_id.get(pid, {})
            grate = aco_by_id.get(cid, {})
            if not parent or not grate:
                aco_debug["assembled_products_skipped_count"] += 1
                if len(aco_debug["sample_aco_assembly_skipped_reasons"]) < 20:
                    aco_debug["sample_aco_assembly_skipped_reasons"].append(f"{fam}:{pid}->{cid}:missing_parent_or_grate")
                continue
            ofam = str(br.get("option_family") or "")
            if fam != ofam:
                aco_debug["assembled_products_skipped_count"] += 1
                if len(aco_debug["sample_aco_assembly_skipped_reasons"]) < 20:
                    aco_debug["sample_aco_assembly_skipped_reasons"].append(f"{fam}:{pid}->{cid}:cross_family")
                continue
            k = (fam, pid, cid)
            if k in seen_assembled_keys:
                aco_debug["assembled_product_duplicate_skipped_count"] += 1
                continue
            seen_assembled_keys.add(k)
            assembled_id = f"aco-assembled-{fam.replace('_','-')}-{pid}__{cid}".lower()
            if assembled_id in existing_ids:
                aco_debug["assembled_product_duplicate_skipped_count"] += 1
                continue
            row = dict(parent)
            row.update({
                "manufacturer": "aco",
                "product_id": assembled_id,
                "product_name": f"{parent.get('product_name','')} + {grate.get('product_name','')}".strip(" +"),
                "candidate_type": "drain",
                "complete_system": "yes",
                "system_role": "assembled_system",
                "promote_to_product": "yes",
                "promotion_reason": "assembled_from_bom",
                "why_not_product_reason": "",
                "assembly_reason": "aco_bom_body_grate_assembly",
                "assembled_from_bom": "true",
                "parent_family": fam,
                "option_family": ofam,
                "base_product_id": pid,
                "grate_component_id": cid,
                "matched_component_ids": ",".join([pid, cid]),
                "source_url": str(br.get("source_url") or parent.get("product_url") or ""),
                "sources": ",".join([str(parent.get("product_url") or ""), str(grate.get("product_url") or "")]).strip(","),
            })
            row["option_label"] = str(br.get("option_label") or grate.get("product_name") or "")
            for tk in tech_keys:
                if tk in parent:
                    row[tk] = parent.get(tk)
            products_rows.append(row)
            comparison_rows.append({
                "manufacturer": "aco",
                "product_id": assembled_id,
                "product_name": row.get("product_name"),
                "candidate_type": "drain",
                "complete_system": "yes",
                "promote_to_product": "yes",
                "promotion_reason": "assembled_from_bom",
                "why_not_product_reason": "",
                "matched_component_ids": row.get("matched_component_ids"),
            })
            existing_ids.add(assembled_id)
            aco_debug["assembled_products_created_count"] += 1
            aco_debug["assembled_products_by_family"][fam] = aco_debug["assembled_products_by_family"].get(fam, 0) + 1
            if len(aco_debug["sample_aco_assembled_products"]) < 20:
                aco_debug["sample_aco_assembled_products"].append(f"{assembled_id}|{pid}|{cid}")

        assembled_rows_now = [
            r for r in products_rows
            if str(r.get("manufacturer") or "").lower() == "aco"
            and str(r.get("promotion_reason") or "").lower() == "assembled_from_bom"
        ]
        aco_debug["assembled_products_emitted_to_products_count"] = sum(
            1 for r in assembled_rows_now
            if str(r.get("candidate_type") or "").lower() == "drain"
            and str(r.get("promote_to_product") or "").lower() == "yes"
            and str(r.get("system_role") or "").lower() in {"assembled_system", "complete_system"}
        )
        aco_debug["assembled_products_left_in_components_count"] = sum(
            1 for r in assembled_rows_now
            if str(r.get("candidate_type") or "").lower() != "drain"
            or str(r.get("promote_to_product") or "").lower() != "yes"
        )

        aco_bom_rows = [r for r in bom_rows if str(r.get("manufacturer") or "").lower() == "aco"]
        aco_all_ids_after = [str(r.get("product_id") or "") for r in registry_rows if str(r.get("manufacturer") or "").lower() == "aco"]
        aco_debug["aco_hash_like_ids_after_count"] = sum(1 for pid in aco_all_ids_after if aco_hash_like_re.match(pid))
        aco_debug["aco_stable_id_migration_count"] = max(0, aco_debug["aco_hash_like_ids_before_count"] - aco_debug["aco_hash_like_ids_after_count"])
        if aco_debug["aco_hash_like_ids_before_count"] > 0 and aco_debug["aco_hash_like_ids_after_count"] == 0:
            aco_debug["sample_aco_stable_id_migrations"].append("hash_like_ids_removed_from_aco_registry_output")
        aco_component_ids = {str(r.get("product_id") or "") for r in registry_rows if str(r.get("manufacturer") or "").lower() == "aco" and str(r.get("candidate_type") or "").lower() == "component"}
        aco_all_registry_ids = {str(r.get("product_id") or "") for r in registry_rows if str(r.get("manufacturer") or "").lower() == "aco"}
        orphan_count = 0
        for br in aco_bom_rows:
            pid = str(br.get("product_id") or "")
            cid = str(br.get("component_id") or "")
            ok = bool(pid in aco_all_registry_ids and cid in aco_component_ids)
            if not ok:
                orphan_count += 1
            if len(aco_debug["sample_aco_bom_id_reference_checks"]) < 20:
                aco_debug["sample_aco_bom_id_reference_checks"].append(f"{pid}->{cid}|ok={str(ok).lower()}")
        aco_debug["aco_orphan_bom_references_count"] = orphan_count
        aco_debug["aco_hash_like_product_ids_after_count"] = sum(
            1 for r in registry_rows
            if str(r.get("manufacturer") or "").lower() == "aco"
            and str(r.get("candidate_type") or "").lower() == "drain"
            and aco_hash_like_re.match(str(r.get("product_id") or ""))
        )
        aco_debug["aco_hash_like_component_ids_after_count"] = sum(
            1 for r in registry_rows
            if str(r.get("manufacturer") or "").lower() == "aco"
            and str(r.get("candidate_type") or "").lower() == "component"
            and aco_hash_like_re.match(str(r.get("product_id") or ""))
        )
        aco_debug["aco_hash_like_bom_product_refs_after_count"] = sum(
            1 for r in aco_bom_rows if aco_hash_like_re.match(str(r.get("product_id") or ""))
        )
        aco_debug["aco_hash_like_bom_component_refs_after_count"] = sum(
            1 for r in aco_bom_rows if aco_hash_like_re.match(str(r.get("component_id") or ""))
        )
        aco_debug["reference_v2_easyflowplus_bom_count"] = sum(1 for r in aco_bom_rows if str(r.get("parent_family") or "") == "easyflowplus")
        aco_debug["reference_v2_easyflow_bom_count"] = sum(1 for r in aco_bom_rows if str(r.get("parent_family") or "") == "easyflow")
        for r in [r for r in aco_bom_rows if str(r.get("parent_family") or "") == "easyflowplus"][:10]:
            if len(aco_debug["sample_reference_v2_easyflowplus_bom"]) < 20:
                aco_debug["sample_reference_v2_easyflowplus_bom"].append(f"{r.get('product_id')}->{r.get('component_id')}:{r.get('option_type')}")
        for r in [r for r in aco_bom_rows if str(r.get("parent_family") or "") == "easyflow"][:10]:
            if len(aco_debug["sample_reference_v2_easyflow_bom"]) < 20:
                aco_debug["sample_reference_v2_easyflow_bom"].append(f"{r.get('product_id')}->{r.get('component_id')}:{r.get('option_type')}")

        aco_debug["bom_options_count"] = max(0, len(bom_rows) - pre_bom_count)

        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_candidates_by_role",
            "snippet": str(aco_debug["candidates_by_role"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_products_by_role",
            "snippet": str(aco_debug["products_by_role"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_components_by_role",
            "snippet": str(aco_debug["components_by_role"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_complete_systems_promoted_count",
            "snippet": str(aco_debug["complete_systems_promoted_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_complete_systems_promoted_sample",
            "snippet": str(aco_debug["complete_systems_promoted_sample"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_components_demoted_by_role_count",
            "snippet": str(aco_debug["components_demoted_by_role_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_components_with_promote_yes_count",
            "snippet": str(aco_debug["components_with_promote_yes_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_promotion_reason_counts",
            "snippet": str(aco_debug["promotion_reason_counts"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_products",
            "snippet": str(aco_debug["sample_aco_products"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_components",
            "snippet": str(aco_debug["sample_aco_components"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_bom_options_count",
            "snippet": str(aco_debug["bom_options_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_bom_options_by_family",
            "snippet": str(aco_debug["bom_options_by_family"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_bom_options_by_type",
            "snippet": str(aco_debug["bom_options_by_type"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_easyflow_bom_count",
            "snippet": str(aco_debug["easyflow_bom_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_showerdrain_bom_count",
            "snippet": str(aco_debug["showerdrain_bom_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_created_count",
            "snippet": str(aco_debug["assembled_products_created_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_by_family",
            "snippet": str(aco_debug.get("assembled_products_by_family", {})),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_assembled_products",
            "snippet": str(aco_debug.get("sample_aco_assembled_products", [])[:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_skipped_count",
            "snippet": str(aco_debug.get("assembled_products_skipped_count", 0)),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_assembly_skipped_reasons",
            "snippet": str(aco_debug.get("sample_aco_assembly_skipped_reasons", [])[:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_product_duplicate_skipped_count",
            "snippet": str(aco_debug.get("assembled_product_duplicate_skipped_count", 0)),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_accessory_combinations_skipped_count",
            "snippet": str(aco_debug.get("assembled_products_accessory_combinations_skipped_count", 0)),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_emitted_to_products_count",
            "snippet": str(aco_debug.get("assembled_products_emitted_to_products_count", 0)),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_assembled_products_left_in_components_count",
            "snippet": str(aco_debug.get("assembled_products_left_in_components_count", 0)),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_bom_options",
            "snippet": str(aco_debug["sample_aco_bom_options"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_unmatched_base_sets",
            "snippet": str(aco_debug["sample_aco_unmatched_base_sets"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_unmatched_grates",
            "snippet": str(aco_debug["sample_aco_unmatched_grates"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_assembly_candidates_rejected",
            "snippet": str(aco_debug["sample_aco_assembly_candidates_rejected"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_showerdrain_c_bom_count",
            "snippet": str(aco_debug["reference_v2_showerdrain_c_bom_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_easyflowplus_products_count",
            "snippet": str(aco_debug["reference_v2_easyflowplus_products_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_easyflow_products_count",
            "snippet": str(aco_debug["reference_v2_easyflow_products_count"]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_easyflowplus_bom_count",
            "snippet": str(aco_debug["reference_v2_easyflowplus_bom_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_easyflow_bom_count",
            "snippet": str(aco_debug["reference_v2_easyflow_bom_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_reference_v2_cross_family_rejected_count",
            "snippet": str(aco_debug["reference_v2_cross_family_rejected_count"]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_reference_v2_showerdrain_c_bom",
            "snippet": str(aco_debug["sample_reference_v2_showerdrain_c_bom"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_reference_v2_easyflow_bom",
            "snippet": str(aco_debug["sample_reference_v2_easyflow_bom"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_reference_v2_easyflowplus_bom",
            "snippet": str(aco_debug["sample_reference_v2_easyflowplus_bom"][:10]),
            "source": "assembly_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_reference_v2_role_corrections",
            "snippet": str(aco_debug["sample_reference_v2_role_corrections"][:10]),
            "source": "promotion_stage",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_stable_id_migration_count",
            "snippet": str(aco_debug["aco_stable_id_migration_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_ids_before_count",
            "snippet": str(aco_debug["aco_hash_like_ids_before_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_ids_after_count",
            "snippet": str(aco_debug["aco_hash_like_ids_after_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_product_ids_after_count",
            "snippet": str(aco_debug["aco_hash_like_product_ids_after_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_component_ids_after_count",
            "snippet": str(aco_debug["aco_hash_like_component_ids_after_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_bom_product_refs_after_count",
            "snippet": str(aco_debug["aco_hash_like_bom_product_refs_after_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_hash_like_bom_component_refs_after_count",
            "snippet": str(aco_debug["aco_hash_like_bom_component_refs_after_count"]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_stable_id_migrations",
            "snippet": str(aco_debug["sample_aco_stable_id_migrations"][:10]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "sample_aco_bom_id_reference_checks",
            "snippet": str(aco_debug["sample_aco_bom_id_reference_checks"][:10]),
            "source": "id_stability",
        })
        evidence_rows.append({
            "manufacturer": "aco",
            "product_id": "__summary__",
            "label": "aco_orphan_bom_references_count",
            "snippet": str(aco_debug["aco_orphan_bom_references_count"]),
            "source": "id_stability",
        })

    has_kaldewei = any(str(r.get("manufacturer") or "").lower() == "kaldewei" for r in products_rows)
    if has_kaldewei:
        try:
            from .connectors import kaldewei as _kaldewei_connector
            kal_source_checks = _kaldewei_connector.validate_kaldewei_sources()
        except Exception:
            kal_source_checks = []
        kaldewei_debug = {
            "kaldewei_assembled_products_created_count": 0,
            "kaldewei_assembled_products_by_family": {},
            "sample_kaldewei_assembled_products": [],
            "kaldewei_assembled_products_skipped_count": 0,
            "sample_kaldewei_assembly_skipped_reasons": [],
            "kaldewei_assembled_product_duplicate_skipped_count": 0,
            "kaldewei_assembled_products_emitted_to_products_count": 0,
            "kaldewei_assembled_products_left_in_components_count": 0,
        }
        kal_rows_map = {str(r.get("product_id") or ""): r for r in products_rows if str(r.get("manufacturer") or "").lower() == "kaldewei"}
        kal_existing_ids = set(kal_rows_map.keys())
        kal_seen_keys: Set[Tuple[str, str]] = set()
        for br in [r for r in bom_rows if str(r.get("manufacturer") or "").lower() == "kaldewei"]:
            if str(br.get("option_type") or "") != "required_trap_set":
                continue
            fam = str(br.get("parent_family") or "")
            if fam not in {"flowline_zero", "flowpoint_zero"}:
                continue
            pid = str(br.get("product_id") or "")
            cid = str(br.get("component_id") or "")
            if cid not in {"kaldewei-flowdrain-horizontal-regular", "kaldewei-flowdrain-horizontal-flat"}:
                continue
            base = kal_rows_map.get(pid)
            trap = kal_rows_map.get(cid)
            if not base or not trap:
                kaldewei_debug["kaldewei_assembled_products_skipped_count"] += 1
                if len(kaldewei_debug["sample_kaldewei_assembly_skipped_reasons"]) < 20:
                    kaldewei_debug["sample_kaldewei_assembly_skipped_reasons"].append(f"{pid}->{cid}:missing_base_or_trap")
                continue
            key = (pid, cid)
            if key in kal_seen_keys:
                kaldewei_debug["kaldewei_assembled_product_duplicate_skipped_count"] += 1
                continue
            kal_seen_keys.add(key)
            trap_suffix = cid.replace("kaldewei-", "")
            asm_id = f"kaldewei-assembled-{fam.replace('_','-')}__{trap_suffix}"
            if asm_id in kal_existing_ids:
                kaldewei_debug["kaldewei_assembled_product_duplicate_skipped_count"] += 1
                continue
            asm = dict(base)
            asm.update({
                "manufacturer": "kaldewei",
                "product_id": asm_id,
                "product_name": f"{base.get('product_name')} with {trap.get('product_name')}",
                "candidate_type": "drain",
                "system_role": "assembled_system",
                "complete_system": "yes",
                "promote_to_product": "yes",
                "promotion_reason": "assembled_from_bom",
                "assembly_reason": "kaldewei_flow_visible_drain_with_flowdrain",
                "assembled_from_bom": "true",
                "product_family": fam,
                "parent_family": fam,
                "option_family": "flowdrain",
                "base_component_id": pid,
                "trap_component_id": cid,
                "matched_component_ids": f"{pid},{cid}",
                "source_url": str(base.get("product_url") or ""),
                "sources": ",".join([str(base.get("product_url") or ""), str(trap.get("product_url") or "")]).strip(","),
                "source_label": "kaldewei_bom_assembly",
                "source_type": "derived_bom_value",
                "source_status": "known",
                "source_note": "derived_from_bom_assembly",
            })
            for k in ("flow_rate_lps", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm", "water_seal_mm", "flow_rate_raw_text"):
                if trap.get(k) not in (None, ""):
                    asm[k] = trap.get(k)
            products_rows.append(asm)
            asm_params = {k: asm.get(k) for k in ("flow_rate_lps","outlet_dn","height_adj_min_mm","height_adj_max_mm","water_seal_mm") if asm.get(k) not in (None, "")}
            asm_param_score, _ = compute_parameter_score(asm_params, cfg)
            asm_equiv_score = compute_equivalence_score({"candidate_type": "drain", **asm_params}, cfg)
            asm_system_score = compute_system_score("drain", has_bom_options=True)
            asm_final_score = compute_final_score(asm_param_score, asm_system_score, asm_equiv_score, cfg)
            asm["param_score"] = asm_param_score
            asm["equiv_score"] = asm_equiv_score
            asm["system_score"] = asm_system_score
            asm["final_score"] = asm_final_score
            comparison_rows.append({
                "manufacturer": "kaldewei", "product_id": asm_id, "product_name": asm.get("product_name"),
                "product_url": asm.get("product_url"), "final_score": asm_final_score, "param_score": asm_param_score, "equiv_score": asm_equiv_score, "system_score": asm_system_score
            })
            kal_existing_ids.add(asm_id)
            kaldewei_debug["kaldewei_assembled_products_created_count"] += 1
            kaldewei_debug["kaldewei_assembled_products_by_family"][fam] = kaldewei_debug["kaldewei_assembled_products_by_family"].get(fam, 0) + 1
            if len(kaldewei_debug["sample_kaldewei_assembled_products"]) < 20:
                kaldewei_debug["sample_kaldewei_assembled_products"].append(f"{asm_id}|{pid}|{cid}")

        kal_nan_clean_count = 0
        kal_nan_clean_sample: List[str] = []
        kaldewei_text_fields = {"promotion_reason", "why_not_product_reason", "assembly_reason", "current_status", "compatibility_caution", "matched_component_ids", "source_url", "sources", "option_meta"}
        for rr in products_rows:
            if str(rr.get("manufacturer") or "").lower() != "kaldewei":
                continue
            for missing_k in kaldewei_text_fields:
                if missing_k not in rr or rr.get(missing_k) is None or (isinstance(rr.get(missing_k), float) and pd.isna(rr.get(missing_k))):
                    rr[missing_k] = ""
            for k, v in list(rr.items()):
                if k not in kaldewei_text_fields:
                    continue
                is_nullish = (v is None) or (isinstance(v, float) and pd.isna(v)) or str(v).strip().lower() in {"nan", "none"}
                if is_nullish:
                    rr[k] = ""
                    kal_nan_clean_count += 1
                    if len(kal_nan_clean_sample) < 20:
                        kal_nan_clean_sample.append(f"{rr.get('product_id')}:{k}")
        for rr in bom_rows:
            if str(rr.get("manufacturer") or "").lower() != "kaldewei":
                continue
            for k in ("option_meta", "source_url", "option_label", "parent_family", "option_family"):
                v = rr.get(k)
                is_nullish = (v is None) or (isinstance(v, float) and pd.isna(v)) or str(v).strip().lower() in {"nan", "none"}
                if is_nullish:
                    rr[k] = ""
                    kal_nan_clean_count += 1
                    if len(kal_nan_clean_sample) < 20:
                        kal_nan_clean_sample.append(f"{rr.get('product_id')}->{rr.get('component_id')}:{k}")

        kal_rows = [r for r in products_rows if str(r.get("manufacturer") or "").lower() == "kaldewei"]
        kal_products = [r for r in kal_rows if str(r.get("candidate_type") or "").lower() == "drain"]
        kal_components = [r for r in kal_rows if str(r.get("candidate_type") or "").lower() == "component"]
        kal_bom = [r for r in bom_rows if str(r.get("manufacturer") or "").lower() == "kaldewei"]
        kal_registry_candidates = 0
        if "manufacturer" in registry_df.columns:
            kal_registry_candidates = int((registry_df["manufacturer"] == "kaldewei").sum())
        def _by_family(rows):
            out = {}
            for r in rows:
                fam = str(r.get("family") or r.get("parent_family") or "unknown")
                out[fam] = out.get(fam, 0) + 1
            return out
        bom_by_type = {}
        for r in kal_bom:
            t = str(r.get("option_type") or "")
            bom_by_type[t] = bom_by_type.get(t, 0) + 1
        unclear = [r for r in kal_rows if "unclear" in str(r.get("current_status") or "") or "unclear" in str(r.get("compatibility_caution") or "")]
        invalid_ka_bom_rows_removed_count = sum(1 for r in kal_bom if str(r.get("product_id") or "") in {"kaldewei-ka-4121", "kaldewei-ka-4122"})
        kaldewei_debug["kaldewei_assembled_products_emitted_to_products_count"] = sum(
            1 for r in kal_rows
            if str(r.get("promotion_reason") or "") == "assembled_from_bom"
            and str(r.get("candidate_type") or "") == "drain"
            and str(r.get("promote_to_product") or "") == "yes"
        )
        kaldewei_debug["kaldewei_assembled_products_left_in_components_count"] = sum(
            1 for r in kal_rows
            if str(r.get("promotion_reason") or "") == "assembled_from_bom"
            and str(r.get("candidate_type") or "") != "drain"
        )
        nexsys_rows = [r for r in kal_rows if str(r.get("product_family") or r.get("family") or "") == "nexsys"]
        nexsys_drain_sets = [r for r in nexsys_rows if str(r.get("system_role") or "") in {"drain_set", "trap_set"}]
        nexsys_covers = [r for r in nexsys_rows if "cover" in str(r.get("system_role") or "")]
        nexsys_bom = [r for r in kal_bom if str(r.get("parent_family") or "") == "nexsys"]
        nexsys_unclear = [r for r in nexsys_rows if "performance_data_unclear" in str(r.get("current_status") or "")]
        flow_finish_components = [r for r in kal_components if str(r.get("system_role") or "") == "finish_cover" and str(r.get("product_id") or "").startswith("kaldewei-flow")]
        flowline_finish_components = [r for r in flow_finish_components if str(r.get("product_family") or "") == "flowline_zero"]
        flowpoint_finish_components = [r for r in flow_finish_components if str(r.get("product_family") or "") == "flowpoint_zero"]
        flow_finish_bom = [r for r in kal_bom if str(r.get("option_type") or "") == "compatible_finish" and str(r.get("product_id") or "") in {"kaldewei-flowline-zero", "kaldewei-flowpoint-zero"}]
        flow_finish_codes = {str(r.get("option_sku") or "") for r in flow_finish_bom if str(r.get("option_sku") or "")}
        for label, snippet in [
            ("kaldewei_registry_candidates_count", str(kal_registry_candidates)),
            ("kaldewei_final_rows_count", str(len(kal_rows))),
            ("kaldewei_products_count", str(len(kal_products))),
            ("kaldewei_components_count", str(len(kal_components))),
            ("kaldewei_candidates_by_family", str(_by_family(kal_rows))),
            ("kaldewei_products_by_family", str(_by_family(kal_products))),
            ("kaldewei_components_by_family", str(_by_family(kal_components))),
            ("kaldewei_bom_options_count", str(len(kal_bom))),
            ("kaldewei_bom_options_by_type", str(bom_by_type)),
            ("sample_kaldewei_products", str([str(r.get('product_id')) for r in kal_products[:10]])),
            ("sample_kaldewei_components", str([str(r.get('product_id')) for r in kal_components[:10]])),
            ("sample_kaldewei_bom_options", str([f"{r.get('product_id')}->{r.get('component_id')}:{r.get('option_type')}" for r in kal_bom[:10]])),
            ("kaldewei_unclear_compatibility_count", str(len(unclear))),
            ("sample_kaldewei_unclear_compatibility", str([str(r.get('product_id')) for r in unclear[:10]])),
            ("kaldewei_seed_param_preservation_count", str(kaldewei_seed_param_preservation_count)),
            ("sample_kaldewei_preserved_seed_params", str(sample_kaldewei_preserved_seed_params[:10])),
            ("kaldewei_invalid_bom_rows_removed_count", str(invalid_ka_bom_rows_removed_count)),
            ("kaldewei_assembled_products_created_count", str(kaldewei_debug["kaldewei_assembled_products_created_count"])),
            ("kaldewei_assembled_products_by_family", str(kaldewei_debug["kaldewei_assembled_products_by_family"])),
            ("sample_kaldewei_assembled_products", str(kaldewei_debug["sample_kaldewei_assembled_products"][:10])),
            ("kaldewei_assembled_products_skipped_count", str(kaldewei_debug["kaldewei_assembled_products_skipped_count"])),
            ("sample_kaldewei_assembly_skipped_reasons", str(kaldewei_debug["sample_kaldewei_assembly_skipped_reasons"][:10])),
            ("kaldewei_assembled_product_duplicate_skipped_count", str(kaldewei_debug["kaldewei_assembled_product_duplicate_skipped_count"])),
            ("kaldewei_assembled_products_emitted_to_products_count", str(kaldewei_debug["kaldewei_assembled_products_emitted_to_products_count"])),
            ("kaldewei_assembled_products_left_in_components_count", str(kaldewei_debug["kaldewei_assembled_products_left_in_components_count"])),
            ("kaldewei_literal_nan_values_cleaned_count", str(kal_nan_clean_count)),
            ("sample_kaldewei_literal_nan_cleanup", str(kal_nan_clean_sample[:10])),
            ("kaldewei_nexsys_detail_layer_applied_count", str(1 if len(nexsys_rows) > 0 else 0)),
            ("kaldewei_nexsys_components_count", str(len([r for r in nexsys_rows if str(r.get("candidate_type") or "") == "component"]))),
            ("kaldewei_nexsys_drain_sets_count", str(len(nexsys_drain_sets))),
            ("kaldewei_nexsys_design_covers_count", str(len(nexsys_covers))),
            ("kaldewei_nexsys_bom_options_count", str(len(nexsys_bom))),
            ("sample_kaldewei_nexsys_components", str([str(r.get("product_id")) for r in nexsys_rows[:10]])),
            ("sample_kaldewei_nexsys_bom_options", str([f"{r.get('product_id')}->{r.get('component_id')}:{r.get('option_type')}" for r in nexsys_bom[:10]])),
            ("kaldewei_nexsys_performance_data_unclear_count", str(len(nexsys_unclear))),
            ("sample_kaldewei_nexsys_unclear_performance_data", str([str(r.get("product_id")) for r in nexsys_unclear[:10]])),
            ("kaldewei_flow_finish_components_count", str(len(flow_finish_components))),
            ("kaldewei_flowline_finish_components_count", str(len(flowline_finish_components))),
            ("kaldewei_flowpoint_finish_components_count", str(len(flowpoint_finish_components))),
            ("kaldewei_flow_finish_bom_options_count", str(len(flow_finish_bom))),
            ("kaldewei_flow_finish_codes_count", str(len(flow_finish_codes))),
            ("sample_kaldewei_flow_finish_components", str([str(r.get("product_id")) for r in flow_finish_components[:10]])),
            ("sample_kaldewei_flow_finish_bom_options", str([f"{r.get('product_id')}->{r.get('component_id')}:{r.get('option_type')}" for r in flow_finish_bom[:10]])),
            ("kaldewei_flow_finish_product_explosion_prevented_count", str(len(flow_finish_bom))),
            ("kaldewei_source_checks_count", str(len(kal_source_checks))),
            ("kaldewei_source_checks_ok_count", str(sum(1 for r in kal_source_checks if str(r.get("review_required") or "") == "no"))),
            ("kaldewei_source_checks_review_required_count", str(sum(1 for r in kal_source_checks if str(r.get("review_required") or "") == "yes"))),
            ("kaldewei_source_hash_changed_count", str(sum(1 for r in kal_source_checks if bool(r.get("hash_changed"))))),
            ("kaldewei_source_unreachable_count", str(sum(1 for r in kal_source_checks if str(r.get("status_code") or "") not in {"200", "200.0"}))),
            ("kaldewei_source_expected_terms_missing_count", str(sum(1 for r in kal_source_checks if str(r.get("expected_terms_missing") or "") != ""))),
            ("kaldewei_new_source_candidates_count", str(sum(int(r.get("new_source_candidate_count") or 0) for r in kal_source_checks))),
            ("sample_kaldewei_source_review_required", str([f"{r.get('source_id')}:{r.get('review_reason')}" for r in kal_source_checks if str(r.get('review_required') or '') == 'yes'][:10])),
            ("sample_kaldewei_new_source_candidates", str([r.get("sample_new_source_candidates") for r in kal_source_checks if str(r.get("sample_new_source_candidates") or "")][:10])),
            ("kaldewei_source_baseline_missing_count", str(sum(1 for r in kal_source_checks if str(r.get("baseline_status") or "") == "missing"))),
            ("kaldewei_source_warning_terms_missing_count", str(sum(1 for r in kal_source_checks if str(r.get("warning_terms_missing") or "") != ""))),
            ("kaldewei_new_product_source_candidates_count", str(sum(int(r.get("new_product_source_candidate_count") or 0) for r in kal_source_checks))),
            ("kaldewei_new_pdf_source_candidates_count", str(sum(int(r.get("new_pdf_source_candidate_count") or 0) for r in kal_source_checks))),
            ("sample_kaldewei_new_product_source_candidates", str([r.get("sample_new_product_source_candidates") for r in kal_source_checks if str(r.get("sample_new_product_source_candidates") or "")][:10])),
            ("sample_kaldewei_new_pdf_source_candidates", str([r.get("sample_new_pdf_source_candidates") for r in kal_source_checks if str(r.get("sample_new_pdf_source_candidates") or "")][:10])),
            ("kaldewei_source_baseline_file_exists", str(Path(getattr(_kaldewei_connector, "BASELINE_PATH", "")).exists())),
            ("kaldewei_source_baseline_can_be_initialized", "yes"),
            ("kaldewei_source_baseline_path", str(getattr(_kaldewei_connector, "BASELINE_PATH", ""))),
            ("kaldewei_ignored_source_candidates_count", str(sum(int(r.get("ignored_candidate_count") or 0) for r in kal_source_checks))),
            ("sample_kaldewei_ignored_source_candidates", str([r.get("sample_ignored_candidates") for r in kal_source_checks if str(r.get("sample_ignored_candidates") or "")][:10])),
            ("kaldewei_ignored_language_variant_candidates_count", str(sum(int(r.get("ignored_language_variant_candidates_count") or 0) for r in kal_source_checks))),
            ("kaldewei_ignored_pricelist_candidates_count", str(sum(int(r.get("ignored_pricelist_candidates_count") or 0) for r in kal_source_checks))),
            ("kaldewei_ignored_asset_candidates_count", str(sum(int(r.get("ignored_asset_candidates_count") or 0) for r in kal_source_checks))),
            ("kaldewei_source_checks_partial_export_rows_count", "0"),
        ]:
            evidence_rows.append({"manufacturer": "kaldewei", "product_id": "__summary__", "label": label, "snippet": snippet, "source": "kaldewei_summary"})
        for sc in kal_source_checks:
            evidence_rows.append({
                "manufacturer": "kaldewei",
                "product_id": "__source_check__",
                "label": f"source_check:{sc.get('source_id')}",
                "snippet": json.dumps(sc, ensure_ascii=False),
                "source": str(sc.get("source_url") or ""),
            })

    products_df = pd.DataFrame(products_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    excluded_df = pd.DataFrame(excluded_rows)
    evidence_df = pd.DataFrame(evidence_rows)
    bom_options_df = pd.DataFrame(bom_rows)

    return products_df, comparison_df, excluded_df, evidence_df, bom_options_df
