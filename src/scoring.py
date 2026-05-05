# src/scoring.py
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Union
from .config import WeightConfig


def _cfg_get(cfg: Union[Dict[str, Any], WeightConfig], key: str, default: Any = None) -> Any:
    if isinstance(cfg, WeightConfig):
        return cfg.get(key, default)
    return cfg.get(key, default)


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


# --- normalizace / helpery (aby pipeline importy nepadaly) ----------------

def normalize_colours_count(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        v = int(float(x))
        return v if v >= 0 else None
    except Exception:
        return None


def determine_complete_system(row: Dict[str, Any]) -> str:
    """
    Jednoduché pravidlo pro sprchové žlaby:
    - finish set / vrchní sada sama o sobě není kompletní systém
    - pokud máme BOM options (base set), tak se to bere jako "requires_base"
    """
    ct = str(row.get("candidate_type", "")).lower()
    if "finish" in ct:
        return "requires_base"
    return str(row.get("complete_system", "unknown"))


# --- scoring -------------------------------------------------------------

def compute_parameter_score(params: Dict[str, Any], cfg: Union[Dict[str, Any], WeightConfig]) -> Tuple[float, Dict[str, float]]:
    """
    Vrací (score 0..1, detail_scores per field 0..1).
    """
    w = _cfg_get(cfg, "param_weights", {}) or {}
    if not isinstance(w, dict) or not w:
        w = {}

    # Normalizace vah
    wsum = sum(float(v) for v in w.values() if _to_float(v) is not None) or 1.0

    detail: Dict[str, float] = {}
    total = 0.0

    flow = _to_float(params.get("flow_rate_lps"))
    if flow is None or flow <= 0:
        detail["flow_rate_lps"] = 0.0
        detail["flow_rate_pass_0_8_lps"] = 0.0
    elif flow >= 0.8:
        detail["flow_rate_lps"] = 1.0
        detail["flow_rate_pass_0_8_lps"] = 1.0
    else:
        detail["flow_rate_lps"] = max(0.0, min(1.0, flow / 0.8))
        detail["flow_rate_pass_0_8_lps"] = 0.0

    # Certifikace
    detail["din_en_1253_cert"] = 1.0 if str(params.get("din_en_1253_cert")).lower() == "yes" else 0.0
    detail["din_18534_compliance"] = 1.0 if str(params.get("din_18534_compliance")).lower() == "yes" else 0.0

    # Materiál V4A
    detail["material_v4a"] = 1.0 if str(params.get("material_v4a")).lower() == "yes" else 0.0

    # Height adjustability (min/max)
    hmin = _to_float(params.get("height_adj_min_mm"))
    hmax = _to_float(params.get("height_adj_max_mm"))
    detail["height_adjustability"] = 1.0 if (hmin is not None and hmax is not None and hmax >= hmin) else 0.0

    # DN
    dn = str(params.get("outlet_dn_default") or params.get("outlet_dn") or "").strip().upper()
    detail["outlet_dn"] = 1.0 if dn.startswith("DN") else 0.0

    # Fleece
    detail["sealing_fleece_preassembled"] = 1.0 if str(params.get("sealing_fleece_preassembled")).lower() == "yes" else 0.0

    # Colours
    cc = normalize_colours_count(params.get("colours_count"))
    detail["colours_count"] = 1.0 if (cc is not None and cc > 0) else 0.0

    for k, wv in w.items():
        wvf = _to_float(wv) or 0.0
        total += (detail.get(k, 0.0) * (wvf / wsum))

    return float(total), detail


def compute_equivalence_score(row: Dict[str, Any], cfg: Union[Dict[str, Any], WeightConfig]) -> float:
    """
    Zatím jednoduché: když je to finish set, bereme jako 0.5 (protože potřebuje base),
    jinak 1.0.
    """
    ct = str(row.get("candidate_type", "")).lower()
    if "finish" in ct:
        return 0.5
    return 1.0


def compute_system_score(candidate_type: str, has_bom_options: bool) -> float:
    ct = str(candidate_type or "").lower()
    if ct == "finish_set":
        return 1.0 if has_bom_options else 0.5
    if ct == "drain":
        return 1.0
    return 0.5


def compute_final_score(
    param_score: float,
    system_score: float,
    equiv_score: float,
    cfg: Union[Dict[str, Any], WeightConfig],
) -> float:
    """
    cfg může mít w_param/w_system/w_equiv buď jako 0..1 nebo jako % (0..100).
    Default: 0.7 / 0.2 / 0.1
    """
    w_param = float(_cfg_get(cfg, "w_param", 0.7) or 0.7)
    w_system = float(_cfg_get(cfg, "w_system", 0.2) or 0.2)
    w_equiv = float(_cfg_get(cfg, "w_equiv", 0.1) or 0.1)

    # Pokud jsou v procentech, převeď:
    if w_param > 1.0 or w_system > 1.0 or w_equiv > 1.0:
        w_param /= 100.0
        w_system /= 100.0
        w_equiv /= 100.0

    s = (w_param + w_system + w_equiv) or 1.0
    w_param /= s
    w_system /= s
    w_equiv /= s

    out = (param_score * w_param) + (system_score * w_system) + (equiv_score * w_equiv)
    # clamp 0..1
    if out < 0:
        out = 0.0
    if out > 1:
        out = 1.0
    return float(out)