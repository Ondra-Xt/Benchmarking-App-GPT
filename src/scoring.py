# src/scoring.py
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Union
import math
import re

from .config import WeightConfig


BENCHMARK_SCORE_KEYS = [
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


def _cfg_get(cfg: Union[Dict[str, Any], WeightConfig], key: str, default: Any = None) -> Any:
    if isinstance(cfg, WeightConfig):
        return cfg.get(key, default)
    return cfg.get(key, default)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    try:
        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass

    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null", "unknown", "not_applicable", "n/a"}


def _to_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None

    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(",", ".")
            cleaned = re.sub(r"[^0-9.\-]+", "", cleaned)
            if cleaned in {"", ".", "-", "-."}:
                return None
            return float(cleaned)

        return float(value)
    except Exception:
        return None


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _joined_text(row: Dict[str, Any], keys: Tuple[str, ...]) -> str:
    return " ".join(str(row.get(k) or "") for k in keys).strip().lower()


def _is_truthy(value: Any) -> bool:
    text = _norm(value)
    return text in {
        "yes",
        "true",
        "1",
        "y",
        "confirmed",
        "pass",
        "passed",
        "available",
        "included",
        "preassembled",
        "included_preassembled",
    }


def _is_falsey(value: Any) -> bool:
    text = _norm(value)
    return text in {"no", "false", "0", "n", "not confirmed", "not_available", "not available"}


def _get_benchmark_weights(cfg: Union[Dict[str, Any], WeightConfig]) -> Dict[str, float]:
    raw = _cfg_get(cfg, "final_weights_pct", {}) or {}

    if not isinstance(raw, dict):
        raw = {}

    weights: Dict[str, float] = {}
    for key in BENCHMARK_SCORE_KEYS:
        val = _to_float(raw.get(key))
        if val is None:
            val = float(DEFAULT_BENCHMARK_WEIGHTS_PCT[key])
        weights[key] = max(0.0, float(val))

    return weights


def normalize_colours_count(value: Any) -> Optional[int]:
    numeric = _to_float(value)
    if numeric is None:
        return None

    try:
        out = int(numeric)
    except Exception:
        return None

    return out if out >= 0 else None


def determine_complete_system(row: Dict[str, Any]) -> str:
    candidate_type = _norm(row.get("candidate_type"))

    if "finish" in candidate_type:
        return "requires_base"

    return str(row.get("complete_system", "unknown"))


def _score_flow_rate(params: Dict[str, Any]) -> Tuple[float, str]:
    flow = _to_float(params.get("flow_rate_lps"))

    if flow is None or flow <= 0:
        return 0.0, "unknown"

    if flow >= 0.8:
        return 1.0, "yes"

    return max(0.0, min(1.0, flow / 0.8)), "no"


def _score_material_v4a(params: Dict[str, Any]) -> float:
    material_text = _joined_text(
        params,
        (
            "material_v4a",
            "material_detail",
            "material_class",
            "material",
            "component_material",
        ),
    )

    if any(token in material_text for token in ("v4a", "1.4404", "1.4571", "316l", "316")):
        return 1.0

    if any(token in material_text for token in ("v2a", "1.4301", "304")):
        return 0.5

    if "stainless" in material_text or "edelstahl" in material_text or "nerez" in material_text:
        return 0.4

    if any(token in material_text for token in ("polypropylene", "polypropylen", "abs", "plastic", "kunststoff")):
        return 0.2

    return 0.0


def _score_din_en_1253(params: Dict[str, Any]) -> float:
    text = _joined_text(
        params,
        (
            "din_en_1253",
            "certification_din_en_1253",
            "din_en_1253_cert",
            "certifications",
            "certificate_text",
            "standard",
            "standards",
        ),
    )

    if "din en 1253" in text or "en 1253" in text or "en1253" in text:
        return 1.0

    for key in ("din_en_1253", "certification_din_en_1253", "din_en_1253_cert"):
        if _is_truthy(params.get(key)):
            return 1.0
        if _is_falsey(params.get(key)):
            return 0.0

    return 0.0


def _score_din_en_18534(params: Dict[str, Any]) -> float:
    text = _joined_text(
        params,
        (
            "din_en_18534",
            "certification_din_en_18534",
            "din_18534_compliance",
            "waterproofing_standard",
            "certifications",
            "certificate_text",
            "standard",
            "standards",
        ),
    )

    if "din en 18534" in text or "en 18534" in text or "en18534" in text:
        return 1.0

    for key in ("din_en_18534", "certification_din_en_18534", "din_18534_compliance"):
        if _is_truthy(params.get(key)):
            return 1.0
        if _is_falsey(params.get(key)):
            return 0.0

    return 0.0


def _score_height_adjustability(params: Dict[str, Any]) -> Tuple[float, float]:
    hmin = _to_float(params.get("height_adj_min_mm"))
    hmax = _to_float(params.get("height_adj_max_mm"))

    if hmin is None or hmax is None or hmax <= hmin:
        return 0.0, 0.0

    height_range = hmax - hmin
    return min(height_range / 100.0, 1.0), height_range


def _get_price(params: Dict[str, Any]) -> Optional[float]:
    for key in ("sales_price", "sales_price_eur", "price_eur", "offer_price"):
        value = _to_float(params.get(key))
        if value is not None and value > 0:
            return value

    return None


def _score_outlet_flexibility(params: Dict[str, Any]) -> float:
    outlet_text = _joined_text(
        params,
        (
            "vertical_outlet_available",
            "side_outlet_available",
            "outlet_orientation",
            "outlet_options",
            "outlet_type",
            "outlet_variants",
        ),
    )

    vertical = _is_truthy(params.get("vertical_outlet_available")) or "vertical" in outlet_text or "svisl" in outlet_text
    side = (
        _is_truthy(params.get("side_outlet_available"))
        or "side" in outlet_text
        or "horizontal" in outlet_text
        or "seitlich" in outlet_text
        or "boční" in outlet_text
        or "bocni" in outlet_text
    )

    if vertical and side:
        return 1.0

    if vertical or side:
        return 0.5

    return 0.0


def _score_sealing_fleece(params: Dict[str, Any]) -> float:
    fleece_text = _joined_text(
        params,
        (
            "sealing_fleece_preassembled",
            "sealing_fleece",
            "waterproofing_fleece_preassembled",
            "fleece",
            "waterproofing",
        ),
    )

    if "preassembled" in fleece_text or "pre-assembled" in fleece_text or "factory fitted" in fleece_text:
        return 1.0

    if _is_truthy(params.get("sealing_fleece_preassembled")) or _is_truthy(params.get("waterproofing_fleece_preassembled")):
        return 1.0

    if "included" in fleece_text or "supplied" in fleece_text or "součást" in fleece_text or "soucast" in fleece_text:
        return 0.5

    if _is_falsey(params.get("sealing_fleece_preassembled")):
        return 0.0

    return 0.0


def _score_colour_count(params: Dict[str, Any]) -> float:
    value = (
        params.get("colours_count")
        or params.get("color_count")
        or params.get("available_colours")
        or params.get("available_colors")
        or params.get("finish_count")
    )

    count = normalize_colours_count(value)

    if count is None or count <= 0:
        return 0.0

    return min(count / 5.0, 1.0)


def compute_parameter_score(
    params: Dict[str, Any],
    cfg: Union[Dict[str, Any], WeightConfig],
) -> Tuple[float, Dict[str, Any]]:
    """
    Benchmark Scoring V2.

    Vrací:
    - celkový benchmark score 0..1
    - detailní score sloupce

    Důležité:
    - fixed construction_height_mm se nepoužívá jako height adjustability
    - sales_price_score se na úrovni jednoho řádku vyřadí z denominatoru,
      pokud cena není dostupná
    - comparison-level price normalizace se dělá později v pipeline.py
    """
    detail: Dict[str, Any] = {}

    flow_score, flow_pass = _score_flow_rate(params)
    detail["flow_rate_score"] = flow_score
    detail["flow_rate_pass_0_8_lps"] = flow_pass

    detail["material_v4a_score"] = _score_material_v4a(params)
    detail["din_en_1253_score"] = _score_din_en_1253(params)
    detail["din_en_18534_score"] = _score_din_en_18534(params)

    height_score, height_range = _score_height_adjustability(params)
    detail["height_adjustability_score"] = height_score
    detail["height_adjustability_range_mm"] = height_range

    price = _get_price(params)
    if price is None:
        detail["sales_price_score"] = 0.0
        detail["scoring_price_available"] = "no"
    else:
        # Row-level provisional score.
        # Finální relativní price score se přepočítá nad Comparison setem v pipeline.py.
        detail["sales_price_score"] = 1.0
        detail["scoring_price_available"] = "yes"

    detail["outlet_flexibility_score"] = _score_outlet_flexibility(params)
    detail["sealing_fleece_score"] = _score_sealing_fleece(params)
    detail["colour_count_score"] = _score_colour_count(params)
    detail["benchmark_scoring_notes"] = "benchmark_scoring_v2"
    detail["scoring_notes"] = "benchmark_scoring_v2"

    weights = _get_benchmark_weights(cfg)

    numerator = 0.0
    denominator = 0.0

    for key in BENCHMARK_SCORE_KEYS:
        weight = float(weights.get(key, 0.0) or 0.0)

        if weight <= 0:
            continue

        # Pokud cena v řádku není, nepenalizuj produkt na úrovni product scoringu.
        # Comparison scoring později rozhodne podle celého setu, jestli ceny existují.
        if key == "sales_price_score" and detail["scoring_price_available"] == "no":
            continue

        numerator += weight * float(detail.get(key, 0.0) or 0.0)
        denominator += weight

    total = numerator / denominator if denominator > 0 else 0.0
    total = max(0.0, min(1.0, float(total)))

    return total, detail


def compute_equivalence_score(
    row: Dict[str, Any],
    cfg: Union[Dict[str, Any], WeightConfig],
) -> float:
    """
    Legacy equivalence score.

    V Benchmark Scoring V2 nemá ovlivňovat hlavní final_score.
    Vracíme 0.0 záměrně, aby staré length/equivalence klíče nedominovaly.
    """
    return 0.0


def compute_system_score(candidate_type: str, has_bom_options: bool) -> float:
    """
    Legacy / diagnostic system score.

    Necháváme jako diagnostický sloupec, ale compute_final_score ho už
    nepoužívá pro hlavní benchmark final_score.
    """
    candidate_type_norm = _norm(candidate_type)

    if candidate_type_norm == "drain":
        return 1.0

    if candidate_type_norm in {"component", "base_set", "finish_set"}:
        return 0.5

    return 0.0


def compute_final_score(
    param_score: float,
    system_score: float,
    equiv_score: float,
    cfg: Union[Dict[str, Any], WeightConfig],
) -> float:
    """
    Benchmark Scoring V2 final score.

    Hlavní final_score je nyní přímo benchmark score z compute_parameter_score().
    Legacy system_score/equiv_score zůstávají jen diagnostické a nesmí final_score
    měnit.
    """
    score = _to_float(param_score)

    if score is None:
        return 0.0

    return max(0.0, min(1.0, float(score)))