# src/config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union
import json

CONFIG_PATH_DEFAULT = "config.json"

# App.py používá EQUIVALENCE_KEYS.index("length_mode_match") -> MUSÍ být v seznamu
# Dejte "length_mode_match" první, ať default index sedí.
EQUIVALENCE_KEYS = [
    "length_mode_match",
    # Robustní doplnění dalších běžných voleb, ať se to nerozbije při rozšiřování appky:
    "selected_length_match",
    "length_delta_within_tolerance",
    "equiv_finish_set_requires_base",
    "equiv_complete_system_bonus",
]

# Mix finálního skóre (app často chce, aby se to rovnalo 100 %)
BENCHMARK_KEYS = [
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
FINAL_KEYS = BENCHMARK_KEYS


@dataclass
class WeightConfig:
    """
    Robustní wrapper:
    - cfg.get("x") funguje
    - cfg.x funguje (atributový přístup pro Streamlit)
    - chybějící klíče umí vrátit default (hlavně pro UI)
    """
    data: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.data)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def keys(self):
        return self.data.keys()

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def __getattr__(self, name: str) -> Any:
        # 1) běžné klíče
        if name in self.data:
            return self.data[name]

        # 2) “unknown penalty” a equivalence klíče – UI nechceme nikdy nechat spadnout
        if name.startswith("unknown_penalty"):
            return 0.0
        if name in EQUIVALENCE_KEYS:
            return 0.0
        if name in FINAL_KEYS:
            # rozumný default, kdyby config byl rozbitý
            return 0.0

        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "data":
            object.__setattr__(self, name, value)
            return
        # zapis do dictu, aby se to propsalo do save_config
        self.data[name] = value


def default_config_dict() -> Dict[str, Any]:
    """
    Kompletní sada defaultů.
    Klíče se nesmí “ztratit”, jinak padá UI.
    """
    return {
        # finální mix v % (součet 100)
        "w_param": 70.0,
        "w_equiv": 10.0,
        "w_system": 20.0,

        # streamlit UI / penalizace unknown
        "unknown_penalty_score": 0.0,

        # equivalence keys (musí existovat, jinak UI může padat)
        "length_mode_match": 0.0,
        "selected_length_match": 0.0,
        "length_delta_within_tolerance": 0.0,
        "equiv_finish_set_requires_base": 0.0,
        "equiv_complete_system_bonus": 0.0,

        # váhy parametrů (škála je libovolná, scoring si to normalizuje)
        "param_weights": {k: 0 for k in BENCHMARK_KEYS},
        "final_weights_pct": {
            "flow_rate_score": 25,
            "material_v4a_score": 15,
            "din_en_1253_score": 10,
            "din_en_18534_score": 10,
            "height_adjustability_score": 10,
            "sales_price_score": 15,
            "outlet_flexibility_score": 5,
            "sealing_fleece_score": 5,
            "colour_count_score": 5,
        },
    }


def default_config() -> WeightConfig:
    return WeightConfig(default_config_dict())


def _merge_defaults(user_cfg: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sloučí config s defaulty tak, aby:
    - žádný default klíč se neztratil
    - pokud user_cfg obsahuje navíc klíče, zachovají se
    - param_weights se mergeují (nepřepisují celé)
    """
    out = dict(defaults)

    # 1) merge top-level
    for k, v in (user_cfg or {}).items():
        if k == "param_weights" and isinstance(v, dict):
            # merge dictu vah
            pw = dict(out.get("param_weights", {}) or {})
            pw.update(v)
            out["param_weights"] = pw
        else:
            out[k] = v

    # 2) jistota: všechny EQUIVALENCE_KEYS existují
    for k in EQUIVALENCE_KEYS:
        out.setdefault(k, 0.0)

    # 3) jistota: FINAL_KEYS existují
    for k in FINAL_KEYS:
        out.setdefault(k, 0.0)
    fw = out.get("final_weights_pct")
    if not isinstance(fw, dict):
        fw = {}
    for k, v in defaults.get("final_weights_pct", {}).items():
        fw.setdefault(k, v)
    out["final_weights_pct"] = fw

    # 4) unknown penalty
    out.setdefault("unknown_penalty_score", 0.0)

    # 5) param_weights jistota
    if "param_weights" not in out or not isinstance(out["param_weights"], dict):
        out["param_weights"] = dict(defaults.get("param_weights", {}) or {})

    return out


def validate_sum_100(values: Union[Dict[str, Any], WeightConfig], keys: Optional[list[str]] = None, tol: float = 0.5) -> bool:
    """
    Kontrola součtu v procentech.
    """
    d = values.to_dict() if isinstance(values, WeightConfig) else dict(values)
    keys = keys or FINAL_KEYS
    s = 0.0
    for k in keys:
        try:
            s += float(d.get(k, 0.0))
        except Exception:
            s += 0.0
    return abs(s - 100.0) <= tol


def load_config(path: str = CONFIG_PATH_DEFAULT) -> WeightConfig:
    """
    Načte config.json a automaticky doplní defaulty.
    """
    defaults = default_config_dict()
    p = Path(path)

    user_cfg: Dict[str, Any] = {}
    if p.exists() and p.is_file():
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                user_cfg = raw
        except Exception:
            user_cfg = {}

    merged = _merge_defaults(user_cfg, defaults)
    return WeightConfig(merged)


def save_config(*args, **kwargs) -> None:
    """
    Robustní save_config:
    - podporuje save_config(cfg, path)
    - podporuje save_config(path, cfg)  <- takhle to volá app.py
    - podporuje save_config(cfg=..., path=...)
    """
    path = kwargs.get("path", CONFIG_PATH_DEFAULT)
    cfg = kwargs.get("cfg", None)

    if len(args) == 2:
        a, b = args
        # (path, cfg)
        if isinstance(a, (str, Path)) and not isinstance(b, (str, Path)):
            path, cfg = a, b
        # (cfg, path)
        elif isinstance(b, (str, Path)) and not isinstance(a, (str, Path)):
            cfg, path = a, b
        else:
            # fallback: ber první jako cfg, druhý jako path
            cfg, path = a, b

    elif len(args) == 1:
        # save_config(cfg) nebo save_config(path)
        a = args[0]
        if isinstance(a, (str, Path)):
            path = a
            # když někdo zavolá jen path, uložíme default config
            cfg = cfg or default_config()
        else:
            cfg = a

    # když cfg pořád není, ulož default
    if cfg is None:
        cfg = default_config()

    # převod na dict
    d = cfg.to_dict() if isinstance(cfg, WeightConfig) else dict(cfg)

    # doplnit defaulty, aby se config “neztenčoval”
    merged = _merge_defaults(d, default_config_dict())

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
