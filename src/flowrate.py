# src/flowrate.py

from __future__ import annotations
import re
from typing import Optional, Tuple, List

def _to_float(x: str) -> Optional[float]:
    try:
        return float(x.replace(",", "."))
    except Exception:
        return None

def _extract_flow_candidates(text: str) -> List[Tuple[float, str, str]]:
    """
    Returns list of tuples: (lps_value, raw_snippet, unit_found)
    """
    if not text:
        return []

    t = " ".join(text.split())
    out: List[Tuple[float, str, str]] = []

    # Examples:
    # "60 l/min", "60 l / min", "60 l/min.", "1,0 l/s"
    pattern = re.compile(
        r"(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>l\s*/\s*min|l\s*/\s*s|l/min|l/s)\b",
        re.IGNORECASE
    )

    for m in pattern.finditer(t):
        val = _to_float(m.group("val"))
        unit = m.group("unit").lower().replace(" ", "")
        if val is None:
            continue

        lps = val
        if "min" in unit:
            lps = val / 60.0

        # sanity
        if 0.05 <= lps <= 5.0:
            lo = max(0, m.start() - 80)
            hi = min(len(t), m.end() + 80)
            out.append((lps, t[lo:hi], unit))

    return out

def select_flow_rate(text: str) -> Tuple[Optional[float], Optional[str], Optional[str], str]:
    """
    Returns: (lps, raw_text, unit, status)
    status:
      - ok
      - ok_no_en1253
      - unknown_no_units
      - rejected_out_of_range
    """
    cands = _extract_flow_candidates(text)
    if not cands:
        return None, None, None, "unknown_no_units"

    # pick max (typicky "bis zu 60 l/min")
    best = max(cands, key=lambda x: x[0])
    lps, raw, unit = best

    # EN1253 context helps but not required
    has_en1253 = bool(re.search(r"(DIN\s*)?EN\s*1253", text, re.IGNORECASE))
    status = "ok" if has_en1253 else "ok_no_en1253"

    return lps, raw, unit, status