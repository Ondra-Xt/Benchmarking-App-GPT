# src/pdf_text.py

from __future__ import annotations
from typing import Tuple, Optional, Dict
import requests

def extract_pdf_text_from_url(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 45) -> Tuple[str, str]:
    """
    Returns (text, status)
    status: ok | http_<code> | exception_<Type> | no_text_extracted
    """
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        if r.status_code != 200:
            return "", f"http_{r.status_code}"

        data = r.content
        try:
            from pypdf import PdfReader
        except Exception:
            return "", "exception_missing_pypdf"

        import io
        reader = PdfReader(io.BytesIO(data))
        texts = []
        for pg in reader.pages:
            t = (pg.extract_text() or "").strip()
            if t:
                texts.append(" ".join(t.split()))
        out = "\n\n".join(texts).strip()
        if not out:
            return "", "no_text_extracted"
        return out, "ok"
    except Exception as e:
        return "", f"exception_{type(e).__name__}"