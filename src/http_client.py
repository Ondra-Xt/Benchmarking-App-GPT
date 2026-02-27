from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import time
import urllib.request

import requests


DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


@dataclass
class FetchResult:
    url: str
    ok: bool
    status_code: Optional[int] = None
    final_url: Optional[str] = None
    error: Optional[str] = None
    elapsed_ms: Optional[int] = None
    bytes: Optional[int] = None
    text: Optional[str] = None
    content: Optional[bytes] = None

    def to_debug_row(self, connector: str) -> Dict[str, Any]:
        return {
            "connector": connector,
            "url": self.url,
            "ok": self.ok,
            "status_code": self.status_code,
            "final_url": self.final_url,
            "elapsed_ms": self.elapsed_ms,
            "bytes": self.bytes,
            "error": self.error,
        }


def _system_proxies() -> Dict[str, str]:
    """Try to pick up OS/system proxy settings.

    Note: requests primarily uses environment proxy variables. On Windows, corporate
    environments often configure proxies via system settings; urllib can read those.
    """
    try:
        proxies = urllib.request.getproxies() or {}
        # urllib can return keys like 'http'/'https' without scheme in some setups
        out: Dict[str, str] = {}
        for k, v in proxies.items():
            if not v:
                continue
            kk = k.lower()
            if kk in {"http", "https"}:
                out[kk] = v
        return out
    except Exception:
        return {}


def fetch_html(
    url: str,
    *,
    timeout: Tuple[int, int] = (10, 30),
    max_retries: int = 2,
    use_system_proxy: bool = True,
    headers: Optional[Dict[str, str]] = None,
) -> FetchResult:
    """Fetch HTML with basic resiliency and debug metadata.

    Returns a FetchResult; if ok=False, `error` is populated.
    """

    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    proxies = _system_proxies() if use_system_proxy else {}

    last_err: Optional[str] = None
    start = time.time()

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            resp = requests.get(
                url,
                headers=hdrs,
                timeout=timeout,
                allow_redirects=True,
                proxies=proxies if proxies else None,
            )
            elapsed_ms = int((time.time() - t0) * 1000)
            text = resp.text
            return FetchResult(
                url=url,
                ok=bool(resp.status_code == 200 and text),
                status_code=int(resp.status_code),
                final_url=str(resp.url),
                elapsed_ms=elapsed_ms,
                bytes=len(resp.content) if resp.content is not None else None,
                text=text,
                content=resp.content,
            )
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            # minimal backoff
            time.sleep(0.3 * attempt)

    elapsed_ms_total = int((time.time() - start) * 1000)
    return FetchResult(
        url=url,
        ok=False,
        status_code=None,
        final_url=None,
        error=last_err or "Unknown error",
        elapsed_ms=elapsed_ms_total,
        bytes=None,
        text=None,
        content=None,
    )


def fetch_bytes(
    url: str,
    *,
    timeout: Tuple[int, int] = (10, 60),
    max_retries: int = 2,
    use_system_proxy: bool = True,
    headers: Optional[Dict[str, str]] = None,
) -> FetchResult:
    """Fetch raw bytes (useful for .xml.gz sitemaps)."""
    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)
    proxies = _system_proxies() if use_system_proxy else {}

    last_err: Optional[str] = None
    start = time.time()
    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            resp = requests.get(
                url,
                headers=hdrs,
                timeout=timeout,
                allow_redirects=True,
                proxies=proxies if proxies else None,
            )
            elapsed_ms = int((time.time() - t0) * 1000)
            content = resp.content
            return FetchResult(
                url=url,
                ok=bool(resp.status_code == 200 and content),
                status_code=int(resp.status_code),
                final_url=str(resp.url),
                elapsed_ms=elapsed_ms,
                bytes=len(content) if content is not None else None,
                text=None,
                content=content,
            )
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(0.3 * attempt)

    elapsed_ms_total = int((time.time() - start) * 1000)
    return FetchResult(
        url=url,
        ok=False,
        status_code=None,
        final_url=None,
        error=last_err or "Unknown error",
        elapsed_ms=elapsed_ms_total,
        bytes=None,
        text=None,
        content=None,
    )
