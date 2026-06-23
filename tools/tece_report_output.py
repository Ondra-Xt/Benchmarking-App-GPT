from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def dumps_json(payload: Any) -> str:
    """Serialize report JSON while preserving Unicode characters."""
    return json.dumps(payload, indent=2, ensure_ascii=False)


def _utf8_stdout() -> Any:
    stream = sys.stdout
    reconfigure = getattr(stream, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8")
        except (AttributeError, TypeError, ValueError, OSError):
            pass
    return stream


def write_text_output(text: str, out: str | Path | None = None) -> None:
    """Write report text as UTF-8 to a file or UTF-8-safe stdout."""
    if out is not None:
        Path(out).write_text(text, encoding="utf-8")
        return

    stream = _utf8_stdout()
    try:
        print(text, file=stream)
    except UnicodeEncodeError:
        # Windows console redirection can expose a cp1252 text stream. Fall back to
        # the binary buffer so report generation does not crash on characters such
        # as ≥, while preserving UTF-8 bytes for redirected files.
        buffer = getattr(stream, "buffer", None)
        if buffer is None:
            print(text.encode("utf-8", errors="replace").decode("utf-8"), file=stream)
        else:
            buffer.write(text.encode("utf-8"))
            buffer.write(b"\n")
            buffer.flush()


def write_json_output(payload: Any, out: str | Path | None = None) -> None:
    write_text_output(dumps_json(payload), out=out)
