from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def emit_log(
    logger: Optional[Callable[[str, Dict[str, Any]], None]],
    phase: str,
    payload: Dict[str, Any],
) -> None:
    if logger is None:
        return
    logger(phase, payload)


def public_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if not str(key).startswith("_")
    }
