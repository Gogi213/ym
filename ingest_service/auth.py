from __future__ import annotations


def is_authorized(expected_token: str, actual_token: str | None) -> bool:
    return bool(expected_token) and actual_token == expected_token
