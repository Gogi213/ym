from __future__ import annotations

from scripts.turso_runtime import connect_turso, load_turso_config


def connect_db():
    return connect_turso(load_turso_config())


__all__ = ["connect_db", "load_turso_config"]
