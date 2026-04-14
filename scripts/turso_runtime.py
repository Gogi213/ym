from __future__ import annotations

from dataclasses import dataclass
import importlib
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class TursoConfig:
    database_url: str
    auth_token: str
    local_replica_path: str


def load_turso_config() -> TursoConfig:
    database_url = str(os.getenv("TURSO_DATABASE_URL") or "").strip()
    auth_token = str(os.getenv("TURSO_AUTH_TOKEN") or "").strip()
    local_replica_path = str(
        os.getenv("TURSO_LOCAL_REPLICA_PATH")
        or (ROOT / ".turso" / "ym-local.db")
    ).strip()

    if not database_url or not auth_token:
        raise RuntimeError(
            "Turso connection is not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
        )

    return TursoConfig(
        database_url=database_url,
        auth_token=auth_token,
        local_replica_path=local_replica_path,
    )


def connect_turso(config: TursoConfig | None = None):
    resolved = config or load_turso_config()
    try:
        libsql = importlib.import_module("libsql")
    except ImportError as exc:
        raise RuntimeError("Missing libsql dependency. Install requirements before using Turso.") from exc

    Path(resolved.local_replica_path).parent.mkdir(parents=True, exist_ok=True)
    connection = libsql.connect(
        database=resolved.local_replica_path,
        sync_url=resolved.database_url,
        auth_token=resolved.auth_token,
    )
    connection.sync()
    return connection
