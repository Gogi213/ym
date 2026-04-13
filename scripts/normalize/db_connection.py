from __future__ import annotations

import os
from urllib.parse import quote, urlsplit, urlunsplit


def load_connection_string() -> str:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    pooler_url = os.getenv("SUPABASE_POOLER_URL")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    if pooler_url and password:
        parsed = urlsplit(pooler_url)
        if "@" not in parsed.netloc:
            raise RuntimeError("SUPABASE_POOLER_URL must include the database username.")
        username, host = parsed.netloc.rsplit("@", 1)
        return urlunsplit(
            (
                parsed.scheme,
                f"{username}:{quote(password, safe='')}@{host}",
                parsed.path,
                parsed.query,
                parsed.fragment,
            )
        )

    raise RuntimeError(
        "Database connection is not configured. Set SUPABASE_DB_URL or both SUPABASE_POOLER_URL and SUPABASE_DB_PASSWORD."
    )


def connect_db():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError("Missing psycopg dependency. Install requirements before running the normalizer.") from exc

    connect_timeout = int(os.getenv("SUPABASE_CONNECT_TIMEOUT_SECONDS", "15"))
    statement_timeout_ms = int(os.getenv("SUPABASE_STATEMENT_TIMEOUT_MS", "300000"))
    idle_tx_timeout_ms = int(os.getenv("SUPABASE_IDLE_IN_TX_TIMEOUT_MS", "60000"))
    application_name = os.getenv("SUPABASE_APPLICATION_NAME", "ym_pipeline")

    options = (
        f"-c statement_timeout={statement_timeout_ms} "
        f"-c idle_in_transaction_session_timeout={idle_tx_timeout_ms}"
    )

    return psycopg.connect(
        load_connection_string(),
        row_factory=dict_row,
        connect_timeout=connect_timeout,
        application_name=application_name,
        options=options,
    )
