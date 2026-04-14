from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ingest_service.app import create_app
from ingest_service.handlers import create_runtime_handlers
from scripts.turso_runtime import connect_turso


def load_ingest_token() -> str:
    token = str(os.getenv("INGEST_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("INGEST_TOKEN is required for the ingest service")
    return token


def create_runtime_app() -> FastAPI:
    ingest_token = load_ingest_token()
    connection = connect_turso()
    handlers = create_runtime_handlers(connection)
    app = create_app(
        ingest_token=ingest_token,
        reset_handler=handlers.reset_handler,
        ingest_handler=handlers.ingest_handler,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            connection.close()

    app.router.lifespan_context = lifespan
    return app
