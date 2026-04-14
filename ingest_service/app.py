from __future__ import annotations

import json
from typing import Any, Callable

from fastapi import FastAPI, File, Form, Header, UploadFile
from fastapi.responses import JSONResponse

from ingest_service.auth import is_authorized
from ingest_service.models import IngestSettings, ResetPayload

ResetHandler = Callable[[ResetPayload], dict[str, Any]]
IngestHandler = Callable[[dict[str, Any], UploadFile], dict[str, Any]]


def _default_reset_handler(payload: ResetPayload) -> dict[str, Any]:
    return {"ok": True, "action": payload.action, "run_date": payload.run_date}


def _default_ingest_handler(meta: dict[str, Any], _upload: UploadFile) -> dict[str, Any]:
    return {"ok": True, "status": "accepted", "rows": 0}


def _unauthorized_response() -> JSONResponse:
    return JSONResponse(status_code=401, content={"ok": False, "error": "Unauthorized"})


def create_app(
    ingest_token: str,
    reset_handler: ResetHandler | None = None,
    ingest_handler: IngestHandler | None = None,
) -> FastAPI:
    settings = IngestSettings(ingest_token=ingest_token)
    resolved_reset_handler = reset_handler or _default_reset_handler
    resolved_ingest_handler = ingest_handler or _default_ingest_handler

    app = FastAPI(title="YM Turso Ingest Service")

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/reset")
    def reset(payload: ResetPayload, x_ingest_token: str | None = Header(default=None)) -> Any:
        if not is_authorized(settings.ingest_token, x_ingest_token):
            return _unauthorized_response()
        return resolved_reset_handler(payload)

    @app.post("/ingest")
    async def ingest(
        meta: str | None = Form(default=None),
        file: UploadFile | None = File(default=None),
        x_ingest_token: str | None = Header(default=None),
    ) -> Any:
        if not is_authorized(settings.ingest_token, x_ingest_token):
            return _unauthorized_response()
        if meta is None or file is None:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Missing multipart meta or file"},
            )
        try:
            parsed_meta = json.loads(meta)
        except json.JSONDecodeError:
            return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid meta JSON"})
        return resolved_ingest_handler(parsed_meta, file)

    return app
