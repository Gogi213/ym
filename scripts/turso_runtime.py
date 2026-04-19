from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib import request
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
MAX_TURSO_PIPELINE_EXECUTES = 50


@dataclass(frozen=True)
class TursoConfig:
    database_url: str
    auth_token: str
    local_replica_path: str

    @property
    def pipeline_url(self) -> str:
        return normalize_turso_pipeline_url(self.database_url)


class TursoHttpCursor:
    def __init__(
        self,
        *,
        columns: Sequence[str] | None = None,
        rows: Sequence[tuple[Any, ...]] | None = None,
        affected_row_count: int = 0,
        last_insert_rowid: Any = None,
    ) -> None:
        self._rows = list(rows or [])
        self.description = [(column, None, None, None, None, None, None) for column in (columns or [])]
        self.rowcount = affected_row_count
        self.lastrowid = last_insert_rowid

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class TursoHttpConnection:
    def __init__(self, config: TursoConfig) -> None:
        self._config = config
        self._baton: str | None = None
        self._base_url: str | None = None
        self._in_transaction = False
        self._closed = False

    def __enter__(self) -> TursoHttpConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self.rollback()
        self.close()
        return False

    def begin(self) -> None:
        if self._closed:
            raise RuntimeError("Turso connection is closed.")
        if self._in_transaction:
            raise RuntimeError("Turso transaction already started.")
        self._send_pipeline([_build_turso_execute_request("BEGIN")])
        self._in_transaction = True

    def execute(self, sql: str, params: Iterable[Any] = ()) -> TursoHttpCursor:
        cursor = self.executemany(sql, [tuple(params)] if tuple(params) else [()])
        return cursor

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> TursoHttpCursor:
        if self._closed:
            raise RuntimeError("Turso connection is closed.")

        normalized_params = [tuple(params) for params in seq_of_params]
        if not normalized_params:
            return TursoHttpCursor()

        last_cursor = TursoHttpCursor()
        for chunk_start in range(0, len(normalized_params), MAX_TURSO_PIPELINE_EXECUTES):
            chunk = normalized_params[chunk_start : chunk_start + MAX_TURSO_PIPELINE_EXECUTES]
            requests = [_build_turso_execute_request(sql, params) for params in chunk]
            if not self._in_transaction:
                requests.append({"type": "close"})

            payload = self._send_pipeline(requests)
            results = payload.get("results") or []
            execute_results = [
                item
                for item in results
                if item and item.get("type") == "ok" and (item.get("response") or {}).get("type") == "execute"
            ]
            if execute_results:
                last_cursor = _build_cursor_from_result(execute_results[-1])
        return last_cursor

    def commit(self) -> None:
        if not self._in_transaction:
            return
        self._send_pipeline(
            [
                _build_turso_execute_request("COMMIT"),
                {"type": "close"},
            ]
        )
        self._in_transaction = False

    def rollback(self) -> None:
        if not self._in_transaction:
            return
        self._send_pipeline(
            [
                _build_turso_execute_request("ROLLBACK"),
                {"type": "close"},
            ]
        )
        self._in_transaction = False

    def close(self) -> None:
        if self._closed:
            return
        if self._baton:
            self._send_pipeline([{"type": "close"}])
        self._closed = True

    def sync(self) -> None:
        return

    def _request_url(self) -> str:
        if self._base_url:
            return normalize_turso_pipeline_url(self._base_url)
        return self._config.pipeline_url

    def _send_pipeline(self, requests: Sequence[dict[str, Any]]) -> dict[str, Any]:
        body: dict[str, Any] = {"requests": list(requests)}
        if self._baton:
            body["baton"] = self._baton
        raw_body = json.dumps(body).encode("utf-8")
        http_request = request.Request(
            self._request_url(),
            data=raw_body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._config.auth_token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(http_request) as response:
                status = getattr(response, "status", 200)
                raw_response = response.read().decode("utf-8")
        except HTTPError as exc:
            raw_response = _read_http_error_body(exc)
            if _is_turso_auth_error(exc, raw_response):
                raise _build_turso_auth_error(exc, raw_response) from exc
            raise RuntimeError(f"Turso HTTP request failed with HTTP {exc.code}: {raw_response or exc.reason}") from exc
        except URLError as exc:
            raise RuntimeError(f"Turso HTTP request failed: {exc}") from exc

        try:
            payload = json.loads(raw_response or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Turso HTTP request returned invalid JSON: {raw_response}") from exc

        if status < 200 or status >= 300:
            if _is_turso_auth_error(status, raw_response):
                raise _build_turso_auth_error(status, raw_response)
            raise RuntimeError(f"Turso HTTP request failed with HTTP {status}: {raw_response}")

        _raise_for_turso_result_errors(payload)

        close_requested = any(item.get("type") == "close" for item in requests)
        self._base_url = str(payload.get("base_url") or self._base_url or "").strip() or None
        if close_requested:
            self._baton = None
            self._base_url = None
        else:
            self._baton = str(payload.get("baton") or self._baton or "").strip() or None
        return payload


def _read_http_error_body(error: HTTPError) -> str:
    if error.fp is None:
        return ""
    try:
        return error.fp.read().decode("utf-8")
    except Exception:
        return ""


def _is_turso_auth_error(error: Any, body: str = "") -> bool:
    parts = [str(error or "").lower(), str(body or "").lower()]
    message = " ".join(parts)
    return "401" in message and ("unauthorized" in message or "invalid jwt" in message)


def _build_turso_auth_error(error: Any, body: str = "") -> RuntimeError:
    detail = body or str(error)
    detail = detail.replace("HTTP Error ", "").replace(": Unauthorized", " Unauthorized")
    return RuntimeError(
        "Turso HTTP request failed because TURSO_AUTH_TOKEN looks invalid or expired "
        f"({detail}). refresh the DB token and retry."
    )


def normalize_turso_pipeline_url(database_url: str) -> str:
    raw = str(database_url or "").strip()
    if not raw:
        return ""
    without_pipeline = raw.removesuffix("/v2/pipeline").rstrip("/")
    if without_pipeline.startswith("libsql://"):
        return "https://" + without_pipeline[len("libsql://") :] + "/v2/pipeline"
    if without_pipeline.startswith("https://") or without_pipeline.startswith("http://"):
        return without_pipeline + "/v2/pipeline"
    return "https://" + without_pipeline.lstrip("/") + "/v2/pipeline"


def _build_turso_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    return {"type": "text", "value": str(value)}


def _build_turso_execute_request(sql: str, params: Iterable[Any] = ()) -> dict[str, Any]:
    statement: dict[str, Any] = {"sql": sql}
    args = [_build_turso_value(value) for value in params]
    if args:
        statement["args"] = args
    return {"type": "execute", "stmt": statement}


def _read_turso_cell_value(cell: Any) -> Any:
    if not isinstance(cell, dict):
        return cell
    cell_type = str(cell.get("type") or "").strip()
    if cell_type == "null":
        return None
    if "value" in cell:
        raw_value = cell.get("value")
        if cell_type == "integer":
            try:
                return int(str(raw_value))
            except (TypeError, ValueError):
                return raw_value
        if cell_type == "float":
            try:
                return float(str(raw_value))
            except (TypeError, ValueError):
                return raw_value
        return raw_value
    if "base64" in cell:
        return cell.get("base64")
    return cell


def _build_cursor_from_result(result: dict[str, Any]) -> TursoHttpCursor:
    execute_result = ((result.get("response") or {}).get("result") or {})
    columns = [
        column if isinstance(column, str) else str((column or {}).get("name") or "")
        for column in (execute_result.get("cols") or [])
    ]
    rows = []
    for row in execute_result.get("rows") or []:
        if isinstance(row, list):
            rows.append(tuple(_read_turso_cell_value(cell) for cell in row))
        elif isinstance(row, dict):
            rows.append(tuple(_read_turso_cell_value(row.get(column)) for column in columns))
        else:
            rows.append((row,))
    return TursoHttpCursor(
        columns=columns,
        rows=rows,
        affected_row_count=int(execute_result.get("affected_row_count") or 0),
        last_insert_rowid=execute_result.get("last_insert_rowid"),
    )


def _raise_for_turso_result_errors(payload: dict[str, Any]) -> None:
    for result in payload.get("results") or []:
        if result and result.get("type") == "ok":
            continue
        error_payload = (result or {}).get("error") or result
        error_text = json.dumps(error_payload, ensure_ascii=False)
        if _is_turso_auth_error(error_text):
            raise _build_turso_auth_error(error_text)
        raise RuntimeError(f"Turso SQL request failed: {error_text}")


def _iter_turso_settings_paths() -> list[Path]:
    candidates: list[Path] = []
    explicit = str(os.getenv("TURSO_SETTINGS_PATH") or "").strip()
    if explicit:
        candidates.append(Path(explicit))

    appdata = str(os.getenv("APPDATA") or "").strip()
    if appdata:
        candidates.append(Path(appdata) / "turso" / "settings.json")

    xdg_config_home = str(os.getenv("XDG_CONFIG_HOME") or "").strip()
    if xdg_config_home:
        candidates.append(Path(xdg_config_home) / "turso" / "settings.json")

    home = str(os.getenv("HOME") or "").strip()
    if home:
        candidates.append(Path(home) / ".config" / "turso" / "settings.json")

    unique_candidates: list[Path] = []
    seen = set()
    for path in candidates:
        normalized = str(path.resolve(strict=False))
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(path)
    return unique_candidates


def _load_turso_cli_cached_config() -> tuple[str, str] | None:
    for settings_path in _iter_turso_settings_paths():
        if not settings_path.exists():
            continue

        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        cache = settings.get("cache") or {}
        databases = ((cache.get("database_names") or {}).get("data") or [])
        if not databases:
            continue

        database = databases[0] or {}
        db_id = str(database.get("dbId") or "").strip()
        hostname = str(database.get("Hostname") or "").strip()
        token = str((((cache.get("database_token") or {}).get(db_id) or {}).get("data")) or "").strip()
        if db_id and hostname and token:
            return f"libsql://{hostname}", token

    return None


def load_turso_config() -> TursoConfig:
    database_url = str(os.getenv("TURSO_DATABASE_URL") or "").strip()
    auth_token = str(os.getenv("TURSO_AUTH_TOKEN") or "").strip()
    local_replica_path = str(
        os.getenv("TURSO_LOCAL_REPLICA_PATH")
        or (ROOT / ".turso" / "ym-local.db")
    ).strip()

    if not database_url or not auth_token:
        cached_config = _load_turso_cli_cached_config()
        if cached_config:
            cached_database_url, cached_auth_token = cached_config
            database_url = database_url or cached_database_url
            auth_token = auth_token or cached_auth_token

    if not database_url or not auth_token:
        raise RuntimeError(
            "Turso connection is not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
        )

    return TursoConfig(
        database_url=database_url,
        auth_token=auth_token,
        local_replica_path=local_replica_path,
    )


def connect_turso(config: TursoConfig | None = None) -> TursoHttpConnection:
    return TursoHttpConnection(config or load_turso_config())
