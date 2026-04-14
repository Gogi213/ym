from __future__ import annotations

from typing import Any, Dict, Iterable, Sequence


def rows_to_dicts(cursor, rows: Sequence[Any] | None = None) -> list[Dict[str, Any]]:
    resolved_rows = list(rows) if rows is not None else list(cursor.fetchall())
    columns = [description[0] for description in (cursor.description or [])]
    result: list[Dict[str, Any]] = []
    for row in resolved_rows:
        if isinstance(row, dict):
            result.append(dict(row))
            continue
        if hasattr(row, "keys"):
            result.append({column: row[column] for column in columns})
            continue
        result.append({column: row[index] for index, column in enumerate(columns)})
    return result


def execute_select(conn, sql: str, params: Iterable[Any] = ()) -> list[Dict[str, Any]]:
    execute = getattr(conn, "execute", None)
    if callable(execute):
        cursor = execute(sql, tuple(params))
        return rows_to_dicts(cursor)

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return rows_to_dicts(cur)


__all__ = ["execute_select", "rows_to_dicts"]
