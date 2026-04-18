from __future__ import annotations

import argparse
import base64
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.turso_runtime import connect_turso, load_turso_config


def fetch_table_columns(conn, table_name: str) -> set[str]:
    cursor = conn.execute(f"pragma table_info('{table_name}')")
    return {str(row[1]) for row in cursor.fetchall()}


def ensure_ingest_file_identity_columns(conn) -> list[str]:
    statements: list[str] = []
    columns = fetch_table_columns(conn, "ingest_files")

    if "raw_file_key" not in columns:
        conn.execute("alter table ingest_files add column raw_file_key text")
        statements.append("alter table ingest_files add column raw_file_key text")
    if "file_hash" not in columns:
        conn.execute("alter table ingest_files add column file_hash text")
        statements.append("alter table ingest_files add column file_hash text")
    if "raw_revision" not in columns:
        conn.execute("alter table ingest_files add column raw_revision integer not null default 0")
        statements.append("alter table ingest_files add column raw_revision integer not null default 0")
    if "updated_at" not in columns:
        conn.execute("alter table ingest_files add column updated_at text")
        statements.append("alter table ingest_files add column updated_at text")

    return statements


def build_file_identity(run_date: str, primary_topic: str, file_base64: str) -> tuple[str, str]:
    file_hash = hashlib.sha256(base64.b64decode(file_base64)).hexdigest()
    raw_file_key = f"{run_date}|{primary_topic}|{file_hash}"
    return raw_file_key, file_hash


def backfill_ingest_file_identity(conn) -> dict[str, int]:
    cursor = conn.execute(
        """
        select f.id, f.run_date, f.primary_topic, p.file_base64
        from ingest_files f
        join ingest_file_payloads p on p.file_id = f.id
        where coalesce(f.raw_file_key, '') = ''
           or coalesce(f.file_hash, '') = ''
           or f.updated_at is null
        order by f.created_at, f.id
        """
    )
    rows = cursor.fetchall()
    if not rows:
        return {"updated_files": 0}

    updates = []
    for row in rows:
        file_id = str(row[0])
        raw_file_key, file_hash = build_file_identity(str(row[1] or ""), str(row[2] or ""), str(row[3] or ""))
        updates.append((raw_file_key, file_hash, file_id))

    conn.executemany(
        """
        update ingest_files
        set raw_file_key = ?,
            file_hash = ?,
            updated_at = coalesce(updated_at, current_timestamp)
        where id = ?
        """,
        updates,
    )
    return {"updated_files": len(updates)}


def ensure_ingest_file_identity_indexes(conn) -> list[str]:
    statements = [
        "create unique index if not exists ingest_files_raw_file_key_idx on ingest_files (raw_file_key)",
        "create index if not exists ingest_files_file_hash_idx on ingest_files (file_hash)",
    ]
    for statement in statements:
        conn.execute(statement)
    return statements


def migrate_ingest_file_identity(conn) -> dict[str, object]:
    altered = ensure_ingest_file_identity_columns(conn)
    backfill = backfill_ingest_file_identity(conn)
    indexed = ensure_ingest_file_identity_indexes(conn)
    conn.commit()
    conn.sync()
    return {
        "altered": altered,
        "backfill": backfill,
        "indexed": indexed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Add and backfill file-centric raw identity columns in Turso ingest_files.")
    parser.parse_args()

    conn = connect_turso(load_turso_config())
    result = migrate_ingest_file_identity(conn)
    print(result)


if __name__ == "__main__":
    main()
