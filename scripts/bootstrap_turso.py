from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BOOTSTRAP_PATH = ROOT / "turso" / "bootstrap_schema.sql"

from scripts.turso_runtime import connect_turso, load_turso_config


def load_bootstrap_sql() -> str:
    return BOOTSTRAP_PATH.read_text(encoding="utf-8")


def split_sql_statements(sql: str) -> list[str]:
    statements = []
    current = []
    for line in sql.splitlines():
        current.append(line)
        if line.strip().endswith(";"):
            statement = "\n".join(current).strip()
            if statement:
                statements.append(statement)
            current = []

    trailing = "\n".join(current).strip()
    if trailing:
        statements.append(trailing)

    return statements


def apply_bootstrap(conn, sql: str) -> None:
    for statement in split_sql_statements(sql):
        conn.execute(statement)
    conn.commit()
    conn.sync()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply Turso bootstrap schema.")
    parser.parse_args()

    config = load_turso_config()
    conn = connect_turso(config)
    apply_bootstrap(conn, load_bootstrap_sql())


if __name__ == "__main__":
    main()
