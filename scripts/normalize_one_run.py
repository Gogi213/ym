from __future__ import annotations

import argparse
import json
from typing import Any, Dict

from scripts.normalize.common import public_payload
from scripts.normalize.pipeline import finalize_normalized_runs, normalize_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-run local normalization CLI for the current Turso-backed contour."
    )
    parser.add_argument("--run-date", required=True, help="Target run date in YYYY-MM-DD format.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    def cli_logger(phase: str, payload: Dict[str, Any]) -> None:
        print(json.dumps({"phase": phase, **payload}, ensure_ascii=False), flush=True)

    result = normalize_run(args.run_date, logger=cli_logger)
    print(
        json.dumps({"ok": True, "run_date": args.run_date, **public_payload(result)}, ensure_ascii=False),
        flush=True,
    )


__all__ = [
    "finalize_normalized_runs",
    "normalize_run",
    "parse_args",
    "public_payload",
]


if __name__ == "__main__":
    main()
