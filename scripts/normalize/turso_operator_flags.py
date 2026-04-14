from __future__ import annotations

from typing import Sequence, Tuple


def _filter_row_keys(row_keys: Sequence[Tuple[str, str]]) -> list[tuple[str, str]]:
    return [
        (str(topic or "").strip(), str(row_hash or "").strip())
        for topic, row_hash in row_keys
        if str(topic or "").strip() and str(row_hash or "").strip()
    ]


def refresh_current_flags_for_row_keys(conn, row_keys: Sequence[Tuple[str, str]]) -> None:
    filtered_keys = _filter_row_keys(row_keys)
    if not filtered_keys:
        return

    placeholders = ", ".join(["(?, ?)"] * len(filtered_keys))
    params = [value for pair in filtered_keys for value in pair]
    conn.execute(
        f"""
        with affected_keys(topic, row_hash) as (
          values {placeholders}
        ),
        ranked as (
          select
            fr.fact_row_id,
            row_number() over (
              partition by fr.topic, fr.row_hash
              order by fr.message_date desc, fr.created_at desc, fr.source_file_id desc
            ) as rn
          from fact_rows fr
          join affected_keys ark
            on ark.topic = fr.topic
           and ark.row_hash = fr.row_hash
        )
        update fact_rows
        set is_current = case
          when fact_row_id in (select fact_row_id from ranked where rn = 1) then 1
          else 0
        end
        where fact_row_id in (select fact_row_id from ranked)
        """,
        tuple(params),
    )


__all__ = ["refresh_current_flags_for_row_keys"]
