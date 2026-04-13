from __future__ import annotations

from typing import Sequence, Tuple


def _filter_row_keys(row_keys: Sequence[Tuple[str, str]]) -> list[tuple[str, str]]:
    return [
        (str(topic or "").strip(), str(row_hash or "").strip())
        for topic, row_hash in row_keys
        if str(topic or "").strip() and str(row_hash or "").strip()
    ]


def _prepare_affected_row_keys(cur, filtered_keys: Sequence[Tuple[str, str]]) -> None:
    cur.execute(
        """
        create temporary table if not exists tmp_affected_row_keys (
          topic text not null,
          row_hash text not null,
          primary key (topic, row_hash)
        ) on commit drop
        """
    )
    cur.execute("truncate tmp_affected_row_keys")
    cur.executemany(
        """
        insert into tmp_affected_row_keys (topic, row_hash)
        values (%s, %s)
        on conflict (topic, row_hash) do nothing
        """,
        filtered_keys,
    )


def _refresh_ranked_current_flags(cur) -> None:
    cur.execute(
        """
        with ranked as (
          select
            fr.fact_row_id,
            row_number() over (
              partition by fr.topic, fr.row_hash
              order by fr.message_date desc nulls last, fr.created_at desc, fr.source_file_id desc
            ) as rn
          from public.fact_rows fr
          join tmp_affected_row_keys ark
            on ark.topic = fr.topic
           and ark.row_hash = fr.row_hash
        )
        update public.fact_rows fr
        set is_current = (ranked.rn = 1)
        from ranked
        where ranked.fact_row_id = fr.fact_row_id
        """
    )


def refresh_current_flags_for_row_keys(conn, row_keys: Sequence[Tuple[str, str]]) -> None:
    filtered_keys = _filter_row_keys(row_keys)
    if not filtered_keys:
        return

    with conn.cursor() as cur:
        _prepare_affected_row_keys(cur, filtered_keys)
        _refresh_ranked_current_flags(cur)
