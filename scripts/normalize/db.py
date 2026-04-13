from __future__ import annotations

from collections import defaultdict
import os
from typing import Any, Dict, List, Sequence, Tuple
from urllib.parse import quote, urlsplit, urlunsplit

from .transform import build_pipeline_run_error_update, build_pipeline_run_ready_update


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


def fetch_ingested_files(conn, run_date: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
              run_date,
              message_id,
              thread_id,
              message_date,
              message_subject,
              primary_topic,
              matched_topic,
              topic_role,
              attachment_name,
              attachment_type,
              header_json
            from public.ingest_files
            where run_date = %s
              and status = 'ingested'
            order by coalesce(primary_topic, matched_topic),
                     case when coalesce(topic_role, 'primary') = 'primary' then 0 else 1 end,
                     message_date, created_at, id
            """,
            (run_date,),
        )
        return list(cur.fetchall())


def fetch_ingest_rows(conn, file_ids: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not file_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select file_id, row_index, row_json
            from public.ingest_rows
            where file_id = any(%s)
            order by file_id, row_index
            """,
            (list(file_ids),),
        )
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[str(row["file_id"])].append(row)
        return grouped


def fetch_ingest_payloads(conn, file_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not file_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select file_id, content_type, file_base64
            from public.ingest_file_payloads
            where file_id = any(%s)
            """,
            (list(file_ids),),
        )
        return {str(row["file_id"]): row for row in cur.fetchall()}


def fetch_existing_goal_slots(conn, topics: Sequence[str]) -> Dict[str, Dict[str, int]]:
    if not topics:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            select topic, goal_slot, source_header
            from public.topic_goal_slots
            where topic = any(%s)
            order by topic, goal_slot
            """,
            (list(topics),),
        )
        slots: Dict[str, Dict[str, int]] = defaultdict(dict)
        for row in cur.fetchall():
            slots[row["topic"]][row["source_header"]] = row["goal_slot"]
        return dict(slots)


def delete_existing_rows_for_run(conn, run_date: str) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with deleted as (
              delete from public.fact_rows fr
              using public.ingest_files f
              where fr.source_file_id = f.id
                and f.run_date = %s
              returning fr.topic, fr.row_hash
            )
            select distinct topic, row_hash
            from deleted
            """,
            (run_date,),
        )
        return [
            (str(row["topic"]), str(row["row_hash"]))
            for row in cur.fetchall()
            if row["topic"] and row["row_hash"]
        ]


def mark_pipeline_run_ready(conn, run_date: str, *, files_count: int, fact_rows_count: int) -> None:
    payload = build_pipeline_run_ready_update(
        files_count=files_count,
        fact_rows_count=fact_rows_count,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalized_files = %(normalized_files)s,
              normalized_rows = %(normalized_rows)s,
              normalize_status = %(normalize_status)s,
              normalized_at = timezone('utc', now()),
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def mark_pipeline_run_error(conn, run_date: str, error_message: str) -> None:
    payload = build_pipeline_run_error_update(error_message)
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.pipeline_runs
            set
              normalize_status = %(normalize_status)s,
              last_error = %(last_error)s,
              updated_at = timezone('utc', now())
            where run_date = %(run_date)s
            """,
            {
                "run_date": run_date,
                **payload,
            },
        )


def upsert_topic_goal_slots(conn, records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.topic_goal_slots (
              topic,
              goal_slot,
              source_header,
              goal_label,
              first_seen_file_id
            )
            values (
              %(topic)s,
              %(goal_slot)s,
              %(source_header)s,
              %(goal_label)s,
              %(first_seen_file_id)s
            )
            on conflict (topic, goal_slot) do update
            set
              source_header = excluded.source_header,
              goal_label = coalesce(public.topic_goal_slots.goal_label, excluded.goal_label),
              updated_at = timezone('utc', now())
            """,
            records,
        )


def copy_records(
    conn,
    *,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    if not rows:
        return

    column_list = ", ".join(columns)
    copy_sql = f"copy {table_name} ({column_list}) from stdin"

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(tuple(row.get(column) for column in columns))


def insert_fact_rows(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_rows",
        columns=[
            "fact_row_id",
            "topic",
            "source_file_id",
            "source_row_index",
            "report_date",
            "report_date_from",
            "report_date_to",
            "message_date",
            "layout_signature",
            "row_hash",
            "source_row_json",
        ],
        rows=rows,
    )


def insert_fact_dimensions(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_dimensions",
        columns=[
            "fact_row_id",
            "dimension_key",
            "dimension_value",
        ],
        rows=rows,
    )


def insert_fact_metrics(conn, rows: Sequence[Dict[str, Any]]) -> None:
    copy_records(
        conn,
        table_name="public.fact_metrics",
        columns=[
            "fact_row_id",
            "metric_key",
            "metric_value",
        ],
        rows=rows,
    )


def refresh_current_flags_for_row_keys(conn, row_keys: Sequence[Tuple[str, str]]) -> None:
    filtered_keys = [
        (str(topic or "").strip(), str(row_hash or "").strip())
        for topic, row_hash in row_keys
        if str(topic or "").strip() and str(row_hash or "").strip()
    ]
    if not filtered_keys:
        return

    with conn.cursor() as cur:
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


def refresh_operator_export_rows_for_run(conn, run_date: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from public.operator_export_rows
            where run_date = %s
            """,
            (run_date,),
        )
        cur.execute(
            """
            with target_rows as (
              select
                fr.fact_row_id,
                f.run_date,
                fr.topic,
                fr.report_date,
                fr.report_date_from,
                fr.report_date_to
              from public.fact_rows fr
              join public.ingest_files f
                on f.id = fr.source_file_id
              where fr.is_current = true
                and f.run_date = %s
            ),
            dimension_pivot as (
              select
                fd.fact_row_id,
                max(case when fd.dimension_key = 'utm_source' then fd.dimension_value end) as utm_source,
                max(case when fd.dimension_key = 'utm_medium' then fd.dimension_value end) as utm_medium,
                max(case when fd.dimension_key = 'utm_campaign' then fd.dimension_value end) as utm_campaign
              from public.fact_dimensions fd
              join target_rows tr
                on tr.fact_row_id = fd.fact_row_id
              group by fd.fact_row_id
            ),
            metric_pivot as (
              select
                fm.fact_row_id,
                max(case when fm.metric_key = 'visits' then fm.metric_value end) as visits,
                max(case when fm.metric_key = 'users' then fm.metric_value end) as users,
                max(case when fm.metric_key = 'bounce_rate' then fm.metric_value end) as bounce_rate,
                max(case when fm.metric_key = 'page_depth' then fm.metric_value end) as page_depth,
                max(case when fm.metric_key = 'time_on_site_seconds' then fm.metric_value end) as time_on_site_seconds,
                max(case when fm.metric_key = 'robot_rate' then fm.metric_value end) as robot_rate,
                max(case when fm.metric_key = 'goal_1' then fm.metric_value end) as goal_1,
                max(case when fm.metric_key = 'goal_2' then fm.metric_value end) as goal_2,
                max(case when fm.metric_key = 'goal_3' then fm.metric_value end) as goal_3,
                max(case when fm.metric_key = 'goal_4' then fm.metric_value end) as goal_4,
                max(case when fm.metric_key = 'goal_5' then fm.metric_value end) as goal_5,
                max(case when fm.metric_key = 'goal_6' then fm.metric_value end) as goal_6,
                max(case when fm.metric_key = 'goal_7' then fm.metric_value end) as goal_7,
                max(case when fm.metric_key = 'goal_8' then fm.metric_value end) as goal_8,
                max(case when fm.metric_key = 'goal_9' then fm.metric_value end) as goal_9,
                max(case when fm.metric_key = 'goal_10' then fm.metric_value end) as goal_10,
                max(case when fm.metric_key = 'goal_11' then fm.metric_value end) as goal_11,
                max(case when fm.metric_key = 'goal_12' then fm.metric_value end) as goal_12,
                max(case when fm.metric_key = 'goal_13' then fm.metric_value end) as goal_13,
                max(case when fm.metric_key = 'goal_14' then fm.metric_value end) as goal_14,
                max(case when fm.metric_key = 'goal_15' then fm.metric_value end) as goal_15,
                max(case when fm.metric_key = 'goal_16' then fm.metric_value end) as goal_16,
                max(case when fm.metric_key = 'goal_17' then fm.metric_value end) as goal_17,
                max(case when fm.metric_key = 'goal_18' then fm.metric_value end) as goal_18,
                max(case when fm.metric_key = 'goal_19' then fm.metric_value end) as goal_19,
                max(case when fm.metric_key = 'goal_20' then fm.metric_value end) as goal_20,
                max(case when fm.metric_key = 'goal_21' then fm.metric_value end) as goal_21,
                max(case when fm.metric_key = 'goal_22' then fm.metric_value end) as goal_22,
                max(case when fm.metric_key = 'goal_23' then fm.metric_value end) as goal_23,
                max(case when fm.metric_key = 'goal_24' then fm.metric_value end) as goal_24,
                max(case when fm.metric_key = 'goal_25' then fm.metric_value end) as goal_25
              from public.fact_metrics fm
              join target_rows tr
                on tr.fact_row_id = fm.fact_row_id
              group by fm.fact_row_id
            )
            insert into public.operator_export_rows (
              run_date, topic, report_date, report_date_from, report_date_to,
              utm_source, utm_medium, utm_campaign, utm_content, utm_term,
              visits, users, bounce_rate, page_depth, time_on_site_seconds, robot_rate,
              goal_1, goal_2, goal_3, goal_4, goal_5, goal_6, goal_7, goal_8, goal_9, goal_10,
              goal_11, goal_12, goal_13, goal_14, goal_15, goal_16, goal_17, goal_18, goal_19, goal_20,
              goal_21, goal_22, goal_23, goal_24, goal_25
            )
            select
              tr.run_date,
              tr.topic,
              tr.report_date,
              tr.report_date_from,
              tr.report_date_to,
              dp.utm_source,
              dp.utm_medium,
              dp.utm_campaign,
              'aggregated'::text as utm_content,
              'aggregated'::text as utm_term,
              sum(mp.visits) as visits,
              sum(mp.users) as users,
              sum(case when mp.bounce_rate is not null then mp.bounce_rate * mp.visits end) as bounce_rate,
              sum(case when mp.page_depth is not null then mp.page_depth * mp.visits end) as page_depth,
              sum(case when mp.time_on_site_seconds is not null then mp.time_on_site_seconds * mp.visits end) as time_on_site_seconds,
              sum(case when mp.robot_rate is not null then mp.robot_rate * mp.visits end) as robot_rate,
              sum(mp.goal_1) as goal_1,
              sum(mp.goal_2) as goal_2,
              sum(mp.goal_3) as goal_3,
              sum(mp.goal_4) as goal_4,
              sum(mp.goal_5) as goal_5,
              sum(mp.goal_6) as goal_6,
              sum(mp.goal_7) as goal_7,
              sum(mp.goal_8) as goal_8,
              sum(mp.goal_9) as goal_9,
              sum(mp.goal_10) as goal_10,
              sum(mp.goal_11) as goal_11,
              sum(mp.goal_12) as goal_12,
              sum(mp.goal_13) as goal_13,
              sum(mp.goal_14) as goal_14,
              sum(mp.goal_15) as goal_15,
              sum(mp.goal_16) as goal_16,
              sum(mp.goal_17) as goal_17,
              sum(mp.goal_18) as goal_18,
              sum(mp.goal_19) as goal_19,
              sum(mp.goal_20) as goal_20,
              sum(mp.goal_21) as goal_21,
              sum(mp.goal_22) as goal_22,
              sum(mp.goal_23) as goal_23,
              sum(mp.goal_24) as goal_24,
              sum(mp.goal_25) as goal_25
            from target_rows tr
            left join dimension_pivot dp
              on dp.fact_row_id = tr.fact_row_id
            left join metric_pivot mp
              on mp.fact_row_id = tr.fact_row_id
            group by
              tr.run_date, tr.topic, tr.report_date, tr.report_date_from, tr.report_date_to,
              dp.utm_source, dp.utm_medium, dp.utm_campaign
            """,
            (run_date,),
        )
