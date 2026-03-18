from __future__ import annotations

import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable

import psycopg

WRITE_AUXILIARY_EVERY = 4

PG_LIKE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pgbench_accounts (
    aid INTEGER PRIMARY KEY,
    bid INTEGER NOT NULL,
    abalance INTEGER NOT NULL DEFAULT 0,
    filler CHAR(84) NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pgbench_branches (
    bid INTEGER PRIMARY KEY,
    bbalance INTEGER NOT NULL DEFAULT 0,
    filler CHAR(88) NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pgbench_tellers (
    tid INTEGER PRIMARY KEY,
    bid INTEGER NOT NULL,
    tbalance INTEGER NOT NULL DEFAULT 0,
    filler CHAR(84) NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pgbench_history (
    tid INTEGER NOT NULL,
    bid INTEGER NOT NULL,
    aid INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    mtime TIMESTAMPTZ NOT NULL DEFAULT now(),
    filler CHAR(22) NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS pgbench_accounts_bid_idx ON pgbench_accounts (bid);
CREATE INDEX IF NOT EXISTS pgbench_tellers_bid_idx ON pgbench_tellers (bid);
""".strip()

PG_LIKE_TRUNCATE_SQL = """
TRUNCATE TABLE pgbench_history, pgbench_tellers, pgbench_accounts, pgbench_branches RESTART IDENTITY;
""".strip()

PG_LIKE_FILL_BRANCHES_SQL = """
INSERT INTO pgbench_branches (bid, bbalance)
SELECT gs, 0
FROM generate_series(1, %(branch_count)s) AS gs;
""".strip()

PG_LIKE_FILL_TELLERS_SQL = """
INSERT INTO pgbench_tellers (tid, bid, tbalance)
SELECT gs, ((gs - 1) / %(tellers_per_branch)s)::int + 1, 0
FROM generate_series(%(id_from)s, %(id_to)s) AS gs;
""".strip()

PG_LIKE_FILL_ACCOUNTS_SQL = """
INSERT INTO pgbench_accounts (aid, bid, abalance)
SELECT gs, ((gs - 1) / %(accounts_per_branch)s)::int + 1, 0
FROM generate_series(%(id_from)s, %(id_to)s) AS gs;
""".strip()

PG_LIKE_READ_SCRIPT_SQL = r"""
\set aid random(1, :scale * 100000)
\set bid random(1, :scale)
\set tid random(1, :scale * 10)

SELECT abalance, bid
FROM pgbench_accounts
WHERE aid = :aid;

SELECT bid,
       count(*) AS cnt,
       sum(abalance) AS total_balance,
       avg(abalance) AS avg_balance
FROM pgbench_accounts
WHERE bid = :bid
GROUP BY bid;

SELECT a.aid, a.abalance, t.tid, t.tbalance, b.bid, b.bbalance
FROM pgbench_accounts a
JOIN pgbench_tellers t ON t.bid = a.bid
JOIN pgbench_branches b ON b.bid = a.bid
WHERE a.aid = :aid
  AND t.tid = :tid;

SELECT aid, abalance
FROM pgbench_accounts
WHERE bid = :bid
ORDER BY abalance DESC
LIMIT 20;
""".strip()

PG_LIKE_READ_SQL_STMTS = (
    """
SELECT abalance, bid
FROM pgbench_accounts
WHERE aid = %(aid)s
""".strip(),
    """
SELECT bid,
       count(*) AS cnt,
       sum(abalance) AS total_balance,
       avg(abalance) AS avg_balance
FROM pgbench_accounts
WHERE bid = %(bid)s
GROUP BY bid
""".strip(),
    """
SELECT a.aid, a.abalance, t.tid, t.tbalance, b.bid, b.bbalance
FROM pgbench_accounts a
JOIN pgbench_tellers t ON t.bid = a.bid
JOIN pgbench_branches b ON b.bid = a.bid
WHERE a.aid = %(aid)s
  AND t.tid = %(tid)s
""".strip(),
    """
SELECT aid, abalance
FROM pgbench_accounts
WHERE bid = %(bid)s
ORDER BY abalance DESC
LIMIT 20
""".strip(),
)

PG_LIKE_READ_SQL = ";\n\n".join(PG_LIKE_READ_SQL_STMTS) + ";"

PG_LIKE_WRITE_SCRIPT_SQL = r"""
\set bid random(1, :scale)
\set tid ((:bid - 1) * 10) + random(1, 10)
\set aid ((:bid - 1) * 100000) + random(1, 100000)
\set delta random(-1000, 1000)

BEGIN;

-- Базовая часть: выполняется в каждой write-транзакции.
SELECT abalance
FROM pgbench_accounts
WHERE aid = :aid;

UPDATE pgbench_accounts
SET abalance = abalance + :delta
WHERE aid = :aid;

COMMIT;
""".strip()

PG_LIKE_WRITE_AUXILIARY_SCRIPT_SQL = rf"""
\set bid random(1, :scale)
\set tid ((:bid - 1) * 10) + random(1, 10)
\set aid ((:bid - 1) * 100000) + random(1, 100000)
\set delta random(-1000, 1000)

BEGIN;

-- Дополнительная часть: приложение выполняет этот блок примерно
-- в каждой {WRITE_AUXILIARY_EVERY}-й write-транзакции, чтобы уменьшить
-- конкуренцию за блокировки на общих строках.

UPDATE pgbench_tellers
SET tbalance = tbalance + :delta
WHERE tid = :tid;

INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
VALUES (:tid, :bid, :aid, :delta, clock_timestamp());

SELECT count(*), sum(delta)
FROM pgbench_history
WHERE bid = :bid;

COMMIT;
""".strip()

PG_LIKE_WRITE_PRIMARY_SQL_STMTS = (
    """
SELECT abalance
FROM pgbench_accounts
WHERE aid = %(aid)s
""".strip(),
    """
UPDATE pgbench_accounts
SET abalance = abalance + %(delta)s
WHERE aid = %(aid)s
""".strip(),
)

PG_LIKE_WRITE_AUXILIARY_SQL_STMTS = (
    """
UPDATE pgbench_tellers
SET tbalance = tbalance + %(delta)s
WHERE tid = %(tid)s
""".strip(),
    """
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
VALUES (%(tid)s, %(bid)s, %(aid)s, %(delta)s, clock_timestamp())
""".strip(),
    """
SELECT count(*), sum(delta)
FROM pgbench_history
WHERE bid = %(bid)s
""".strip(),
)

PG_LIKE_WRITE_SQL_STMTS = PG_LIKE_WRITE_PRIMARY_SQL_STMTS + PG_LIKE_WRITE_AUXILIARY_SQL_STMTS

PG_LIKE_WRITE_SQL = (
    "-- Базовая часть: выполняется в каждой write-транзакции.\n"
    "BEGIN;\n\n"
    + ";\n\n".join(PG_LIKE_WRITE_PRIMARY_SQL_STMTS)
    + ";\n\nCOMMIT;"
)

PG_LIKE_WRITE_AUXILIARY_SQL = (
    f"-- Дополнительная часть: приложение выполняет этот блок примерно в каждой {WRITE_AUXILIARY_EVERY}-й "
    "write-транзакции.\n"
    "BEGIN;\n\n"
    + ";\n\n".join(PG_LIKE_WRITE_AUXILIARY_SQL_STMTS)
    + ";\n\nCOMMIT;"
)


@dataclass
class PgLikeSizing:
    target_size_gb: float
    branch_count: int
    tellers_per_branch: int
    accounts_per_branch: int

    @property
    def teller_count(self) -> int:
        return self.branch_count * self.tellers_per_branch

    @property
    def account_count(self) -> int:
        return self.branch_count * self.accounts_per_branch


def estimate_pg_like_sizing(target_size_gb: float, fillfactor: float = 1.25) -> PgLikeSizing:
    bytes_per_account = 128 * fillfactor
    target_bytes = max(target_size_gb, 0.01) * (1024**3)
    branches = max(1, math.ceil(target_bytes / (100000 * bytes_per_account)))
    return PgLikeSizing(
        target_size_gb=target_size_gb,
        branch_count=branches,
        tellers_per_branch=10,
        accounts_per_branch=100000,
    )


def _normalized_worker_count(worker_count: int | None) -> int:
    cpu_count = os.cpu_count() or 1
    if worker_count is None:
        return min(4, cpu_count)
    return max(1, min(int(worker_count), cpu_count))


def _build_id_chunks(total_rows: int, workers: int) -> list[tuple[int, int]]:
    if total_rows <= 0:
        return []
    chunk_size = max(1, math.ceil(total_rows / (workers * 4)))
    return [
        (start, min(start + chunk_size - 1, total_rows))
        for start in range(1, total_rows + 1, chunk_size)
    ]


def _parallel_fill(
    dsn: str,
    sql: str,
    chunks: list[tuple[int, int]],
    workers: int,
    shared_params: dict[str, int],
    progress_prefix: str,
    progress_start: float,
    progress_span: float,
    report: Callable[[float, str], None],
) -> None:
    if not chunks:
        report(progress_start + progress_span, f"{progress_prefix} (нет строк)")
        return

    total_chunks = len(chunks)
    completed = 0

    def run_chunk(chunk: tuple[int, int]) -> None:
        id_from, id_to = chunk
        params = {**shared_params, "id_from": id_from, "id_to": id_to}
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    report(progress_start, f"{progress_prefix} (0/{total_chunks} чанков)")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(run_chunk, chunk) for chunk in chunks]
        for _ in as_completed(futures):
            completed += 1
            progress = progress_start + progress_span * (completed / total_chunks)
            report(progress, f"{progress_prefix} ({completed}/{total_chunks} чанков)")


def initialize_pg_like_dataset(
    dsn: str,
    target_size_gb: float,
    worker_count: int | None = None,
    progress_cb: Callable[[float, str, float | None], None] | None = None,
) -> PgLikeSizing:
    sizing = estimate_pg_like_sizing(target_size_gb)
    started_at = time.perf_counter()
    workers = _normalized_worker_count(worker_count)

    def report(progress: float, stage: str) -> None:
        if not progress_cb:
            return
        elapsed = max(0.001, time.perf_counter() - started_at)
        eta = (elapsed / progress - elapsed) if progress > 0 else None
        progress_cb(progress, stage, eta)

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            report(0.05, "Создание схемы pgbench-like")
            cur.execute(PG_LIKE_SCHEMA_SQL)

            report(0.15, "Очистка таблиц")
            cur.execute(PG_LIKE_TRUNCATE_SQL)

            report(0.25, "Заполнение pgbench_branches")
            cur.execute(PG_LIKE_FILL_BRANCHES_SQL, {"branch_count": sizing.branch_count})

            report(0.35, "Заполнение pgbench_tellers")
            _parallel_fill(
                dsn=dsn,
                sql=PG_LIKE_FILL_TELLERS_SQL,
                chunks=_build_id_chunks(sizing.teller_count, workers),
                workers=workers,
                shared_params={"tellers_per_branch": sizing.tellers_per_branch},
                progress_prefix=f"Заполнение pgbench_tellers, потоков: {workers}",
                progress_start=0.35,
                progress_span=0.15,
                report=report,
            )

            report(0.45, "Заполнение pgbench_accounts")
            _parallel_fill(
                dsn=dsn,
                sql=PG_LIKE_FILL_ACCOUNTS_SQL,
                chunks=_build_id_chunks(sizing.account_count, workers),
                workers=workers,
                shared_params={"accounts_per_branch": sizing.accounts_per_branch},
                progress_prefix=f"Заполнение pgbench_accounts, потоков: {workers}",
                progress_start=0.5,
                progress_span=0.3,
                report=report,
            )

            report(0.8, "ANALYZE")
            cur.execute("ANALYZE pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history")

            report(0.9, "Оценка размера")
            cur.execute(
                """
                SELECT pg_database_size(current_database())::float / (1024^3)
                """
            )
            _actual_size = cur.fetchone()[0]

    report(1.0, "Готово")
    return sizing


def run_pg_like_tx(conn: psycopg.Connection, write_tx: bool, sizing: PgLikeSizing) -> None:
    with conn.cursor() as cur:
        if write_tx:
            bid = random.randint(1, sizing.branch_count)
            teller_offset = random.randint(1, sizing.tellers_per_branch)
            account_offset = random.randint(1, sizing.accounts_per_branch)
            params = {
                "bid": bid,
                "tid": (bid - 1) * sizing.tellers_per_branch + teller_offset,
                "aid": (bid - 1) * sizing.accounts_per_branch + account_offset,
                "delta": random.randint(-1000, 1000),
            }
            for statement in PG_LIKE_WRITE_PRIMARY_SQL_STMTS:
                cur.execute(statement, params)
            if random.randint(1, WRITE_AUXILIARY_EVERY) == 1:
                for statement in PG_LIKE_WRITE_AUXILIARY_SQL_STMTS:
                    cur.execute(statement, params)
        else:
            params = {
                "aid": random.randint(1, sizing.account_count),
                "bid": random.randint(1, sizing.branch_count),
                "tid": random.randint(1, sizing.teller_count),
            }
            for statement in PG_LIKE_READ_SQL_STMTS:
                cur.execute(statement, params)
