from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from typing import Callable

import psycopg

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
FROM generate_series(1, %(teller_count)s) AS gs;
""".strip()

PG_LIKE_FILL_ACCOUNTS_SQL = """
INSERT INTO pgbench_accounts (aid, bid, abalance)
SELECT gs, ((gs - 1) / %(accounts_per_branch)s)::int + 1, 0
FROM generate_series(1, %(account_count)s) AS gs;
""".strip()

PG_LIKE_READ_SQL = """
SELECT aid, bid, abalance
FROM pgbench_accounts
WHERE aid BETWEEN %(aid_from)s AND %(aid_to)s
ORDER BY aid
LIMIT 50;
""".strip()

PG_LIKE_WRITE_SQL_STMTS = (
    """
WITH sel AS (
    SELECT floor(random() * %(account_count)s + 1)::int AS aid,
           floor(random() * %(teller_count)s + 1)::int AS tid,
           floor(random() * %(branch_count)s + 1)::int AS bid,
           floor(random() * 2000 - 1000)::int AS delta
)
UPDATE pgbench_accounts a
SET abalance = a.abalance + sel.delta
FROM sel
WHERE a.aid = sel.aid
""".strip(),
    """

WITH sel AS (
    SELECT floor(random() * %(teller_count)s + 1)::int AS tid,
           floor(random() * %(branch_count)s + 1)::int AS bid,
           floor(random() * 2000 - 1000)::int AS delta
)
UPDATE pgbench_tellers t
SET tbalance = t.tbalance + sel.delta
FROM sel
WHERE t.tid = sel.tid
""".strip(),
    """

WITH sel AS (
    SELECT floor(random() * %(branch_count)s + 1)::int AS bid,
           floor(random() * 2000 - 1000)::int AS delta
)
UPDATE pgbench_branches b
SET bbalance = b.bbalance + sel.delta
FROM sel
WHERE b.bid = sel.bid
""".strip(),
    """

INSERT INTO pgbench_history (tid, bid, aid, delta)
SELECT floor(random() * %(teller_count)s + 1)::int,
       floor(random() * %(branch_count)s + 1)::int,
       floor(random() * %(account_count)s + 1)::int,
       floor(random() * 2000 - 1000)::int
""".strip(),
)

PG_LIKE_WRITE_SQL = ";\n\n".join(PG_LIKE_WRITE_SQL_STMTS) + ";"


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


def initialize_pg_like_dataset(
    dsn: str,
    target_size_gb: float,
    progress_cb: Callable[[float, str, float | None], None] | None = None,
) -> PgLikeSizing:
    sizing = estimate_pg_like_sizing(target_size_gb)
    started_at = time.perf_counter()

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
            cur.execute(
                PG_LIKE_FILL_TELLERS_SQL,
                {
                    "teller_count": sizing.teller_count,
                    "tellers_per_branch": sizing.tellers_per_branch,
                },
            )

            report(0.45, "Заполнение pgbench_accounts")
            cur.execute(
                PG_LIKE_FILL_ACCOUNTS_SQL,
                {
                    "account_count": sizing.account_count,
                    "accounts_per_branch": sizing.accounts_per_branch,
                },
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
            params = {
                "account_count": sizing.account_count,
                "teller_count": sizing.teller_count,
                "branch_count": sizing.branch_count,
            }
            for statement in PG_LIKE_WRITE_SQL_STMTS:
                cur.execute(statement, params)
        else:
            upper = max(50, sizing.account_count)
            aid_from = random.randint(1, upper - 49)
            cur.execute(
                PG_LIKE_READ_SQL,
                {"aid_from": aid_from, "aid_to": aid_from + 49},
            )

