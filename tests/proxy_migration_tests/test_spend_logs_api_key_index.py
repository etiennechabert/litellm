"""The budget-window seed reads only api_key, startTime and spend, so the
LiteLLM_SpendLogs index on those columns has to serve it without the heap.

api_key correlates at ~0 in a spend-logs heap (rows for one key land roughly one
per page), so an index that stops at (api_key, startTime) sends the planner to a
bitmap heap scan that reads a page per matched row. Carrying spend as a trailing
key column is what makes the seed an index-only scan.
"""

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from re import sub
from typing import Final
from uuid import uuid4

import pytest
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from litellm.proxy.db.budget_window_spend_writer import (
    _SEED_FROM_SPEND_LOGS_KEY_SQL,
    _SEED_FROM_SPEND_LOGS_KEY_UNBOUNDED_SQL,
)

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.timeout(180)

requires_db: Final = pytest.mark.skipif(
    "DATABASE_URL" not in os.environ,
    reason="requires a postgres database (DATABASE_URL)",
)

COVERING_INDEX: Final = "LiteLLM_SpendLogs_api_key_startTime_spend_idx"
PROXY_EXTRAS_SCHEMA: Final = Path("litellm-proxy-extras/litellm_proxy_extras/schema.prisma")
ROW_COUNT: Final = 50_000
KEY_COUNT: Final = 300
PROBE_KEY: Final = "sk-key-0137"
SEED_ARGUMENTS: Final = (
    f"'{PROBE_KEY}'",
    "(now() - interval '7 days')",
    "(now() - interval '2 minutes')",
)

SEED_ROWS_SQL: Final = f"""
INSERT INTO "LiteLLM_SpendLogs"
    (request_id, call_type, api_key, spend, "startTime", "endTime", model)
SELECT
    'req_' || g,
    'acompletion',
    'sk-key-' || lpad(((g::bigint * 7919) % {KEY_COUNT})::text, 4, '0'),
    (random() * 0.05)::double precision,
    now() - ((({ROW_COUNT} - g) * interval '30 days') / {ROW_COUNT}),
    now() - ((({ROW_COUNT} - g) * interval '30 days') / {ROW_COUNT}),
    'gpt-5.6-terra'
FROM generate_series(1, {ROW_COUNT}) g
"""


class _PlanNode(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    node_type: str = Field(alias="Node Type")
    index_name: str | None = Field(default=None, alias="Index Name")
    heap_fetches: int | None = Field(default=None, alias="Heap Fetches")
    children: tuple["_PlanNode", ...] = Field(default=(), alias="Plans")


class _ExplainRow(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    plan: _PlanNode = Field(alias="Plan")


_EXPLAIN_ADAPTER: Final = TypeAdapter(tuple[_ExplainRow, ...])


def _scan_nodes(node: _PlanNode) -> tuple[_PlanNode, ...]:
    """Every leaf of the plan tree, i.e. the nodes that touch a relation."""
    if not node.children:
        return (node,)
    return tuple(leaf for child in node.children for leaf in _scan_nodes(child))


def _admin_url() -> str:
    return os.environ["DATABASE_URL"].split("?")[0]


def _explain(connection: "psycopg.Connection", sql: str) -> _PlanNode:
    """Plan the writer's own statement, with its $n placeholders swapped for the
    arguments it binds. Everything the planner reads, the predicates, the
    columns and the table, stays exactly as the writer issues it."""
    bound: Final = sub(r"\$(\d+)", lambda match: SEED_ARGUMENTS[int(match[1]) - 1], sql)
    raw: Final = connection.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {bound}").fetchone()
    assert raw is not None
    return _EXPLAIN_ADAPTER.validate_python(raw[0])[0].plan


@pytest.fixture(scope="module")
def seeded_database() -> Iterator[str]:
    """A fresh database with every committed migration applied and enough spend
    logs for the planner to make a real choice between the index and the heap."""
    admin_url: Final = _admin_url()
    name: Final = f"spend_logs_index_{uuid4().hex[:8]}"
    url: Final = f"{admin_url.rsplit('/', 1)[0]}/{name}"

    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    try:
        subprocess.run(
            ["prisma", "migrate", "deploy", "--schema", str(PROXY_EXTRAS_SCHEMA)],
            check=True,
            env={**os.environ, "DATABASE_URL": url, "DIRECT_URL": url},
        )
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(SEED_ROWS_SQL)
            conn.execute('VACUUM ANALYZE "LiteLLM_SpendLogs"')
        yield url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@requires_db
@pytest.mark.parametrize(
    "seed_sql",
    (_SEED_FROM_SPEND_LOGS_KEY_SQL, _SEED_FROM_SPEND_LOGS_KEY_UNBOUNDED_SQL),
    ids=("windowed", "unbounded"),
)
def test_budget_window_seed_reads_spend_from_the_index_alone(seeded_database: str, seed_sql: str) -> None:
    with psycopg.connect(seeded_database, autocommit=True) as conn:
        scans: Final = _scan_nodes(_explain(conn, seed_sql))

    assert [(scan.node_type, scan.index_name) for scan in scans] == [("Index Only Scan", COVERING_INDEX)]
    assert scans[0].heap_fetches == 0


@requires_db
def test_spend_is_the_last_key_column_so_the_range_scan_still_leads(seeded_database: str) -> None:
    """spend must trail api_key and startTime: leading it would strand the
    window's range predicate and cost the seek the index exists for."""
    with psycopg.connect(seeded_database, autocommit=True) as conn:
        columns: Final = conn.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_attribute a ON a.attrelid = i.indexrelid "
            "WHERE c.relname = %s ORDER BY a.attnum",
            (COVERING_INDEX,),
        ).fetchall()

    assert [column for (column,) in columns] == ["api_key", "startTime", "spend"]
