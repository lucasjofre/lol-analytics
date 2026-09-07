"""Write fetched matches to bronze Delta tables.

Bronze keeps Riot's JSON verbatim in a string column. Parsing it into
columns is dbt's job, so a change to Riot's schema never breaks ingestion.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

MATCHES_TABLE = "matches"
TIMELINES_TABLE = "timelines"
LEAGUE_TABLE = "league_entries"  # ladder writes it, cohort reads it

SCHEMA = "match_id string, platform string, fetched_at timestamp, payload string"


def _rows(matches: list[dict], platform: str, payload_key: str) -> list[tuple]:
    fetched_at = datetime.now(timezone.utc)
    return [
        (m["match_id"], platform, fetched_at, json.dumps(m[payload_key]))
        for m in matches
    ]


def write_bronze(
    spark,
    matches: list[dict],
    platform: str,
    schema: str = "lol.bronze",
    batch_size: int = 50,
) -> dict[str, int]:
    """Append match details and timelines to their bronze tables.

    Written in batches: timelines run ~670KB each, and Spark Connect caps a
    single message near 128MB, which one account's full history already
    reaches. Batching also means an interrupted run keeps what it wrote.
    """
    written = {}
    for table, payload_key in ((MATCHES_TABLE, "match"), (TIMELINES_TABLE, "timeline")):
        rows = _rows(matches, platform, payload_key)
        for i in range(0, len(rows), batch_size):
            df = spark.createDataFrame(rows[i : i + batch_size], SCHEMA)
            df.write.mode("append").saveAsTable(f"{schema}.{table}")
        written[table] = len(rows)
    return written


def unseen_match_ids(
    spark, match_ids: list[str], platform: str, schema: str = "lol.bronze"
) -> list[str]:
    """Of these ids, the ones not already in bronze, in the order given.

    An anti-join against just the ids being asked about, rather than pulling
    every stored id to the driver: bronze grows without bound, but a run only
    ever needs to know about the handful of matches it just listed. Reading
    the table each time also means this sees what the same run already wrote,
    so callers don't have to track that themselves.
    """
    if not match_ids or not spark.catalog.tableExists(f"{schema}.{MATCHES_TABLE}"):
        return match_ids
    candidates = spark.createDataFrame([(m,) for m in match_ids], "match_id string")
    stored = spark.sql(
        f"select match_id from {schema}.{MATCHES_TABLE} where platform = '{platform}'"
    )
    known = {r.match_id for r in candidates.join(stored, "match_id", "left_semi").collect()}
    return [m for m in match_ids if m not in known]
