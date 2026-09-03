"""Write crawled matches to bronze Delta tables.

Bronze keeps Riot's JSON verbatim in a string column. Parsing it into
columns is dbt's job, so a change to Riot's schema never breaks ingestion.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

MATCHES_TABLE = "matches"
TIMELINES_TABLE = "timelines"
LEAGUE_TABLE = "league_entries"

SCHEMA = "match_id string, platform string, fetched_at timestamp, payload string"

LEAGUE_SCHEMA = (
    "puuid string, platform string, queue_type string, tier string, division string, "
    "league_points int, wins int, losses int, inactive boolean, fetched_at timestamp"
)


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


def write_league_entries(spark, entries: list[dict], platform: str, schema: str = "lol.bronze") -> int:
    """Append a ladder snapshot.

    Kept per run rather than overwritten - the history of who sat at what rank
    on which day is data the API can't give you retroactively.

    Note the puuids here are encrypted under the client's primary key; they
    only work with that same key.
    """
    fetched_at = datetime.now(timezone.utc)
    rows = [
        (
            e["puuid"],
            platform,
            e.get("queueType"),
            e.get("tier"),
            e.get("rank"),
            e.get("leaguePoints"),
            e.get("wins"),
            e.get("losses"),
            e.get("inactive"),
            fetched_at,
        )
        for e in entries
    ]
    spark.createDataFrame(rows, LEAGUE_SCHEMA).write.mode("append").saveAsTable(
        f"{schema}.{LEAGUE_TABLE}"
    )
    return len(rows)


def existing_match_ids(spark, platform: str, schema: str = "lol.bronze") -> set[str]:
    """Match ids already in bronze, so a re-run doesn't refetch them."""
    if not spark.catalog.tableExists(f"{schema}.{MATCHES_TABLE}"):
        return set()
    rows = spark.sql(
        f"select match_id from {schema}.{MATCHES_TABLE} where platform = '{platform}'"
    ).collect()
    return {r.match_id for r in rows}
