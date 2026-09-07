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
LISTED_ACCOUNTS_TABLE = "listed_accounts"

SCHEMA = "match_id string, platform string, fetched_at timestamp, payload string"

LEAGUE_SCHEMA = (
    "puuid string, platform string, queue_type string, tier string, division string, "
    "league_points int, wins int, losses int, inactive boolean, fetched_at timestamp"
)

LISTED_ACCOUNTS_SCHEMA = "puuid string, platform string, listed_at timestamp"


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


def write_listed_accounts(spark, puuids: list[str], platform: str, schema: str = "lol.bronze") -> int:
    """Record that these accounts' matches were just listed.

    Written whether or not any matches were found - most accounts have none
    in a given window, and a restart needs to skip those too, not just the
    ones that turned up something. Shared by any job that lists by puuid, not
    specific to the cohort crawl.
    """
    listed_at = datetime.now(timezone.utc)
    rows = [(p, platform, listed_at) for p in puuids]
    spark.createDataFrame(rows, LISTED_ACCOUNTS_SCHEMA).write.mode("append").saveAsTable(
        f"{schema}.{LISTED_ACCOUNTS_TABLE}"
    )
    return len(rows)


def recently_listed_puuids(
    spark, platform: str, within_hours: int, schema: str = "lol.bronze"
) -> set[str]:
    """Puuids already listed within within_hours, so a restart can skip them.

    Pass the same value as the match lookback: an account listed that
    recently was already searched for everything the current run would ask
    for, so re-listing it can only repeat work, not find anything new.
    """
    if not spark.catalog.tableExists(f"{schema}.{LISTED_ACCOUNTS_TABLE}"):
        return set()
    rows = spark.sql(f"""
        select distinct puuid from {schema}.{LISTED_ACCOUNTS_TABLE}
        where platform = '{platform}' and listed_at > now() - interval {within_hours} hours
    """).collect()
    return {r.puuid for r in rows}


def latest_league_entries(
    spark, platform: str, tiers: tuple[str, ...], max_accounts: int, schema: str = "lol.bronze"
) -> list[dict]:
    """Each account's own most recent row, filtered to the requested tiers.

    league_entries is append-only - one row per account per ladder() run - so
    this is a per-puuid latest, not a global one. That also makes it correct
    across a partial ladder() failure (some tiers newer than others) and for
    an account that has since moved out of the requested tiers: its true
    latest row wins and it drops out, instead of matching on a stale one.
    """
    if not spark.catalog.tableExists(f"{schema}.{LEAGUE_TABLE}"):
        return []
    tier_list = ", ".join(f"'{t}'" for t in tiers)
    rows = spark.sql(f"""
        select puuid, tier, league_points from (
            select puuid, tier, league_points,
                   row_number() over (partition by puuid order by fetched_at desc) as rn
            from {schema}.{LEAGUE_TABLE}
            where platform = '{platform}'
        )
        where rn = 1 and tier in ({tier_list})
        order by league_points desc
        limit {max_accounts}
    """).collect()
    return [r.asDict() for r in rows]
