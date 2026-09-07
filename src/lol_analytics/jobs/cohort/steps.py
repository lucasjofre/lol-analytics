"""The steps the cohort ingest runs against bronze.

listed_accounts is entirely the cohort's: it writes the checkpoint and reads
it back on a restart. latest_league_entries reads the snapshot that the
ladder job writes - the read lives here because the cohort is its only
consumer, while the table name it needs stays in the root ingest module.
"""

from __future__ import annotations

from datetime import datetime, timezone

from lol_analytics.ingest import LEAGUE_TABLE

LISTED_ACCOUNTS_TABLE = "listed_accounts"
LISTED_ACCOUNTS_SCHEMA = "puuid string, platform string, listed_at timestamp"


def write_listed_accounts(spark, puuids: list[str], platform: str, schema: str = "lol.bronze") -> int:
    """Record that these accounts' matches were just listed.

    Written whether or not any matches were found - most accounts have none
    in a given window, and a restart needs to skip those too, not just the
    ones that turned up something. Shared by any job that lists by puuid, not
    specific to the cohort ingest.
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

    Ordered by puuid, not LP. LP is not comparable across tiers - apex runs to
    thousands while everything below resets 0-100 per division - so ordering by
    it returns nothing but apex accounts for any range reaching Master, and
    ignores division within a single tier. It also moves every game, which
    would churn cohort membership run to run. puuid is arbitrary but fixed, so
    the tier filter is what selects and the same accounts come back each time.
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
        order by puuid
        limit {max_accounts}
    """).collect()
    return [r.asDict() for r in rows]
