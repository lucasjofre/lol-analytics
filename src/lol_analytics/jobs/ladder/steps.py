"""The steps the ladder snapshot runs: page the ladder, write the snapshot.

The league_entries table name is shared - cohort reads these rows back - so it
lives in the root ingest module. Only the write schema is the ladder's own.
"""

from __future__ import annotations

from datetime import datetime, timezone

from lol_analytics.client import RiotClient
from lol_analytics.ingest import LEAGUE_TABLE

LEAGUE_SCHEMA = (
    "puuid string, platform string, queue_type string, tier string, division string, "
    "league_points int, wins int, losses int, inactive boolean, fetched_at timestamp"
)


def list_league_entries(
    client: RiotClient,
    platform: str,
    tier: str,
    divisions: tuple[str, ...] = ("I", "II", "III", "IV"),
    queue: str = "RANKED_SOLO_5x5",
    max_accounts: int | None = None,
) -> list[dict]:
    """Page the ladder for a tier, returning entries with puuid, rank and LP.

    ~205 accounts per call, so discovery is cheap next to the per-account
    listing that follows it.
    """
    entries: list[dict] = []
    for division in divisions:
        page = 1
        while max_accounts is None or len(entries) < max_accounts:
            batch = client.get_league_entries(platform, tier, division, page, queue)
            if not batch:
                break  # empty page means the division is exhausted
            entries.extend(batch)
            page += 1
        if max_accounts is not None and len(entries) >= max_accounts:
            break
    return entries if max_accounts is None else entries[:max_accounts]


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
