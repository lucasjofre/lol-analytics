"""Weekly full-ladder snapshot: who sits at what rank, into bronze.

This is what the cohort crawl picks its accounts from - it reads these bronze
rows rather than paging the ladder itself, so the two jobs share the ladder
cost instead of each paying it.
"""

from __future__ import annotations

import logging
from collections import Counter

from pyspark.sql import SparkSession

from lol_analytics.client import APEX_TIERS, DIVISION_TIERS, RiotClient, get_keys
from lol_analytics.crawl import discover_cohort
from lol_analytics.ingest import write_league_entries

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("ladder")

PLATFORM = "br1"


def main() -> None:
    """Snapshot every ranked solo account on the platform into bronze.

    Measured 1,194,692 accounts on br1 at ~205 per call, so the whole ladder is
    ~5.9k calls - under 30 min on 4 keys, cheap enough to run weekly. Written
    one division at a time so peak memory stays at one bucket (~100k rows at
    worst) and a failure keeps everything already written.
    """
    spark = SparkSession.builder.getOrCreate()
    client = RiotClient(get_keys(spark))

    buckets = [(t, d) for t in DIVISION_TIERS for d in ("I", "II", "III", "IV")]
    buckets += [(t, "I") for t in APEX_TIERS]
    log.info("ladder snapshot: %s, %d tier/division buckets", PLATFORM, len(buckets))

    total = 0
    by_tier: Counter = Counter()
    for n, (tier, division) in enumerate(buckets, 1):
        entries = discover_cohort(client, PLATFORM, tier, (division,))
        total += write_league_entries(spark, entries, PLATFORM)
        by_tier[tier] += len(entries)
        log.info("  [%2d/%d] %-12s %-4s %7d accounts (%d so far)",
                 n, len(buckets), tier, division, len(entries), total)

    log.info("ladder snapshot done: %d accounts on %s", total, PLATFORM)
    log.info("  tier breakdown: %s", dict(by_tier))


if __name__ == "__main__":
    main()
