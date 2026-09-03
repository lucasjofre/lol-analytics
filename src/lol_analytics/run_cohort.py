"""Daily cohort crawl: everything a tier played since the last run.

History is not feasible for a cohort this size - one call per account per day
is already the dominant cost - so this only ever looks forward. Backfill stays
a personal-account thing.
"""

from __future__ import annotations

import logging
import time

from pyspark.sql import SparkSession

from lol_analytics.client import RiotClient
from lol_analytics.crawl import crawl_batches, discover_cohort, list_match_ids
from lol_analytics.ingest import existing_match_ids, write_bronze, write_league_entries
from lol_analytics.run import get_keys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("cohort")

PLATFORM = "br1"
TIER = "DIAMOND"
DIVISIONS = ("I",)
MAX_ACCOUNTS = 1000
LOOKBACK_HOURS = 24

# Stop before the run outgrows its window; whatever is missed is picked up
# tomorrow, since bronze is the state.
CALL_BUDGET = 12_000


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    client = RiotClient(get_keys(spark))

    entries = discover_cohort(client, PLATFORM, TIER, DIVISIONS, max_accounts=MAX_ACCOUNTS)
    log.info("discovered %d accounts in %s %s", len(entries), TIER, "/".join(DIVISIONS))
    write_league_entries(spark, entries, PLATFORM)

    since = int(time.time()) - LOOKBACK_HOURS * 3600
    already = existing_match_ids(spark, PLATFORM)

    found: set[str] = set()
    for n, entry in enumerate(entries, 1):
        found.update(list_match_ids(client, PLATFORM, entry["puuid"], start_time=since))
        if n % 200 == 0:
            log.info("  listed %d/%d accounts, %d distinct matches", n, len(entries), len(found))

    todo = [m for m in found if m not in already]
    log.info("%d distinct matches, %d new after dedupe", len(found), len(todo))

    budget_left = CALL_BUDGET - len(entries)
    if len(todo) * 2 > budget_left:
        todo = todo[: max(0, budget_left // 2)]
        log.info("trimmed to %d matches to stay inside the call budget", len(todo))

    written = 0
    for batch in crawl_batches(client, PLATFORM, todo):
        write_bronze(spark, batch, PLATFORM)
        written += len(batch)
        log.info("  %d/%d written", written, len(todo))

    log.info("done: %d accounts, %d matches written", len(entries), written)


if __name__ == "__main__":
    main()
