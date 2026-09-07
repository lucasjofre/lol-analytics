"""Daily cohort ingest: everything a ranked cohort played since the last run.

History is not feasible for a cohort this size - one call per account per day
is already the dominant cost - so this only ever looks forward. Backfill stays
a personal-account thing, in personal.py.

The cohort comes from the bronze ladder snapshot that ladder.py writes, not
from a live ladder call.

What this run does
------------------
1. select_accounts()
     - latest_league_entries()   each account's own latest row in the bronze
                                 ladder snapshot, tiers filtered, N ordered by
                                 puuid (stable run to run, unlike LP)
     - recently_listed_puuids()  drop accounts already listed within the
                                 lookback - a restart resumes, not re-lists
2. For each batch of 100 accounts:
     a. list_match_ids()         ids played since the lookback
                                 (1 call per account, pinned key)
     b. write_listed_accounts()  record the batch        <- checkpoint
     c. unseen_match_ids()       anti-join bronze -> only the new ids
     d. For each batch of 50 new matches:
          fetch_matches()        details + timeline
                                 (2 calls per match, one worker per key)
          write_bronze()         append both             <- checkpoint
     e. stop if the call budget is spent - the rest is picked up tomorrow

Every write is a checkpoint, so a kill mid-run only loses the batch in flight.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from lol_analytics.client import TIER_ORDER, RiotClient, get_keys
from lol_analytics.fetch import fetch_matches, list_match_ids
from lol_analytics.ingest import unseen_match_ids, write_bronze
from lol_analytics.jobs.cohort.steps import (
    latest_league_entries,
    recently_listed_puuids,
    write_listed_accounts,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("cohort")

PLATFORM = "br1"

# Accounts listed (pinned key, slow) before their matches get crawled (4 keys,
# written to bronze) and the run checkpoints. Listing 50k accounts can take
# hours on one key; without this, a kill mid-run loses everything listed so
# far, since nothing reaches bronze until every account has been listed.
LISTING_BATCH = 100

CALLS_PER_MATCH = 2  # match details + timeline


def parse_tiers(spec: str) -> tuple[str, ...]:
    """'PLATINUM+' -> Platinum and everything above. 'DIAMOND' -> just Diamond."""
    if spec.endswith("+"):
        base = TIER_ORDER.index(spec[:-1].upper())
        return TIER_ORDER[base:]
    return (spec.upper(),)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tiers", default="DIAMOND", help="e.g. 'DIAMOND' or 'PLATINUM+'")
    p.add_argument("--max-accounts", type=int, default=1000)
    p.add_argument("--lookback-hours", type=int, default=24,
                    help="how far back to search for matches played, not ladder staleness")
    # Stop before the run outgrows its window; whatever is missed is picked up
    # tomorrow, since bronze is the state.
    p.add_argument("--call-budget", type=int, default=12_000)
    return p.parse_args()


def select_accounts(
    spark, tiers: tuple[str, ...], max_accounts: int, lookback_hours: int
) -> list[dict]:
    """The accounts this run will list: latest ladder rows, minus recently listed.

    An account listed within the lookback was already searched for everything
    this run would ask for, so a restart skips it instead of re-spending the
    pinned key's slow, single-key listing calls for no new discovery.
    """
    log.info("resolving each account's own most recent ladder row (per-puuid latest, "
             "not one snapshot date) for tiers=%s", tiers)
    entries = latest_league_entries(spark, PLATFORM, tiers, max_accounts)
    log.info("%d accounts from the latest ladder snapshot in %s", len(entries), tiers)
    if not entries:
        log.info("nothing to ingest - has the ladder job run yet for this platform?")
        return []
    by_tier = Counter(e["tier"] for e in entries)
    log.info("  tier breakdown: %s", dict(sorted(by_tier.items(), key=lambda kv: -kv[1])))

    recently_listed = recently_listed_puuids(spark, PLATFORM, lookback_hours)
    if recently_listed:
        entries = [e for e in entries if e["puuid"] not in recently_listed]
        log.info("%d accounts already listed within the last %dh, skipping - %d left to list",
                  len(recently_listed), lookback_hours, len(entries))
    if not entries:
        log.info("nothing left to list - everything was covered by a previous run")
    return entries


def main() -> None:
    args = parse_args()
    tiers = parse_tiers(args.tiers)
    log.info("cohort: platform=%s tiers=%s max_accounts=%d lookback_hours=%d call_budget=%d",
              PLATFORM, tiers, args.max_accounts, args.lookback_hours, args.call_budget)

    spark = SparkSession.builder.getOrCreate()
    client = RiotClient(get_keys(spark))

    entries = select_accounts(spark, tiers, args.max_accounts, args.lookback_hours)
    if not entries:
        return

    since = int(time.time()) - args.lookback_hours * 3600
    since_str = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    log.info("searching for matches played since %s (lookback=%dh)", since_str, args.lookback_hours)

    # Accounts are listed and their matches crawled batch by batch, not all
    # listing then all crawling, so a kill mid-run only loses the current
    # batch - everything before it is already in bronze. Bronze is also the
    # only record of what's been fetched: unseen_match_ids re-reads it each
    # batch, so a match seen under two accounts is still fetched once without
    # this loop tracking that itself.
    written = 0
    budget_used = 0

    for start in range(0, len(entries), LISTING_BATCH):
        chunk = entries[start:start + LISTING_BATCH]
        found = {
            m
            for e in chunk
            for m in list_match_ids(client, PLATFORM, e["puuid"], start_time=since)
        }
        write_listed_accounts(spark, [e["puuid"] for e in chunk], PLATFORM)
        budget_used += len(chunk)

        affordable = max(0, (args.call_budget - budget_used) // CALLS_PER_MATCH)
        todo = unseen_match_ids(spark, sorted(found), PLATFORM)[:affordable]

        for batch in fetch_matches(client, PLATFORM, todo):
            write_bronze(spark, batch, PLATFORM)
            written += len(batch)
            budget_used += len(batch) * CALLS_PER_MATCH

        listed = min(start + LISTING_BATCH, len(entries))
        log.info("checkpoint: %d/%d accounts listed, %d found this batch, %d new, "
                  "%d written so far, %d/%d call budget used",
                  listed, len(entries), len(found), len(todo), written,
                  budget_used, args.call_budget)

        if budget_used >= args.call_budget:
            log.info("call budget exhausted with %d/%d accounts listed, stopping early - "
                      "the rest are picked up next run", listed, len(entries))
            break

    log.info("done: %d accounts, %d matches written", len(entries), written)


if __name__ == "__main__":
    main()
