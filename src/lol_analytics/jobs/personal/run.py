"""Personal-account ingest: full history for the Riot IDs given.

Unlike the cohort job this does walk history - a handful of accounts is cheap
enough that there's no reason to only look forward.

Streams batches straight into Delta so peak memory stays flat and an
interrupted run keeps whatever it already wrote.

What this run does
------------------
1. parse_riot_ids()          split --riot-ids into Name/TAG pairs
2. For each Riot ID:
     a. get_account()        resolve the Riot ID to a puuid
                             (1 call, pinned key)
     b. list_match_ids()     the whole match history
                             (100 ids per call, pinned key)
     c. unseen_match_ids()   anti-join bronze -> only the new ids
     d. For each batch of 50 new matches:
          fetch_matches()    details + timeline
                             (2 calls per match, one worker per key)
          write_bronze()     append both            <- checkpoint

No lookback and no call budget: a handful of accounts is cheap enough to walk
in full, so the only stopping condition is running out of history.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

from lol_analytics.client import RiotClient, get_keys
from lol_analytics.fetch import fetch_matches, list_match_ids
from lol_analytics.ingest import unseen_match_ids, write_bronze

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("personal")


def parse_riot_ids(spec: str) -> list[tuple[str, str]]:
    """'Name#BR1,Other#1234' -> [('Name', 'BR1'), ('Other', '1234')]."""
    accounts = []
    for raw in spec.split(","):
        game_name, _, tag_line = raw.strip().partition("#")
        accounts.append((game_name, tag_line))
    return accounts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    # One platform per run: a Riot ID means nothing without knowing which
    # server it's on, and accounts split across servers are just two runs.
    p.add_argument("--platform", default="br1",
                    help="server code, e.g. 'br1', 'na1', 'euw1', 'kr'")
    p.add_argument("--riot-ids", default="Humper#humpe",
                    help="comma-separated Riot IDs, e.g. 'Name#BR1,Other#1234'")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    accounts = parse_riot_ids(args.riot_ids)
    log.info("personal ingest: platform=%s accounts=%s",
             args.platform, ", ".join(f"{n}#{t}" for n, t in accounts))

    spark = SparkSession.builder.getOrCreate()
    client = RiotClient(get_keys(spark))

    for game_name, tag_line in accounts:
        puuid = client.get_account(args.platform, game_name, tag_line)["puuid"]
        history = list_match_ids(client, args.platform, puuid)
        todo = unseen_match_ids(spark, history, args.platform)
        log.info("%s#%s: %d matches in history, %d new",
                 game_name, tag_line, len(history), len(todo))

        written = 0
        for batch in fetch_matches(client, args.platform, todo):
            write_bronze(spark, batch, args.platform)
            written += len(batch)
            log.info("  %d/%d written", written, len(todo))

        log.info("%s#%s: done, %d written", game_name, tag_line, written)


if __name__ == "__main__":
    main()
