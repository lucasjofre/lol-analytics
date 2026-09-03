"""Daily crawl job: fetch new matches for each account and land them in bronze.

Streams batches straight into Delta so peak memory stays flat and an
interrupted run keeps whatever it already wrote.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from lol_analytics.client import RiotClient
from lol_analytics.crawl import crawl_batches, list_match_ids, resolve_puuid
from lol_analytics.ingest import existing_match_ids, write_bronze

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("crawl")

ACCOUNTS = [
    ("br1", "Humper", "humpe"),
]

SECRET_SCOPE = "lol"
SECRET_KEY = "api_keys"


def get_keys(spark: SparkSession) -> list[str]:
    from pyspark.dbutils import DBUtils

    raw = DBUtils(spark).secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    return [k.strip() for k in raw.split(",") if k.strip()]


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    client = RiotClient(get_keys(spark))

    for platform, game_name, tag_line in ACCOUNTS:
        puuid = resolve_puuid(client, platform, game_name, tag_line)
        already = existing_match_ids(spark, platform)
        todo = [m for m in list_match_ids(client, platform, puuid) if m not in already]
        log.info("%s#%s: %d new matches (%d already stored)", game_name, tag_line, len(todo), len(already))

        written = 0
        for batch in crawl_batches(client, platform, todo):
            write_bronze(spark, batch, platform)
            written += len(batch)
            log.info("  %d/%d written", written, len(todo))

        log.info("%s#%s: done, %d written", game_name, tag_line, written)


if __name__ == "__main__":
    main()
