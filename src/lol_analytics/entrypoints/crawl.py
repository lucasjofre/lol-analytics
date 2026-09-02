"""Job task 2: discover new match ids for every target and fetch details plus timelines."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from lol_analytics.crawl.config import load_config
from lol_analytics.crawl.discover import discover
from lol_analytics.crawl.fetch import fetch_batches
from lol_analytics.entrypoints import common
from lol_analytics.ingest.bronze import BronzeStore

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    common.configure_logging()
    args = common.parser(__doc__).parse_args(argv)
    config = load_config(args.config)
    spark = common.get_spark()
    api = common.build_api(common.get_riot_api_key(spark, args.secret_scope, args.secret_key))
    store = BronzeStore(spark, args.catalog)
    store.ensure_tables()

    started = datetime.now(timezone.utc)
    deadline = time.monotonic() + config.time_budget_minutes * 60
    since = int((started - timedelta(days=config.since_days)).timestamp())

    targets = store.load_targets()
    seen = store.seen_match_ids()
    log.info("%d targets, %d matches already in bronze", len(targets), len(seen))

    pending = discover(api, targets, seen, queue_id=config.queue_id, start_time=since)

    matches = timelines = errors = 0
    stop_reason = "completed"
    for batch in fetch_batches(
        api,
        pending,
        fetch_timelines=config.fetch_timelines,
        batch_size=config.batch_size,
        deadline=deadline,
    ):
        matches += store.append_matches(batch.matches)
        timelines += store.append_timelines(batch.timelines)
        errors += batch.errors
        log.info("progress: %d/%d matches, %d timelines, %d errors, %d api calls",
                 matches, len(pending), timelines, errors, api.client.limiter.calls)
    if matches < len(pending):
        stop_reason = "time_budget"

    store.append_run(
        {
            "run_id": str(uuid.uuid4()),
            "task": "crawl",
            "config_name": config.name,
            "started_at": started,
            "finished_at": datetime.now(timezone.utc),
            "targets": len(targets),
            "ids_discovered": len(pending),
            "matches_fetched": matches,
            "timelines_fetched": timelines,
            "errors": errors,
            "api_calls": api.client.limiter.calls,
            "stop_reason": stop_reason,
        }
    )
    log.info("done: %s, %d matches, %d timelines", stop_reason, matches, timelines)


if __name__ == "__main__":
    main()
