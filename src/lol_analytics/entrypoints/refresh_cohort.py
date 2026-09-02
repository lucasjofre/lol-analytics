"""Job task 1: resolve cohorts and personal accounts into bronze.targets."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from lol_analytics.crawl.config import load_config
from lol_analytics.crawl.targets import resolve_cohorts, resolve_personal
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
    cohort_targets, entries = resolve_cohorts(api, config.cohorts)
    personal_targets = resolve_personal(api, config.personal)

    # personal accounts override cohort membership so their max_games applies
    merged = {t.puuid: t for t in cohort_targets}
    merged.update({t.puuid: t for t in personal_targets})
    targets = list(merged.values())

    store.append_league_entries(entries)
    store.replace_targets(targets)
    store.append_run(
        {
            "run_id": str(uuid.uuid4()),
            "task": "refresh_cohort",
            "config_name": config.name,
            "started_at": started,
            "finished_at": datetime.now(timezone.utc),
            "targets": len(targets),
            "ids_discovered": 0,
            "matches_fetched": 0,
            "timelines_fetched": 0,
            "errors": 0,
            "api_calls": api.client.limiter.calls,
            "stop_reason": "completed",
        }
    )
    log.info("wrote %d targets (%d cohort, %d personal), %d league entries",
             len(targets), len(cohort_targets), len(personal_targets), len(entries))


if __name__ == "__main__":
    main()
