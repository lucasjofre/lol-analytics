"""Stage 2a: list match ids for every target and drop the ones bronze already holds."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from lol_analytics.crawl.targets import Target
from lol_analytics.riot.client import NotFound
from lol_analytics.riot.endpoints import RiotApi

log = logging.getLogger(__name__)


def platform_of(match_id: str) -> str:
    """Match ids are '<PLATFORM>_<number>', e.g. BR1_2987654321."""
    return match_id.split("_", 1)[0].lower()


def discover(
    api: RiotApi,
    targets: Iterable[Target],
    seen: set[str],
    *,
    queue_id: int | None,
    start_time: int | None,
) -> list[str]:
    """Return unseen match ids across all targets, deduplicated, in discovery order."""
    pending: dict[str, None] = {}
    listed = 0
    for target in targets:
        try:
            ids = api.iter_match_ids(
                target.platform,
                target.puuid,
                max_games=target.max_games,
                queue=queue_id,
                start_time=start_time,
            )
            for match_id in ids:
                listed += 1
                if match_id not in seen:
                    pending.setdefault(match_id)
        except NotFound:
            log.warning("puuid %s not found on %s, skipping", target.puuid[:8], target.platform)
    log.info("listed %d ids, %d new", listed, len(pending))
    return list(pending)
