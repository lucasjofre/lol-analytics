"""Stage 2b: fetch match details and timelines for pending ids, in batches, under a time budget."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone

from lol_analytics.crawl.discover import platform_of
from lol_analytics.riot.client import NotFound, RiotApiError
from lol_analytics.riot.endpoints import RiotApi
from lol_analytics.riot.routing import region_for

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MatchRow:
    match_id: str
    platform: str
    region: str
    fetched_at: datetime
    payload: str


@dataclass(frozen=True)
class TimelineRow:
    match_id: str
    platform: str
    region: str
    fetched_at: datetime
    status: str  # "ok" | "not_found"
    payload: str | None


@dataclass
class Batch:
    matches: list[MatchRow] = field(default_factory=list)
    timelines: list[TimelineRow] = field(default_factory=list)
    errors: int = 0


def _dump(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


def fetch_batches(
    api: RiotApi,
    pending: list[str],
    *,
    fetch_timelines: bool,
    batch_size: int,
    deadline: float | None,
    clock=time.monotonic,
) -> Iterator[Batch]:
    """Yield batches of rows. Stops early when `deadline` (monotonic seconds) passes.

    A missing timeline (older than Riot's one-year retention) is recorded with
    status not_found so the anti-join does not retry it every day.
    """
    batch = Batch()
    for i, match_id in enumerate(pending):
        if deadline is not None and clock() >= deadline:
            log.info("time budget reached after %d of %d matches", i, len(pending))
            break
        platform = platform_of(match_id)
        region = region_for(platform)
        now = datetime.now(timezone.utc)
        try:
            match = api.match(platform, match_id)
        except NotFound:
            log.warning("match %s not found", match_id)
            continue
        except RiotApiError as exc:
            log.error("match %s failed: %s", match_id, exc)
            batch.errors += 1
            continue
        batch.matches.append(MatchRow(match_id, platform, region, now, _dump(match)))

        if fetch_timelines:
            try:
                timeline = api.timeline(platform, match_id)
                batch.timelines.append(TimelineRow(match_id, platform, region, now, "ok", _dump(timeline)))
            except NotFound:
                batch.timelines.append(TimelineRow(match_id, platform, region, now, "not_found", None))
            except RiotApiError as exc:
                log.error("timeline %s failed: %s", match_id, exc)
                batch.errors += 1

        if len(batch.matches) >= batch_size:
            yield batch
            batch = Batch()
    if batch.matches or batch.timelines:
        yield batch
