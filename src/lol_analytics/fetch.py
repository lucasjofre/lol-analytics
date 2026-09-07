"""What to fetch from Riot, and in what order."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

from lol_analytics.client import RiotClient

PAGE_SIZE = 100  # Riot's max per call


def list_match_ids(
    client: RiotClient,
    platform: str,
    puuid: str,
    max_games: int | None = None,
    queue: int | None = None,
    start_time: int | None = None,
) -> list[str]:
    """Match ids for this puuid, newest first.

    Ids are ~15 bytes each, so even a full history is trivial to hold.
    max_games=None (the default) walks everything; start_time (epoch seconds)
    limits it to matches after that moment, which is how the cohort job keeps
    each account to a single call per day.
    """
    match_ids: list[str] = []
    start = 0
    while max_games is None or len(match_ids) < max_games:
        batch = client.get_match_ids(
            platform, puuid, start=start, count=PAGE_SIZE, queue=queue, start_time=start_time
        )
        match_ids.extend(batch)
        if len(batch) < PAGE_SIZE:
            break  # fewer than a full page means there's nothing left
        start += PAGE_SIZE
    return match_ids if max_games is None else match_ids[:max_games]


def fetch_matches(
    client: RiotClient,
    platform: str,
    match_ids: list[str],
    batch_size: int = 50,
) -> Iterator[list[dict]]:
    """Fetch match details + timeline, yielding a batch at a time.

    Peak memory stays at one batch (~600KB per timeline) instead of the whole
    history, and whatever the caller has already written stays durable if the
    run dies partway.

    One worker per key, each pinned to its own key. A single thread rotating
    keys can only wait on one reply at a time, so the other keys' quotas sit
    idle - measured 2.4 calls/s against a 3.3 ceiling with 4 keys. Pinning by
    index also keeps threads off the non-atomic _next_key counter.
    """
    def fetch(item: tuple[int, str]) -> dict:
        i, match_id = item
        key = client.keys[i % len(client.keys)]
        return {
            "match_id": match_id,
            "match": client.get_match(platform, match_id, key=key),
            "timeline": client.get_timeline(platform, match_id, key=key),
        }

    with ThreadPoolExecutor(max_workers=len(client.keys)) as pool:
        for start in range(0, len(match_ids), batch_size):
            chunk = list(enumerate(match_ids[start:start + batch_size], start))
            yield list(pool.map(fetch, chunk))
