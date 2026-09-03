"""Crawl orchestration: what to fetch and in what order, for one account."""

from __future__ import annotations

from typing import Iterator

from lol_analytics.client import RiotClient

PAGE_SIZE = 100  # Riot's max per call


def resolve_puuid(client: RiotClient, platform: str, game_name: str, tag_line: str) -> str:
    return client.get_account(platform, game_name, tag_line)["puuid"]


def list_match_ids(
    client: RiotClient,
    platform: str,
    puuid: str,
    max_games: int | None = None,
    queue: int | None = None,
) -> list[str]:
    """Every match id Riot still retains for this puuid, newest first.

    Ids are ~15 bytes each, so even a full history is trivial to hold.
    max_games=None (the default) walks everything.
    """
    match_ids: list[str] = []
    start = 0
    while max_games is None or len(match_ids) < max_games:
        batch = client.get_match_ids(platform, puuid, start=start, count=PAGE_SIZE, queue=queue)
        match_ids.extend(batch)
        if len(batch) < PAGE_SIZE:
            break  # fewer than a full page means there's nothing left
        start += PAGE_SIZE
    return match_ids if max_games is None else match_ids[:max_games]


def crawl_batches(
    client: RiotClient,
    platform: str,
    match_ids: list[str],
    batch_size: int = 50,
) -> Iterator[list[dict]]:
    """Fetch match details + timeline, yielding a batch at a time.

    Peak memory stays at one batch (~600KB per timeline) instead of the whole
    history, and whatever the caller has already written stays durable if the
    run dies partway.
    """
    batch: list[dict] = []
    for match_id in match_ids:
        batch.append({
            "match_id": match_id,
            "match": client.get_match(platform, match_id),
            "timeline": client.get_timeline(platform, match_id),
        })
        if len(batch) == batch_size:
            yield batch
            batch = []  # drops the caller's reference; the old batch can be freed
    if batch:
        yield batch


def crawl_player(
    client: RiotClient,
    platform: str,
    game_name: str,
    tag_line: str,
    max_games: int | None = None,
    queue: int | None = None,
) -> list[dict]:
    """Everything for one Riot ID, in one list. Convenient for exploration.

    Holds the full history in memory - fine for a single account, but jobs
    should drive crawl_batches directly and write as they go.
    """
    puuid = resolve_puuid(client, platform, game_name, tag_line)
    match_ids = list_match_ids(client, platform, puuid, max_games, queue)
    return [m for batch in crawl_batches(client, platform, match_ids) for m in batch]
