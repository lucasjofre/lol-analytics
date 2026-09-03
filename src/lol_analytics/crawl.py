"""Crawl orchestration: what to fetch and in what order, for one account."""

from __future__ import annotations

from lol_analytics.client import RiotClient


PAGE_SIZE = 100  # Riot's max per call


def crawl_player(
    client: RiotClient,
    platform: str,
    game_name: str,
    tag_line: str,
    max_games: int | None = None,
    queue: int | None = None,
) -> list[dict]:
    """Riot ID -> puuid -> all match ids (paginated) -> match details + timeline for each.

    max_games=None (the default) walks the full history Riot still retains.
    """
    account = client.get_account(platform, game_name, tag_line)
    puuid = account["puuid"]

    match_ids = []
    start = 0
    while max_games is None or len(match_ids) < max_games:
        batch = client.get_match_ids(platform, puuid, start=start, count=PAGE_SIZE, queue=queue)
        match_ids.extend(batch)
        if len(batch) < PAGE_SIZE:
            break  # fewer than a full page means there's nothing left
        start += PAGE_SIZE
    if max_games is not None:
        match_ids = match_ids[:max_games]

    matches = []
    for match_id in match_ids:
        matches.append({
            "match_id": match_id,
            "match": client.get_match(platform, match_id),
            "timeline": client.get_timeline(platform, match_id),
        })
    return matches
