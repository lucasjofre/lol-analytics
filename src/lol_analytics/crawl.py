"""Crawl orchestration: what to fetch and in what order, for one account."""

from __future__ import annotations

from lol_analytics.client import RiotClient


def crawl_player(
    client: RiotClient,
    platform: str,
    game_name: str,
    tag_line: str,
    count: int = 20,
    queue: int | None = None,
) -> list[dict]:
    """Riot ID -> puuid -> match ids -> match details + timeline for each."""
    account = client.get_account(platform, game_name, tag_line)
    puuid = account["puuid"]

    match_ids = client.get_match_ids(platform, puuid, count=count, queue=queue)

    matches = []
    for match_id in match_ids:
        matches.append({
            "match_id": match_id,
            "match": client.get_match(platform, match_id),
            "timeline": client.get_timeline(platform, match_id),
        })
    return matches
