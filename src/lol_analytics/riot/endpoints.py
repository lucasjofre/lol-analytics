"""Typed wrappers for the endpoints this project uses."""

from __future__ import annotations

from typing import Any, Iterator

from lol_analytics.riot.client import RiotClient
from lol_analytics.riot.routing import platform_host, regional_host

APEX_TIERS = ("CHALLENGER", "GRANDMASTER", "MASTER")
_APEX_PATH = {
    "CHALLENGER": "challengerleagues",
    "GRANDMASTER": "grandmasterleagues",
    "MASTER": "masterleagues",
}


class RiotApi:
    def __init__(self, client: RiotClient) -> None:
        self.client = client

    # account-v1 (regional)
    def account_by_riot_id(self, platform: str, game_name: str, tag_line: str) -> dict[str, Any]:
        return self.client.get(
            regional_host(platform),
            f"/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}",
        )

    def account_by_puuid(self, platform: str, puuid: str) -> dict[str, Any]:
        return self.client.get(regional_host(platform), f"/riot/account/v1/accounts/by-puuid/{puuid}")

    # summoner-v4 / league-v4 (platform)
    def summoner_by_puuid(self, platform: str, puuid: str) -> dict[str, Any]:
        return self.client.get(platform_host(platform), f"/lol/summoner/v4/summoners/by-puuid/{puuid}")

    def league_entries_by_puuid(self, platform: str, puuid: str) -> list[dict[str, Any]]:
        return self.client.get(platform_host(platform), f"/lol/league/v4/entries/by-puuid/{puuid}")

    def apex_league(self, platform: str, tier: str, queue: str) -> dict[str, Any]:
        return self.client.get(platform_host(platform), f"/lol/league/v4/{_APEX_PATH[tier]}/by-queue/{queue}")

    # league-exp-v4 (platform)
    def league_exp_page(self, platform: str, queue: str, tier: str, division: str, page: int) -> list[dict[str, Any]]:
        return self.client.get(
            platform_host(platform),
            f"/lol/league-exp/v4/entries/{queue}/{tier}/{division}",
            params={"page": page},
        )

    def iter_league_entries(self, platform: str, queue: str, tier: str, division: str = "I") -> Iterator[dict[str, Any]]:
        """Yield every ladder entry for a tier/division. Apex tiers come from one league-v4 call."""
        if tier in APEX_TIERS:
            league = self.apex_league(platform, tier, queue)
            for entry in league.get("entries", []):
                yield {**entry, "tier": tier, "queueType": queue}
            return
        page = 1
        while True:
            entries = self.league_exp_page(platform, queue, tier, division, page)
            if not entries:
                return
            yield from entries
            page += 1

    # match-v5 (regional)
    def match_ids(
        self,
        platform: str,
        puuid: str,
        *,
        start: int = 0,
        count: int = 100,
        queue: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[str]:
        params: dict[str, Any] = {"start": start, "count": count}
        if queue is not None:
            params["queue"] = queue
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.client.get(regional_host(platform), f"/lol/match/v5/matches/by-puuid/{puuid}/ids", params)

    def iter_match_ids(self, platform: str, puuid: str, *, max_games: int, **filters: Any) -> Iterator[str]:
        start = 0
        while start < max_games:
            count = min(100, max_games - start)
            batch = self.match_ids(platform, puuid, start=start, count=count, **filters)
            yield from batch
            if len(batch) < count:
                return
            start += count

    def match(self, platform: str, match_id: str) -> dict[str, Any]:
        return self.client.get(regional_host(platform), f"/lol/match/v5/matches/{match_id}")

    def timeline(self, platform: str, match_id: str) -> dict[str, Any]:
        return self.client.get(regional_host(platform), f"/lol/match/v5/matches/{match_id}/timeline")
