"""Riot API client: key rotation, pacing, retries, and the endpoints we use."""

from __future__ import annotations

import time

import requests

PLATFORM_TO_REGION = {
    "na1": "americas",
    "br1": "americas",
    "la1": "americas",
    "la2": "americas",
    "euw1": "europe",
    "eun1": "europe",
    "tr1": "europe",
    "ru": "europe",
    "kr": "asia",
    "jp1": "asia",
}


class RiotClient:
    def __init__(self, keys: list[str]):
        self.keys = keys
        self._next_key = 0

    def get(self, platform: str, path: str, params: dict | None = None, key: str | None = None) -> dict:
        """Make an authenticated GET, retry on 429.

        Riot encrypts puuids per API key - a puuid fetched with one key means
        nothing to another. So any call that takes a puuid must pin a single
        `key` (same one used to obtain that puuid). Calls keyed by matchId
        (universal, not encrypted) can omit `key` and round-robin instead.

        No proactive pacing - just fire and let 429s tell us to back off. If a
        key comes back 429, try the other keys immediately instead of
        sleeping - one blocked key doesn't mean they all are. Only sleep once
        every key has been tried and failed, and then only for as long as the
        soonest one might free up. With a pinned key there's only one to try.
        """
        host = PLATFORM_TO_REGION[platform]
        url = f"https://{host}.api.riotgames.com{path}"
        attempts = 1 if key is not None else len(self.keys)

        while True:
            shortest_wait = None

            for _ in range(attempts):
                if key is not None:
                    use_key = key
                else:
                    key_index = self._next_key
                    self._next_key = (self._next_key + 1) % len(self.keys)
                    use_key = self.keys[key_index]

                resp = requests.get(url, headers={"X-Riot-Token": use_key}, params=params, timeout=30)

                if resp.status_code != 429:
                    resp.raise_for_status()
                    return resp.json()

                retry_after = float(resp.headers.get("Retry-After", 5))
                shortest_wait = retry_after if shortest_wait is None else min(shortest_wait, retry_after)

            time.sleep(shortest_wait)  # every candidate key was 429'd this round

    def get_account(self, platform: str, game_name: str, tag_line: str) -> dict:
        return self.get(
            platform, f"/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}", key=self.keys[0]
        )

    def get_match_ids(self, platform: str, puuid: str, start: int = 0, count: int = 20, queue: int | None = None) -> list[str]:
        params = {"start": start, "count": count}
        if queue is not None:
            params["queue"] = queue
        return self.get(platform, f"/lol/match/v5/matches/by-puuid/{puuid}/ids", params, key=self.keys[0])

    def get_match(self, platform: str, match_id: str) -> dict:
        return self.get(platform, f"/lol/match/v5/matches/{match_id}")

    def get_timeline(self, platform: str, match_id: str) -> dict:
        return self.get(platform, f"/lol/match/v5/matches/{match_id}/timeline")
