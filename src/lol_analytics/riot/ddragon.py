"""Data Dragon static data. Unauthenticated and not rate limited."""

from __future__ import annotations

from typing import Any

import requests

BASE = "https://ddragon.leagueoflegends.com"
FILES = ("champion", "item", "runesReforged", "summoner")


def versions(session: requests.Session | None = None) -> list[str]:
    s = session or requests.Session()
    resp = s.get(f"{BASE}/api/versions.json", timeout=30)
    resp.raise_for_status()
    return resp.json()


def static_file(version: str, name: str, locale: str = "en_US", session: requests.Session | None = None) -> Any:
    s = session or requests.Session()
    resp = s.get(f"{BASE}/cdn/{version}/data/{locale}/{name}.json", timeout=60)
    resp.raise_for_status()
    return resp.json()


def game_version_to_ddragon(game_version: str, known: list[str]) -> str | None:
    """Map a match gameVersion like '16.17.712.1234' to the ddragon version '16.17.1'.

    `known` must be newest-first, as returned by `versions()`.
    """
    major_minor = ".".join(game_version.split(".")[:2])
    for v in known:
        if v.startswith(major_minor + "."):
            return v
    return None
