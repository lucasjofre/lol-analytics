"""Riot routing: platform hosts (summoner, league, spectator) vs regional hosts (account, match)."""

PLATFORM_TO_REGION: dict[str, str] = {
    "na1": "americas",
    "br1": "americas",
    "la1": "americas",
    "la2": "americas",
    "euw1": "europe",
    "eun1": "europe",
    "tr1": "europe",
    "ru": "europe",
    "me1": "europe",
    "kr": "asia",
    "jp1": "asia",
    "oc1": "sea",
    "ph2": "sea",
    "sg2": "sea",
    "th2": "sea",
    "tw2": "sea",
    "vn2": "sea",
}

REGIONS = frozenset(PLATFORM_TO_REGION.values())


def region_for(platform: str) -> str:
    try:
        return PLATFORM_TO_REGION[platform.lower()]
    except KeyError as exc:
        raise ValueError(f"unknown platform {platform!r}") from exc


def platform_host(platform: str) -> str:
    region_for(platform)  # validates
    return f"{platform.lower()}.api.riotgames.com"


def regional_host(platform_or_region: str) -> str:
    key = platform_or_region.lower()
    region = key if key in REGIONS else region_for(key)
    return f"{region}.api.riotgames.com"
