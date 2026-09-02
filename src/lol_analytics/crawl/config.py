"""Crawl configuration loaded from config/crawl.<target>.yml."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator

from lol_analytics.riot.routing import region_for

DIVISIONS = ("I", "II", "III", "IV")
RANKED_SOLO_QUEUE_ID = 420


class Cohort(BaseModel):
    """A rank slice on one platform, enumerated daily through league-exp."""

    name: str
    platform: str
    queue: str = "RANKED_SOLO_5x5"
    tiers: list[str]
    divisions: list[str] = list(DIVISIONS)
    max_games_per_account: int = 20

    @field_validator("platform")
    @classmethod
    def _platform_known(cls, v: str) -> str:
        region_for(v)
        return v.lower()

    @field_validator("tiers")
    @classmethod
    def _tiers_upper(cls, v: list[str]) -> list[str]:
        return [t.upper() for t in v]


class Personal(BaseModel):
    """A single account followed regardless of rank."""

    name: str
    platform: str
    game_name: str
    tag_line: str
    max_games: int = 200

    @field_validator("platform")
    @classmethod
    def _platform_known(cls, v: str) -> str:
        region_for(v)
        return v.lower()


class CrawlConfig(BaseModel):
    name: str
    cohorts: list[Cohort] = Field(default_factory=list)
    personal: list[Personal] = Field(default_factory=list)
    queue_id: int | None = RANKED_SOLO_QUEUE_ID
    since_days: int = 365
    time_budget_minutes: int = 480
    batch_size: int = 200
    fetch_timelines: bool = True


def load_config(path: str | Path) -> CrawlConfig:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return CrawlConfig.model_validate(raw)
