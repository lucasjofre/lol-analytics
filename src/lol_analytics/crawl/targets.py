"""Stage 1: turn cohorts and personal accounts into a list of puuids to follow."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lol_analytics.crawl.config import Cohort, Personal
from lol_analytics.riot.endpoints import APEX_TIERS, RiotApi

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Target:
    puuid: str
    platform: str
    source: str  # "cohort" | "personal"
    name: str  # cohort name or personal name
    max_games: int


@dataclass(frozen=True)
class LeagueEntryRow:
    snapshot_date: str
    platform: str
    queue: str
    tier: str
    rank: str
    puuid: str
    league_points: int
    wins: int
    losses: int
    payload: str


def resolve_cohorts(api: RiotApi, cohorts: list[Cohort]) -> tuple[list[Target], list[LeagueEntryRow]]:
    today = datetime.now(timezone.utc).date().isoformat()
    targets: dict[str, Target] = {}
    entries: list[LeagueEntryRow] = []
    for cohort in cohorts:
        for tier in cohort.tiers:
            divisions = ["I"] if tier in APEX_TIERS else cohort.divisions
            for division in divisions:
                n = 0
                for entry in api.iter_league_entries(cohort.platform, cohort.queue, tier, division):
                    puuid = entry.get("puuid")
                    if not puuid:
                        continue
                    n += 1
                    entries.append(_entry_row(today, cohort, tier, entry))
                    # first cohort to claim a puuid wins; apex players listed once
                    targets.setdefault(
                        puuid,
                        Target(puuid, cohort.platform, "cohort", cohort.name, cohort.max_games_per_account),
                    )
                log.info("cohort %s %s %s: %d entries", cohort.name, tier, division, n)
    return list(targets.values()), entries


def _entry_row(today: str, cohort: Cohort, tier: str, entry: dict[str, Any]) -> LeagueEntryRow:
    return LeagueEntryRow(
        snapshot_date=today,
        platform=cohort.platform,
        queue=cohort.queue,
        tier=tier,
        rank=entry.get("rank", "I"),
        puuid=entry["puuid"],
        league_points=int(entry.get("leaguePoints", 0)),
        wins=int(entry.get("wins", 0)),
        losses=int(entry.get("losses", 0)),
        payload=json.dumps(entry, separators=(",", ":")),
    )


def resolve_personal(api: RiotApi, accounts: list[Personal]) -> list[Target]:
    targets = []
    for acc in accounts:
        account = api.account_by_riot_id(acc.platform, acc.game_name, acc.tag_line)
        targets.append(Target(account["puuid"], acc.platform, "personal", acc.name, acc.max_games))
        log.info("personal %s -> %s", acc.name, account["puuid"][:8])
    return targets
