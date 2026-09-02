# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring the Riot API
# MAGIC
# MAGIC Goal: see what the API actually returns before designing any tables.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Riot ID -> puuid
# MAGIC 2. puuid -> list of match ids
# MAGIC 3. match id -> match details (the scoreboard)
# MAGIC 4. match id -> timeline (the minute-by-minute replay)
# MAGIC
# MAGIC Get a dev key at https://developer.riotgames.com (expires every 24h).

# COMMAND ----------

import json

import requests

# Paste your key here for now. Later this moves to a Databricks secret.
API_KEY = ""

GAME_NAME = "CHANGE_ME"  # the part before the #
TAG_LINE = "BR1"  # the part after the #

PLATFORM = "br1"  # summoner / league endpoints
REGION = "americas"  # account / match endpoints (na1, br1, la1, la2 all use americas)

HEADERS = {"X-Riot-Token": API_KEY}


def get(host: str, path: str, **params):
    url = f"https://{host}.api.riotgames.com{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    print(r.status_code, url)
    r.raise_for_status()
    return r.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Riot ID -> puuid
# MAGIC The puuid is the key for everything else.

# COMMAND ----------

account = get(REGION, f"/riot/account/v1/accounts/by-riot-id/{GAME_NAME}/{TAG_LINE}")
PUUID = account["puuid"]
account

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Match id list
# MAGIC Up to 100 per call. `queue=420` is ranked solo/duo. Ids look like `BR1_1234567890`.

# COMMAND ----------

match_ids = get(REGION, f"/lol/match/v5/matches/by-puuid/{PUUID}/ids", start=0, count=20, queue=420)
print(len(match_ids))
match_ids

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Match details
# MAGIC Two top-level keys: `metadata` (ids) and `info` (everything else).
# MAGIC `info.participants` has 10 entries, one per player.

# COMMAND ----------

MATCH_ID = match_ids[0]
match = get(REGION, f"/lol/match/v5/matches/{MATCH_ID}")

print("metadata keys:", list(match["metadata"]))
print("info keys:", list(match["info"]))
print("participant keys:", len(match["info"]["participants"][0]), "fields")

# COMMAND ----------

# A scoreboard view of the 10 players
import pandas as pd

cols = ["riotIdGameName", "teamId", "teamPosition", "championName", "kills", "deaths", "assists",
        "totalMinionsKilled", "goldEarned", "totalDamageDealtToChampions", "visionScore", "win"]
pd.DataFrame(match["info"]["participants"])[cols]

# COMMAND ----------

# Your own row, every field. Scroll through it: this is what one participant record holds.
me = next(p for p in match["info"]["participants"] if p["puuid"] == PUUID)
print(json.dumps(me, indent=2)[:6000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timeline
# MAGIC `info.frames` is one entry per minute. Each frame has:
# MAGIC - `participantFrames`: gold, xp, cs, level, position for each of the 10 players
# MAGIC - `events`: everything that happened in that minute (kills, item buys, objectives...)

# COMMAND ----------

timeline = get(REGION, f"/lol/match/v5/matches/{MATCH_ID}/timeline")
frames = timeline["info"]["frames"]
print("frames:", len(frames), "(roughly game length in minutes)")
print("participantFrame keys:", list(frames[10]["participantFrames"]["1"]))

# COMMAND ----------

# Gold per minute for each player
rows = []
for minute, frame in enumerate(frames):
    for pid, pf in frame["participantFrames"].items():
        rows.append({"minute": minute, "participant": int(pid), "gold": pf["totalGold"], "xp": pf["xp"], "cs": pf["minionsKilled"]})
gold = pd.DataFrame(rows).pivot(index="minute", columns="participant", values="gold")
gold.tail()

# COMMAND ----------

# Which event types exist, and how often
from collections import Counter

events = [e for f in frames for e in f["events"]]
Counter(e["type"] for e in events).most_common()

# COMMAND ----------

# Every champion kill, with position on the map and who was involved
kills = [e for e in events if e["type"] == "CHAMPION_KILL"]
pd.DataFrame(kills)[["timestamp", "killerId", "victimId", "assistingParticipantIds", "position"]].head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Size check
# MAGIC Useful for estimating storage later.

# COMMAND ----------

print("match details:", len(json.dumps(match)) // 1024, "KB")
print("timeline:     ", len(json.dumps(timeline)) // 1024, "KB")
