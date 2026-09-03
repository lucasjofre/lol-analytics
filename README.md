# lol-analytics

League of Legends match analytics: a Riot API crawler landing raw JSON in
Delta, with dbt models on top. Runs on Databricks (Free Edition, serverless).

## Setup

```bash
uv sync
```

Keys go in `.env` (gitignored), comma-separated:

```
API_KEYS=RGAPI-first-key,RGAPI-second-key
```

Register keys at [developer.riotgames.com](https://developer.riotgames.com).
A *development* key expires every 24h; a *personal* key doesn't (same rate
limits, needs a short project description, light review).

## Usage

```python
from lol_analytics.client import RiotClient
from lol_analytics.crawl import crawl_batches, list_match_ids, resolve_puuid
from lol_analytics.ingest import existing_match_ids, write_bronze

client = RiotClient(keys)
puuid = resolve_puuid(client, "br1", "GameName", "TAG")

already = existing_match_ids(spark, "br1")
todo = [m for m in list_match_ids(client, "br1", puuid) if m not in already]

for batch in crawl_batches(client, "br1", todo):
    write_bronze(spark, batch, "br1")
```

`crawl_player()` returns everything in one list instead - convenient in a
notebook, but it holds the whole history in memory, so jobs should use
`crawl_batches` as above.

## Layout

```
src/lol_analytics/
  client.py   # Riot API: key rotation, retries. One call at a time.
  crawl.py    # What to fetch, in what order. Streams batches.
  ingest.py   # Bronze Delta writes.
dbt/          # Models over the bronze JSON (stock dbt init so far)
notebooks/    # API exploration
```

## Riot API constraints

These are measured against the live API, not guessed.

### puuids are encrypted per API key

The same account returns a **different** puuid depending which key asked, and
a puuid only decrypts with the key that issued it. Cross-key reuse returns
`400 Bad Request - Exception decrypting`. It's a privacy measure - it stops
two apps correlating the same player - and it shapes the multi-key design:

| Call type | Key handling |
|---|---|
| Takes a **puuid** (account, match id list, league entries) | Pinned to one key (`keys[0]`) |
| Takes a **matchId** (match details, timeline) | Any key - matchIds are universal |

matchId-based calls are ~99% of volume, so round-robin still does most of the
work. Any future cohort or fan-out discovery inherits this rule: whichever key
discovered a puuid is the only key that can use it.

**This leaks into stored match payloads.** Since match fetches round-robin,
each bronze row was fetched by whichever key was next, so participant
identifiers are not comparable *between rows*. Diffing one match fetched with
two keys, exactly these vary:

| Varies by key | Stable |
|---|---|
| `participants[].puuid` | `riotIdGameName` / `riotIdTagline` |
| `participants[].summonerId` | `championName`, `championId`, all stats |
| `metadata.participants[]` | |

**Use `riotIdGameName` + `riotIdTagline` as player identity** (e.g.
`EsquiiiLo#BR1`). Never join across rows on `puuid` or `summonerId` - it will
silently produce wrong results.

The tradeoff: Riot IDs are renameable, so a player who renames appears as two
identities. puuid would solve that but is key-scoped. No identifier is both
stable across keys and across renames. Accepted, since renames are rare.
For fan-out crawling later, resolve a Riot ID back to a puuid with the pinned
key at crawl time (one extra call) rather than pinning all match fetches,
which would halve throughput.

### Rate limits are per key, so parallelism buys nothing

The ceiling is `n_keys x limit` whether one process rotates keys or N
processes each own one. A personal/dev key allows 20 req/s and 100 req/120s,
i.e. one request per 1.2s per key.

This is why the crawler is one script with a key list, not parallel tasks.
Splitting the same workload across tasks adds partitioning logic, N secrets,
and N log streams for zero throughput gain. Two workloads that genuinely must
run concurrently should get *disjoint* key subsets, never a shared pool.

### With few keys you're latency-bound, not limit-bound

Each call gets a budget of `1.2s / n_keys`, and Riot's round-trip is ~640ms.
Measured with 2 keys:

| Matches | Calls | Time | Rate | 429s |
|---|---|---|---|---|
| 199 | 401 | 4.1 min | 1.63/s (ceiling 1.67) | **0** |

429s only start appearing around 3+ keys, where the per-call budget (400ms)
drops below round-trip time. This is why the client is reactive - no proactive
pacing - and why that works: it rarely has to intervene.

### Transient 5xx are routine

A single 503 killed a 400-call crawl mid-run. Bounded retry with backoff is
required for long runs.

### Retention

Matches ~2 years, timelines ~1 year, rolling. Delta is the long-term archive:
anything not ingested before it ages out is gone for good.

## Storage

Bronze keeps Riot's JSON verbatim in a `payload` string column, so Riot schema
drift never breaks ingestion - dbt parses it downstream.

| | Per record | 199 matches logical | On disk |
|---|---|---|---|
| `matches` | ~73 KB | 14.6 MB | 1.4 MB |
| `timelines` | ~600 KB | 118.8 MB | 11.8 MB |

Delta/Parquet compresses this JSON **~10x**, and timelines are ~89% of the
data. Storage is not a constraint at any realistic scale; rate limits are.

## Memory

Accumulating a whole crawl before writing scales linearly and will OOM a
driver. Measured over 400 matches:

| Approach | Peak growth |
|---|---|
| Accumulate all | +258 MB |
| Stream batches | +1 MB |

`crawl_batches()` yields a batch at a time, so peak memory tracks batch size
rather than history length - and partial progress survives a crash.

## Environment

Databricks Free Edition, serverless only. Catalog `lol`, schema `bronze`.
`databricks-connect` (a dev dependency) drives Spark locally; its ~128MB
message cap is a local-only constraint that doesn't apply to a deployed job.
