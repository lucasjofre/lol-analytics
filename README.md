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
from lol_analytics.crawl import crawl_batches, list_match_ids
from lol_analytics.ingest import existing_match_ids, write_bronze

client = RiotClient(keys)
puuid = client.get_account("br1", "GameName", "TAG")["puuid"]

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

### Rate limits are per key, so extra processes buy nothing

The ceiling is `n_keys x limit` whether one process rotates keys or N
processes each own one. A personal/dev key allows 20 req/s and 100 req/120s,
i.e. one request per 1.2s per key.

This is why the crawler is one script with a key list, not parallel tasks.
Splitting the same workload across tasks adds partitioning logic, N secrets,
and N log streams for zero throughput gain. Two workloads that genuinely must
run concurrently should get *disjoint* key subsets, never a shared pool.

Concurrency *inside* that one script is a different question, and the answer
is the opposite - see below. Extra processes don't raise the ceiling; one
thread per key is what reaches it.

### One blocking thread can't reach the ceiling; one worker per key can

A single thread waits on one reply at a time, so while it blocks on one key
the other keys' quotas sit idle. Rotating keys doesn't fix that - with 4 keys
a round-robin returns to each key only every `4 x latency`, longer than the
1.2s that key would have allowed.

`crawl_batches()` therefore runs one worker per key, each pinned to its own
key by task index (which also keeps threads off the non-atomic `_next_key`
counter). Measured over 250 matches per arm - 500 calls each, deliberately
past the 100/120s per-key allowance so the limiter actually engages:

| | matches/s | calls/s | % of ceiling | Wall |
|---|---|---|---|---|
| Sequential | 0.76 | 1.52 | 46% | 5.5 min |
| One worker per key | 1.80 | 3.60 | 108% | 2.3 min |

**2.37x**, or ~2,700 -> ~6,500 matches/hour on 4 keys. The concurrent arm
lands at the quota ceiling, which is the whole point: it is now limit-bound
rather than latency-bound, so the only way further up is more keys.

More workers than keys buys nothing - a key's allowance is fixed no matter how
many threads ask for it. Extra workers per key would only pay off if
round-trip time exceeded the 1.2s per-key budget, and it doesn't: ~385ms
median, ~600ms p95 for match details. Timelines are ~8x the payload and
correspondingly slower on the wire, which is why the sequential arm above
sits below the details-only rate.

429s stay rare and the client's reactive backoff absorbs them - 4 in 500 calls
at one worker per key. Riot serves 8 concurrent requests with no latency
penalty at all, so concurrency is never the constraint; quota is.

### Transient 5xx are routine

A single 503 killed a 400-call crawl mid-run. Bounded retry with backoff is
required for long runs.

Dropped connections are the same hazard one layer down, and `get()` does *not*
handle them - it retries status codes, not transport errors, so a
`requests.ConnectionError` still ends a run. Fresh connections per call never
hit this in 1,600 measured calls; a pooled `Session` hit it 4 times in 500,
Riot closing keep-alives mid-crawl. That is also why there is no `Session`
here: reuse measured *slower* (3.38 vs 3.79 calls/s) once reconnect cost is
counted, despite lower per-call latency.

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
