# CLAUDE.md

## Change discipline

- Make the SMALLEST change that satisfies the request. Do not refactor,
  rename, reformat, or "improve" code you were not asked to touch.
- Do NOT create new files unless strictly required. Prefer editing an
  existing file over adding a new one.
- Do NOT add abstractions (interfaces, base classes, factories, wrappers,
  generic helpers) for a single use case. Inline first; abstract only when
  there are 3+ real call sites.
- Do NOT add error handling, fallbacks, retries, or defensive guards unless
  the input is genuinely untrusted or I explicitly ask. No try/except around
  code that cannot throw.
- Do NOT add caching, memoization, or batching unless I name a performance
  requirement.
- Match the surrounding code's style, naming, and patterns. Do not introduce
  a new pattern.
- If a change seems to need more than ~20 lines or a new file, STOP and
  propose the plan first instead of writing it.
- Comments only where the "why" is non-obvious. No narration of what the
  code plainly does.

## Riot API constraints (measured, not guessed)

**puuids are encrypted per API key.** The same account returns a *different*
puuid depending which key asked, and a puuid only decrypts with the key that
issued it - cross-key reuse returns `400 Bad Request - Exception decrypting`.
This is a privacy measure (it stops two apps correlating the same player), and
it shapes the whole multi-key design:

- Calls taking a **puuid** (account lookup, match id list, league entries) must
  pin one key. `RiotClient` pins these to `keys[0]`.
- Calls taking a **matchId** (match details, timeline) are safe on any key -
  matchIds are universal. These round-robin, and they're ~99% of call volume.
- Any future cohort/fan-out discovery inherits this: whichever key discovered
  a puuid is the only key that can use it.

**Rate limits are per key, so parallelism buys nothing.** The ceiling is
`keys x limit` whether one process rotates keys or N processes each own one.
Personal/dev key: 20 req/s and 100 req/120s -> 1 request per 1.2s per key.
Hence one job, one task, one script with a key list - not parallel tasks.
Two workloads that must run concurrently should get *disjoint* key subsets,
never a shared pool.

**With few keys you're latency-bound, not limit-bound.** Each call gets a
budget of `1.2s / n_keys`; Riot's round-trip is ~640ms. Measured with 2 keys:
199 matches, 401 calls, 1.63 calls/sec against a 1.67 ceiling, **zero 429s**.
429s only start appearing around 3+ keys, where the per-call budget (400ms)
drops below round-trip time. This is why the client is reactive (no proactive
pacing) - it works because it rarely has to intervene.

**Transient 5xx are routine.** A single 503 killed a 400-call crawl. Bounded
retry with backoff is required for any long run, not defensive coding.

**Retention:** matches ~2 years, timelines ~1 year, rolling. Delta is therefore
the long-term archive - anything not ingested before it ages out is gone.

## Storage facts (measured)

- Timelines are ~600KB, match details ~73KB. Timelines are ~89% of the data.
- Delta/Parquet compresses this JSON **~10x** (199 matches: 133MB logical ->
  13MB on disk). Storage is not a constraint at any realistic scale; API rate
  limits are.
- Bronze keeps Riot's JSON verbatim in a `payload` string column so schema
  drift never breaks ingestion. dbt parses it downstream.

## Memory

Accumulating a whole crawl before writing scales linearly and OOMs a driver.
Measured on 400 matches: **+258MB accumulating vs +1MB** streaming in batches.
Jobs drive `crawl_batches()` + `write_bronze()` together so peak memory tracks
batch size, not history length - and partial progress survives a crash.
`crawl_player()` returns everything in one list and is for interactive use only.

## Environment

- Workspace is Databricks Free Edition: serverless only, catalog `lol` with
  schema `bronze`. `databricks-connect` (dev dependency) drives it locally;
  its ~128MB message cap is a local-only constraint, not a job constraint.
- Keys live in `.env` as `API_KEYS=key1,key2` (gitignored).
