# CLAUDE.md

## Change discipline

- Make the SMALLEST change that satisfies the request. Do not refactor,
  rename, reformat, or "improve" code you were not asked to touch.
- New files and folders are fine when they genuinely make things simpler -
  don't overdo it, and say so before you create them. Default to editing an
  existing file; reach for a new one when a file is doing two jobs at once.
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
- If a change seems to need more than ~20 lines, STOP and propose the plan
  first instead of writing it.
- Comments only where the "why" is non-obvious. No narration of what the
  code plainly does.

## Gotchas that will bite you

These cause real bugs if forgotten. Full measured detail, plus storage,
memory, and throughput numbers, is in README.md - read it before changing
crawl/ingest behavior or the key-handling design.

- **puuids are encrypted per API key.** The same account returns a different
  puuid per key, and it only decrypts with the key that issued it. puuid-based
  calls pin one key; matchId-based calls can use any. Don't "simplify" that.
- **Rate limits are per key, so extra processes buy nothing.** The ceiling is
  `n_keys x limit` regardless of process count. One script with a key list -
  not parallel tasks. Concurrent workloads need disjoint key subsets. Threads
  *inside* that one script are the exception and are required: one worker per
  key, because a single blocking thread reaches only ~46% of the ceiling.
- **These are personal keys, not development keys.** They don't expire every
  24h, so a run can span the whole day and no daily regeneration step exists.
  Don't add one, and don't assume a crawl has to finish inside a 24h key life.
