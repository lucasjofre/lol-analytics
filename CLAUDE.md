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
