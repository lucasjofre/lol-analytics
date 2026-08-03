# lol-analytics

Riot API ingestion → dbt data models on Databricks → an MCP server that lets an LLM
answer questions against the modelled data.

## Layout

```
databricks.yml            Bundle root. Its presence here is what makes this a DAB project.
resources/                Bundle resources, one file per concern. Globbed by `include:`.
  schemas.yml               UC schemas (raw/staging/marts) + the landing volume.
  ingest_riot.job.yml       Riot ingestion job. Declared but PAUSED — see below.
  dbt_transform.job.yml     `dbt build` against the SQL warehouse.

src/
  lol_analytics/          The Python package (this is what ships in the wheel).
    config/                 Settings, region/queue enums, env loading.
    riot/                   API client — transport concerns only.
      endpoints/              One module per Riot API surface.
      rate_limit.py           Per-region + per-method token buckets.
      retry.py                Backoff, 429 + Retry-After, 5xx.
    ingestion/              Crawl logic — what to fetch and in what order.
      crawlers/               Ladder discovery, match backfill.
      state.py                Watermarks / seen-match cursors.
      sinks.py                The landing-zone writer (contract boundary, see below).
    mcp/                    MCP server exposing the semantic layer to an LLM.
  dbt/                    The dbt project. Sibling of the package, not inside it.
    models/staging/         stg_*: rename + cast, 1:1 with raw.
    models/intermediate/    int_*: unnest participants, join patch context.
    models/marts/core/      dim_champion, dim_player, fct_match, fct_participant.
    seeds/                  Data Dragon static data (champions, items, runes).
    snapshots/              SCD2: player rank over time, champion stats per patch.

tests/                    pytest. Weighted toward rate_limit and retry.
docs/data-model/          Grain contracts, ERDs, modelling decisions.
scratch/                  Exploration. Gitignored.
```

`src/dbt/` sits beside `src/lol_analytics/` rather than inside it: the `uv_build`
backend treats `src/lol_analytics/` as the wheel contents, so a nested dbt project
would be packaged into the wheel and pollute the import namespace. As a sibling it
still gets synced to the workspace by the bundle, and the dbt task points at it via
`project_directory: src/dbt`.

## Where ingestion runs

Ingestion is a long-running, rate-limited crawl — cheap on a laptop, expensive on
cloud compute. So it runs locally, and the Databricks job in
`resources/ingest_riot.job.yml` stays `PAUSED` (via the `ingestion_paused` variable)
so the schedule, parameters and entry point remain reviewable in git.

This works because the **landing zone is a contract**. `ingestion/sinks.py` is the
only module that knows where raw payloads go; everything downstream only knows
"raw lands in `/Volumes/<catalog>/<prefix>_raw/landing`". Whoever writes there —
your machine or a job — is invisible to dbt.

## Getting started

```bash
uv sync
cp .env.example .env        # fill in RIOT_API_KEY and Databricks host/token

databricks bundle validate                 # needs a real workspace login
databricks bundle deploy --target dev

cd src/dbt && dbt deps && dbt build
```

## Note on the semantic layer

The MCP tools should expose defined metrics (`list_metrics`, `query_metric`,
`describe_model`) rather than arbitrary SQL — that is what makes LLM answers
reliable rather than plausible. Decide early whether that layer is dbt MetricFlow or
Databricks metric views, because it shapes how the marts are grained.
