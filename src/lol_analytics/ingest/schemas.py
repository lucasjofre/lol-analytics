"""Bronze table schemas, kept thin: keys plus the raw payload as a JSON string.

Declared as (name, spark type name, nullable) tuples so this module imports
without pyspark; `bronze.py` turns them into StructTypes.
"""

TARGETS = "targets"
LEAGUE_ENTRIES = "league_entries"
MATCHES = "matches"
TIMELINES = "timelines"
CRAWL_RUNS = "crawl_runs"

SCHEMAS: dict[str, list[tuple[str, str, bool]]] = {
    TARGETS: [
        ("puuid", "string", False),
        ("platform", "string", False),
        ("source", "string", False),
        ("name", "string", False),
        ("max_games", "int", False),
        ("resolved_at", "timestamp", False),
    ],
    LEAGUE_ENTRIES: [
        ("snapshot_date", "date", False),
        ("platform", "string", False),
        ("queue", "string", False),
        ("tier", "string", False),
        ("rank", "string", False),
        ("puuid", "string", False),
        ("league_points", "int", False),
        ("wins", "int", False),
        ("losses", "int", False),
        ("payload", "string", False),
    ],
    MATCHES: [
        ("match_id", "string", False),
        ("platform", "string", False),
        ("region", "string", False),
        ("fetched_at", "timestamp", False),
        ("payload", "string", False),
    ],
    TIMELINES: [
        ("match_id", "string", False),
        ("platform", "string", False),
        ("region", "string", False),
        ("fetched_at", "timestamp", False),
        ("status", "string", False),
        ("payload", "string", True),
    ],
    CRAWL_RUNS: [
        ("run_id", "string", False),
        ("task", "string", False),
        ("config_name", "string", False),
        ("started_at", "timestamp", False),
        ("finished_at", "timestamp", False),
        ("targets", "int", False),
        ("ids_discovered", "int", False),
        ("matches_fetched", "int", False),
        ("timelines_fetched", "int", False),
        ("errors", "int", False),
        ("api_calls", "int", False),
        ("stop_reason", "string", False),
    ],
}

PARTITION_BY: dict[str, list[str]] = {
    LEAGUE_ENTRIES: ["snapshot_date"],
    MATCHES: ["platform"],
    TIMELINES: ["platform"],
}
