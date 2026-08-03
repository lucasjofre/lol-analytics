"""Command line entry point.

Everything ingestion-related is driven from here so the same code path runs on a
laptop and inside the Databricks job.
"""

from __future__ import annotations

import typer

app = typer.Typer(help="Riot API ingestion and local operations for lol-analytics.")

ingest = typer.Typer(help="Ingestion commands.")
app.add_typer(ingest, name="ingest")


@ingest.command("ladder")
def ingest_ladder(
    region: str = typer.Option(..., help="Riot platform routing value, e.g. euw1."),
    landing_path: str = typer.Option(..., help="Destination for raw payloads."),
) -> None:
    """Crawl the ranked ladder and record the players to fetch matches for."""
    raise NotImplementedError


@ingest.command("matches")
def ingest_matches(
    region: str = typer.Option(..., help="Riot platform routing value, e.g. euw1."),
    landing_path: str = typer.Option(..., help="Destination for raw payloads."),
) -> None:
    """Backfill match detail for known players."""
    raise NotImplementedError


if __name__ == "__main__":
    app()
