"""Bronze writers. The only module that touches Spark."""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable
from datetime import date, datetime, timezone
from typing import Any

from lol_analytics.crawl.targets import Target
from lol_analytics.ingest import schemas

log = logging.getLogger(__name__)


def _struct(table: str):
    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "int": T.IntegerType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
    }
    return T.StructType([T.StructField(n, mapping[t], nullable) for n, t, nullable in schemas.SCHEMAS[table]])


class BronzeStore:
    def __init__(self, spark: Any, catalog: str, schema: str = "bronze") -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def ensure_tables(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        for name in schemas.SCHEMAS:
            if not self.spark.catalog.tableExists(self.table(name)):
                writer = self.spark.createDataFrame([], _struct(name)).write.format("delta")
                if name in schemas.PARTITION_BY:
                    writer = writer.partitionBy(*schemas.PARTITION_BY[name])
                writer.saveAsTable(self.table(name))
                log.info("created %s", self.table(name))

    def _append(self, table: str, rows: Iterable[Any], mode: str = "append") -> int:
        data = [_as_tuple(r, table) for r in rows]
        if not data:
            return 0
        df = self.spark.createDataFrame(data, _struct(table))
        df.write.format("delta").mode(mode).saveAsTable(self.table(table))
        return len(data)

    # stage 1 outputs
    def replace_targets(self, targets: Iterable[Target]) -> int:
        now = datetime.now(timezone.utc)
        rows = [(t.puuid, t.platform, t.source, t.name, t.max_games, now) for t in targets]
        return self._append(schemas.TARGETS, rows, mode="overwrite")

    def append_league_entries(self, rows: Iterable[Any]) -> int:
        return self._append(schemas.LEAGUE_ENTRIES, rows)

    # stage 2 inputs and outputs
    def load_targets(self) -> list[Target]:
        rows = self.spark.table(self.table(schemas.TARGETS)).collect()
        return [Target(r.puuid, r.platform, r.source, r.name, r.max_games) for r in rows]

    def seen_match_ids(self) -> set[str]:
        """Ids that already have both a match row and a timeline row (ok or not_found)."""
        m = self.spark.table(self.table(schemas.MATCHES)).select("match_id")
        t = self.spark.table(self.table(schemas.TIMELINES)).select("match_id")
        return {r.match_id for r in m.intersect(t).collect()}

    def seen_match_ids_without_timelines(self) -> set[str]:
        m = self.spark.table(self.table(schemas.MATCHES)).select("match_id")
        t = self.spark.table(self.table(schemas.TIMELINES)).select("match_id")
        return {r.match_id for r in m.subtract(t).collect()}

    def append_matches(self, rows: Iterable[Any]) -> int:
        return self._append(schemas.MATCHES, rows)

    def append_timelines(self, rows: Iterable[Any]) -> int:
        return self._append(schemas.TIMELINES, rows)

    def append_run(self, row: dict[str, Any]) -> None:
        self._append(schemas.CRAWL_RUNS, [row])


def _as_tuple(row: Any, table: str) -> tuple:
    if isinstance(row, tuple):
        return row
    if dataclasses.is_dataclass(row):
        row = dataclasses.asdict(row)
    if isinstance(row, dict):
        out = []
        for name, typ, _ in schemas.SCHEMAS[table]:
            v = row[name]
            if typ == "date" and isinstance(v, str):
                v = date.fromisoformat(v)
            out.append(v)
        return tuple(out)
    raise TypeError(f"cannot convert {type(row)} to a {table} row")
