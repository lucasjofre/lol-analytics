"""Shared wiring for job entrypoints: Spark session, secrets, API client, logging."""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from lol_analytics.riot import RateLimiter, RiotApi, RiotClient


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--config", required=True, help="path to crawl.<target>.yml")
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog for bronze tables")
    p.add_argument("--secret-scope", default="lol")
    p.add_argument("--secret-key", default="riot_api_key")
    return p


def get_spark() -> Any:
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def get_riot_api_key(spark: Any, scope: str, key: str) -> str:
    """Env var wins for local runs; otherwise read the Databricks secret."""
    env = os.environ.get("RIOT_API_KEY")
    if env:
        return env
    try:
        from databricks.sdk.runtime import dbutils  # serverless / notebook runtime
    except ImportError:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    return dbutils.secrets.get(scope=scope, key=key)


def build_api(api_key: str) -> RiotApi:
    return RiotApi(RiotClient(api_key, limiter=RateLimiter()))
