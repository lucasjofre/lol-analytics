"""Thin HTTP client for the Riot API with rate limiting and retries."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from lol_analytics.riot.limiter import RateLimiter

log = logging.getLogger(__name__)

RETRYABLE = {429, 500, 502, 503, 504}


class RiotApiError(Exception):
    def __init__(self, status: int, url: str, body: str = "") -> None:
        super().__init__(f"{status} from {url}: {body[:200]}")
        self.status = status
        self.url = url


class NotFound(RiotApiError):
    pass


class RiotClient:
    def __init__(
        self,
        api_key: str,
        limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
        max_retries: int = 5,
        timeout: float = 30.0,
        sleep=time.sleep,
    ) -> None:
        self._key = api_key
        self.limiter = limiter or RateLimiter()
        self._session = session or requests.Session()
        self._max_retries = max_retries
        self._timeout = timeout
        self._sleep = sleep

    def get(self, host: str, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"https://{host}{path}"
        headers = {"X-Riot-Token": self._key}
        attempt = 0
        while True:
            self.limiter.acquire(host)
            resp = self._session.get(url, params=params, headers=headers, timeout=self._timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                raise NotFound(404, url, resp.text)
            if resp.status_code in RETRYABLE and attempt < self._max_retries:
                attempt += 1
                backoff = float(resp.headers.get("Retry-After", 0) or 0) or min(2**attempt, 30)
                if resp.status_code == 429:
                    self.limiter.penalize(host, backoff)
                log.warning("%s on %s, retry %d in %.1fs", resp.status_code, url, attempt, backoff)
                self._sleep(backoff)
                continue
            raise RiotApiError(resp.status_code, url, resp.text)
