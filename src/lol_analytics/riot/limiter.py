"""Sliding-window rate limiter keyed by host.

Riot enforces limits per API key per host. A development or personal key gets
20 requests / 1 s and 100 requests / 120 s. Each window is tracked with a deque
of request timestamps; `acquire` blocks until every window has room.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Window:
    limit: int
    seconds: float


DEV_KEY_WINDOWS: tuple[Window, ...] = (Window(20, 1.0), Window(100, 120.0))


@dataclass
class _HostState:
    windows: tuple[Window, ...]
    history: list[deque[float]] = field(default_factory=list)
    blocked_until: float = 0.0

    def __post_init__(self) -> None:
        self.history = [deque() for _ in self.windows]


class RateLimiter:
    def __init__(
        self,
        windows: tuple[Window, ...] = DEV_KEY_WINDOWS,
        clock=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        self._windows = windows
        self._clock = clock
        self._sleep = sleep
        self._hosts: dict[str, _HostState] = {}
        self._lock = threading.Lock()
        self.calls = 0

    def _state(self, host: str) -> _HostState:
        if host not in self._hosts:
            self._hosts[host] = _HostState(self._windows)
        return self._hosts[host]

    @staticmethod
    def _wait_needed(state: _HostState, now: float) -> float:
        wait = max(0.0, state.blocked_until - now)
        for window, hist in zip(state.windows, state.history):
            cutoff = now - window.seconds
            while hist and hist[0] <= cutoff:
                hist.popleft()
            if len(hist) >= window.limit:
                wait = max(wait, hist[0] + window.seconds - now)
        return wait

    def acquire(self, host: str) -> None:
        """Block until a request to `host` is allowed, then record it."""
        while True:
            with self._lock:
                state = self._state(host)
                now = self._clock()
                wait = self._wait_needed(state, now)
                if wait <= 0:
                    for hist in state.history:
                        hist.append(now)
                    self.calls += 1
                    return
            self._sleep(wait)

    def penalize(self, host: str, seconds: float) -> None:
        """Honor a Retry-After from a 429 by blocking the host."""
        with self._lock:
            state = self._state(host)
            state.blocked_until = max(state.blocked_until, self._clock() + seconds)
