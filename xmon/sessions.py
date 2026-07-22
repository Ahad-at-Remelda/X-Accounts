"""Single-session auth + rate governor.

One cookie, one rate-limit bucket. No rotation, no pool. If X rejects the
session (401/403/suspended), we surface it and stop — we do not swap in
another identity to keep access alive.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("xmon.session")


class SessionBlocked(Exception):
    """The single session was rejected by X (auth failure / lock / suspend)."""


@dataclass
class RateState:
    """Tracks the server-reported rate window plus our own soft cap."""

    limit: int = 0
    remaining: int = 0
    reset_epoch: float = 0.0


class Session:
    """Holds credentials and enforces spacing/soft-cap before each request."""

    def __init__(
        self,
        auth_token: str,
        ct0: str,
        bearer: str,
        *,
        max_requests_per_15min: int,
        min_gap_ms: int,
    ) -> None:
        self.auth_token = auth_token
        self.ct0 = ct0
        self.bearer = bearer
        self._max_per_window = max_requests_per_15min
        self._min_gap = min_gap_ms / 1000.0

        self._lock = asyncio.Lock()
        self._last_request_at = 0.0
        # Our own rolling 15-min counter (independent of server headers).
        self._window_start = time.monotonic()
        self._window_count = 0
        self.rate = RateState()

    # -- header plumbing ----------------------------------------------------
    def cookie_header(self) -> str:
        return f"auth_token={self.auth_token}; ct0={self.ct0}"

    def base_headers(self) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self.bearer}",
            "x-csrf-token": self.ct0,
            "cookie": self.cookie_header(),
            "content-type": "application/json",
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "referer": "https://x.com/",
            "origin": "https://x.com",
        }

    # -- pacing -------------------------------------------------------------
    async def acquire(self) -> None:
        """Block until it is polite to issue the next request."""
        async with self._lock:
            now = time.monotonic()

            # Reset our rolling soft-cap window.
            if now - self._window_start >= 900:
                self._window_start = now
                self._window_count = 0

            # Soft cap: if we've hit our own budget, wait out the window.
            if self._window_count >= self._max_per_window:
                wait = 900 - (now - self._window_start)
                if wait > 0:
                    log.warning("soft rate cap hit; sleeping %.0fs", wait)
                    await asyncio.sleep(wait)
                    self._window_start = time.monotonic()
                    self._window_count = 0

            # Server-reported window nearly exhausted: wait for reset.
            if self.rate.limit and self.rate.remaining <= 1:
                wait = self.rate.reset_epoch - time.time()
                if wait > 0:
                    log.warning(
                        "server rate window exhausted; sleeping %.0fs to reset", wait
                    )
                    await asyncio.sleep(wait + 1)

            # Minimum spacing between any two requests.
            gap = time.monotonic() - self._last_request_at
            if gap < self._min_gap:
                await asyncio.sleep(self._min_gap - gap)

            self._last_request_at = time.monotonic()
            self._window_count += 1

    def note_headers(self, headers: dict[str, str]) -> None:
        """Update rate state from x-rate-limit-* response headers."""
        try:
            if "x-rate-limit-limit" in headers:
                self.rate.limit = int(headers["x-rate-limit-limit"])
            if "x-rate-limit-remaining" in headers:
                self.rate.remaining = int(headers["x-rate-limit-remaining"])
            if "x-rate-limit-reset" in headers:
                self.rate.reset_epoch = float(headers["x-rate-limit-reset"])
        except (ValueError, TypeError):
            pass

    async def backoff_429(self) -> None:
        """Honor a 429 for this session only. No rotation."""
        wait = max(self.rate.reset_epoch - time.time(), 15)
        log.warning("429 received; backing off %.0fs (single session)", wait)
        await asyncio.sleep(wait + 1)
