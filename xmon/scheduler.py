"""Polling loop.

For <200 accounts at 1-2 min lag on one session, a simple per-account timer
is enough: each account has a next-due time; we sleep to the soonest, poll it,
reschedule. The single rate governor inside Session keeps total throughput
under the session's budget regardless of how many accounts come due at once.
"""
from __future__ import annotations

import asyncio
import heapq
import logging
import random
import time
from dataclasses import dataclass, field

from .api import UserUnavailable, XApi
from .config import Config
from .parse import parse_timeline
from .sessions import SessionBlocked
from .sinks.base import Sink
from .store import Store, UserRecord

log = logging.getLogger("xmon.scheduler")


@dataclass(order=True)
class _Due:
    due_at: float
    handle: str = field(compare=False)


class Monitor:
    def __init__(
        self,
        cfg: Config,
        api: XApi,
        store: Store,
        sinks: list[Sink],
        *,
        dry_run: bool = False,
    ) -> None:
        self._cfg = cfg
        self._api = api
        self._store = store
        self._sinks = sinks
        self._dry_run = dry_run
        self._queue: list[_Due] = []
        self._stop = asyncio.Event()
        self._polls = 0
        self._emitted = 0

    def request_stop(self) -> None:
        self._stop.set()

    def _next_due(self, base: float | None = None) -> float:
        base = base if base is not None else time.monotonic()
        j = self._cfg.poll.jitter_seconds
        return base + self._cfg.poll.interval_seconds + random.uniform(-j, j)

    async def _ensure_user(self, handle: str) -> UserRecord | None:
        rec = self._store.get_user(handle)
        if rec:
            return rec
        try:
            info = await self._api.resolve_user(handle)
        except UserUnavailable as exc:
            log.warning("skip @%s: %s", handle, exc)
            return None
        rec = UserRecord(handle=handle, rest_id=info["rest_id"], name=info["name"])
        self._store.put_user(rec)
        log.info("resolved @%s -> %s (%s)", handle, rec.rest_id, rec.name)
        return rec

    async def _poll_once(self, handle: str) -> None:
        rec = await self._ensure_user(handle)
        if not rec:
            return

        data = await self._api.user_tweets(rec.rest_id, self._cfg.poll.tweets_per_request)
        tweets = parse_timeline(
            data,
            handle,
            include_replies=self._cfg.poll.include_replies,
            include_retweets=self._cfg.poll.include_retweets,
        )
        self._polls += 1

        first_time = not self._store.is_initialized(handle)
        last_seen = self._store.get_last_seen(handle)
        max_id = int(last_seen) if last_seen else 0

        new_tweets = []
        for tw in tweets:  # already oldest-first
            if self._store.is_seen(tw.id):
                continue
            if last_seen and int(tw.id) <= int(last_seen):
                continue
            new_tweets.append(tw)

        if first_time and not self._cfg.poll.backfill_on_first_seen:
            # Establish a baseline without flooding on the first poll.
            for tw in tweets:
                self._store.mark_seen(tw.id, handle)
                max_id = max(max_id, int(tw.id))
            self._store.set_state(handle, str(max_id) if max_id else None, initialized=True)
            log.info("baselined @%s (%d existing tweets ignored)", handle, len(tweets))
            return

        for tw in new_tweets:
            await self._deliver(tw)
            self._store.mark_seen(tw.id, handle)
            max_id = max(max_id, int(tw.id))

        if new_tweets:
            log.info("@%s: %d new", handle, len(new_tweets))
        self._store.set_state(
            handle, str(max_id) if max_id else last_seen, initialized=True
        )

    async def _deliver(self, tweet) -> None:
        self._emitted += 1
        if self._dry_run:
            log.info("[dry-run] would emit %s @%s", tweet.id, tweet.handle)
            return
        for sink in self._sinks:
            try:
                await sink.emit(tweet)
            except Exception as exc:  # a bad sink must not kill the loop
                log.warning("sink %s failed: %s", type(sink).__name__, exc)

    async def run(self) -> None:
        if not self._cfg.accounts:
            log.error("no accounts configured (accounts.txt is empty)")
            return

        now = time.monotonic()
        # Stagger initial polls so they don't all fire at once.
        spread = max(self._cfg.poll.interval_seconds, 1)
        for i, handle in enumerate(self._cfg.accounts):
            offset = (i / max(len(self._cfg.accounts), 1)) * spread
            heapq.heappush(self._queue, _Due(now + offset, handle))

        log.info("monitoring %d accounts", len(self._cfg.accounts))
        prune_at = time.monotonic() + 3600

        while not self._stop.is_set():
            if not self._queue:
                break
            nxt = self._queue[0]
            wait = nxt.due_at - time.monotonic()
            if wait > 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                    break  # stop was set
                except asyncio.TimeoutError:
                    pass

            due = heapq.heappop(self._queue)
            try:
                await self._poll_once(due.handle)
            except SessionBlocked as exc:
                # The one identity is dead. We do not rotate. Stop cleanly.
                log.error("session blocked by X, stopping: %s", exc)
                self.request_stop()
                break
            except Exception as exc:
                log.warning("poll @%s failed: %s", due.handle, exc)

            heapq.heappush(self._queue, _Due(self._next_due(), due.handle))

            if time.monotonic() >= prune_at:
                self._store.prune_seen()
                prune_at = time.monotonic() + 3600

        log.info("stopped. polls=%d emitted=%d", self._polls, self._emitted)
