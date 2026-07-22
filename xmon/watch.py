"""Latency probe: poll each account every N seconds and report, for every new
tweet, how long after it was posted we first saw it.

lag = detected_at (now, UTC) - tweet.created_at (X's post timestamp)

created_at has 1-second resolution and is X's server clock, so treat the number
as accurate to ~1-2s, not milliseconds. Good enough to answer "do we catch a
post within one 5s poll cycle?".

Run:
    python -m xmon.watch --interval 5            # watch all accounts.txt handles
    python -m xmon.watch --interval 5 --handle yourhandle
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from .api import UserUnavailable, XApi
from .config import load_config
from .parse import parse_timeline
from .queryids import QueryIdResolver
from .sessions import Session, SessionBlocked
from .store import Store, UserRecord

log = logging.getLogger("xmon.watch")


def _created_epoch(created_at: str) -> float | None:
    if not created_at:
        return None
    try:
        return parsedate_to_datetime(created_at).timestamp()
    except (TypeError, ValueError):
        try:
            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            return dt.timestamp()
        except ValueError:
            return None


class Watcher:
    def __init__(self, cfg, api: XApi, store: Store, handles: list[str], interval: float):
        self._cfg = cfg
        self._api = api
        self._store = store
        self._handles = handles
        self._interval = interval
        self._ids: dict[str, str] = {}   # handle -> rest_id
        self._last: dict[str, int] = {}   # handle -> newest tweet id seen
        self._polls = 0

    async def _resolve(self, handle: str) -> str | None:
        rec = self._store.get_user(handle)
        if rec:
            self._ids[handle] = rec.rest_id
            return rec.rest_id
        try:
            info = await self._api.resolve_user(handle)
        except UserUnavailable as exc:
            log.warning("skip @%s: %s", handle, exc)
            return None
        rec = UserRecord(handle, info["rest_id"], info["name"])
        self._store.put_user(rec)
        self._ids[handle] = rec.rest_id
        return rec.rest_id

    async def _poll_handle(self, handle: str) -> None:
        rest_id = self._ids.get(handle) or await self._resolve(handle)
        if not rest_id:
            return
        data = await self._api.user_tweets(rest_id, 5)
        tweets = parse_timeline(data, handle, include_replies=True, include_retweets=True)
        if not tweets:
            return
        newest = tweets[-1]  # chronological; newest last
        newest_id = int(newest.id)

        # First sighting of this handle: set a baseline, don't count as "new".
        if handle not in self._last:
            self._last[handle] = newest_id
            age = self._age(newest)
            print(
                f"  baseline @{handle}: latest is {newest.id} "
                f"(posted {age}) — waiting for the NEXT post…",
                flush=True,
            )
            return

        if newest_id <= self._last[handle]:
            return  # nothing new

        # One or more new tweets since last poll. Report each, oldest first.
        detected = time.time()
        new_ones = [t for t in tweets if int(t.id) > self._last[handle]]
        for t in new_ones:
            posted = _created_epoch(t.created_at)
            if posted is not None:
                lag = detected - posted
                lag_str = f"{lag:.1f}s after posting"
            else:
                lag_str = "lag unknown (no timestamp)"
            kind = "RT" if t.is_retweet else ("reply" if t.is_reply else "tweet")
            text = " ".join(t.text.split())
            if len(text) > 100:
                text = text[:97] + "..."
            print(
                f"\n  >>> NEW {kind} @{handle}  DETECTED {lag_str}\n"
                f"      posted:   {t.created_at}\n"
                f"      detected: {datetime.now(timezone.utc).strftime('%a %b %d %H:%M:%S +0000 %Y')}\n"
                f"      id {t.id}: {text}\n"
                f"      {t.url}",
                flush=True,
            )
        self._last[handle] = newest_id

    def _age(self, tweet) -> str:
        posted = _created_epoch(tweet.created_at)
        if posted is None:
            return "unknown age"
        secs = max(0, time.time() - posted)
        if secs < 90:
            return f"{secs:.0f}s ago"
        if secs < 5400:
            return f"{secs/60:.0f}m ago"
        return f"{secs/3600:.1f}h ago"

    async def run(self) -> None:
        print(
            f"Watching {len(self._handles)} account(s) every {self._interval:g}s. "
            f"Post a tweet and watch for the lag. Ctrl-C to stop.\n",
            flush=True,
        )
        while True:
            start = time.monotonic()
            self._polls += 1
            for handle in self._handles:
                try:
                    await self._poll_handle(handle)
                except SessionBlocked as exc:
                    log.error("session blocked by X, stopping: %s", exc)
                    return
                except Exception as exc:
                    log.warning("poll @%s failed: %s", handle, exc)

            # Heartbeat so you can see it's alive between posts.
            elapsed = time.monotonic() - start
            hb = "  ".join(
                f"@{h}:{self._last.get(h, '-')}" for h in self._handles
            )
            print(
                f"[poll #{self._polls}  {datetime.now().strftime('%H:%M:%S')}  "
                f"{elapsed*1000:.0f}ms]  latest {hb}",
                flush=True,
            )

            await asyncio.sleep(max(0.0, self._interval - elapsed))


async def _amain(args: argparse.Namespace) -> int:
    cfg = load_config(args.config_dir)
    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if not cfg.auth.valid():
        log.error("auth not configured in config.yaml")
        return 2

    handles = [args.handle.lstrip("@").lower()] if args.handle else cfg.accounts
    if not handles:
        log.error("no handles: pass --handle or fill accounts.txt")
        return 2

    client = httpx.AsyncClient(http2=True, follow_redirects=True, timeout=30)
    session = Session(
        cfg.auth.auth_token, cfg.auth.ct0, cfg.auth.bearer,
        max_requests_per_15min=cfg.rate.max_requests_per_15min,
        min_gap_ms=cfg.rate.min_gap_ms,
    )
    resolver = QueryIdResolver(
        client, os.path.join(args.config_dir, ".queryids.json"),
        ttl_hours=cfg.queryids.cache_ttl_hours, fallback=cfg.queryids.fallback,
        headers_provider=session.base_headers,
    )
    await resolver.ensure()
    store = Store(cfg.db_path)
    api = XApi(client, session, resolver, cfg.features)

    watcher = Watcher(cfg, api, store, handles, args.interval)
    try:
        await watcher.run()
    finally:
        await client.aclose()
        store.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="xmon.watch", description="Tweet-latency probe")
    parser.add_argument("--config-dir", default=".")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between polls")
    parser.add_argument("--handle", default=None, help="watch one handle (default: accounts.txt)")
    args = parser.parse_args()
    try:
        raise SystemExit(asyncio.run(_amain(args)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
