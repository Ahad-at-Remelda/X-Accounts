"""Lists mode: batch all accounts into one X List and poll it as a unit.

One `ListLatestTweetsTimeline` request returns the recent tweets of every List
member, so N accounts cost ~1 request per poll instead of N. This is the free
way to scale to hundreds/thousands of accounts on a single session.

Pipeline:
  ensure_list()   -> create (or reuse) the List, remember its id in the DB
  sync_members()  -> resolve accounts.txt handles, add any missing to the List
  poll loop       -> list_tweets() -> parse -> dedup -> emit
"""
from __future__ import annotations

import asyncio
import logging
import time

from .api import ApiError, UserUnavailable, XApi
from .config import Config
from .parse import parse_list_members, parse_list_timeline
from .sessions import SessionBlocked
from .sinks.base import Sink
from .store import Store, UserRecord

log = logging.getLogger("xmon.lists")

_LIST_ID_KEY = "lists.active_id"


class ListManager:
    def __init__(self, cfg: Config, api: XApi, store: Store) -> None:
        self._cfg = cfg
        self._api = api
        self._store = store
        self.list_id: str | None = None

    async def ensure_list(self) -> str:
        """Return the List id to use: config > remembered-in-DB > freshly created."""
        # 1) Explicit id in config wins.
        if self._cfg.lists.list_id:
            self.list_id = self._cfg.lists.list_id
            log.info("using configured list_id %s", self.list_id)
            return self.list_id

        # 2) A previously auto-created id, remembered in the DB.
        remembered = self._store.get_meta(_LIST_ID_KEY)
        if remembered:
            # Confirm it still exists / we still own it.
            try:
                meta = await self._api.list_meta(remembered)
                if meta.get("id_str") or meta.get("rest_id"):
                    self.list_id = remembered
                    log.info(
                        "reusing list '%s' (%s, %s members)",
                        meta.get("name"), remembered, meta.get("member_count"),
                    )
                    return self.list_id
            except ApiError as exc:
                log.warning("remembered list %s unusable (%s); creating new", remembered, exc)

        # 3) Create a fresh List and remember it.
        self.list_id = await self._api.create_list(
            self._cfg.lists.name,
            self._cfg.lists.description,
            self._cfg.lists.private,
        )
        self._store.set_meta(_LIST_ID_KEY, self.list_id)
        log.info("created list '%s' -> %s", self._cfg.lists.name, self.list_id)
        return self.list_id

    async def _current_members(self) -> dict[str, str]:
        """screen_name(lower) -> rest_id for everyone currently in the List."""
        members: dict[str, str] = {}
        cursor: str | None = None
        for _ in range(200):  # hard page cap (200*100 = 20k, well past 5k limit)
            data = await self._api.list_members_page(self.list_id, 100, cursor)
            page, cursor = parse_list_members(data)
            for m in page:
                members[m["screen_name"].lower()] = m["rest_id"]
                # Opportunistically cache the id so polling never re-resolves.
                if not self._store.get_user(m["screen_name"].lower()):
                    self._store.put_user(
                        UserRecord(m["screen_name"].lower(), m["rest_id"], m["name"])
                    )
            if not cursor or not page:
                break
        return members

    async def _resolve_id(self, handle: str) -> str | None:
        rec = self._store.get_user(handle)
        if rec:
            return rec.rest_id
        try:
            info = await self._api.resolve_user(handle)
        except UserUnavailable as exc:
            log.warning("skip @%s: %s", handle, exc)
            return None
        self._store.put_user(UserRecord(handle, info["rest_id"], info["name"]))
        return info["rest_id"]

    async def sync_members(self) -> None:
        """Add every accounts.txt handle to the List; optionally prune extras."""
        want = self._cfg.accounts
        if not want:
            log.warning("accounts.txt empty; List will have no members")
            return

        current = await self._current_members()
        current_ids = set(current.values())
        log.info("List has %d members; want %d handles", len(current), len(want))

        added = 0
        for handle in want:
            if handle in current:
                continue
            uid = await self._resolve_id(handle)
            if not uid:
                continue
            if uid in current_ids:
                continue  # already a member under a different-cased handle
            try:
                await self._api.add_list_member(self.list_id, uid)
                current_ids.add(uid)
                added += 1
                log.info("added @%s to list", handle)
            except (ApiError, UserUnavailable) as exc:
                log.warning("could not add @%s: %s", handle, exc)
        if added:
            log.info("added %d new member(s)", added)

        if self._cfg.lists.prune_extra:
            want_ids = set()
            for h in want:
                rec = self._store.get_user(h)
                if rec:
                    want_ids.add(rec.rest_id)
            for sn, uid in current.items():
                if uid not in want_ids:
                    try:
                        await self._api.remove_list_member(self.list_id, uid)
                        log.info("pruned @%s from list", sn)
                    except ApiError as exc:
                        log.warning("could not prune @%s: %s", sn, exc)


class ListMonitor:
    def __init__(self, cfg: Config, api: XApi, store: Store, sinks: list[Sink],
                 *, dry_run: bool = False) -> None:
        self._cfg = cfg
        self._api = api
        self._store = store
        self._sinks = sinks
        self._dry_run = dry_run
        self._stop = asyncio.Event()
        self._polls = 0
        self._emitted = 0

    def request_stop(self) -> None:
        self._stop.set()

    async def _deliver(self, tweet) -> None:
        self._emitted += 1
        if self._dry_run:
            log.info("[dry-run] would emit %s @%s", tweet.id, tweet.handle)
            return
        for sink in self._sinks:
            try:
                await sink.emit(tweet)
            except Exception as exc:
                log.warning("sink %s failed: %s", type(sink).__name__, exc)

    async def _poll(self, list_id: str) -> None:
        # Page through the list timeline until we pass tweets we've already seen.
        state_key = f"lists.last_seen.{list_id}"
        last_seen = self._store.get_meta(state_key)
        first_time = last_seen is None

        collected = []
        cursor: str | None = None
        max_pages = 1 if first_time else 5  # baseline once; then bounded paging
        newest = int(last_seen) if last_seen else 0

        for _ in range(max_pages):
            data = await self._api.list_tweets(
                list_id, self._cfg.lists.tweets_per_request, cursor
            )
            tweets = parse_list_timeline(
                data,
                include_replies=self._cfg.poll.include_replies,
                include_retweets=self._cfg.poll.include_retweets,
            )
            self._polls += 1
            if not tweets:
                break
            fresh = [t for t in tweets if int(t.id) > (int(last_seen) if last_seen else 0)]
            collected.extend(fresh)
            page_min = min(int(t.id) for t in tweets)
            # If the whole page is newer than last_seen, older new tweets may be
            # on the next page; keep paging. Else we've covered the new ones.
            if last_seen and page_min <= int(last_seen):
                break
            if first_time:
                break
            # advance cursor for next page
            cursor = _bottom_cursor(data)
            if not cursor:
                break

        if first_time:
            # Baseline: record newest id, emit nothing.
            for t in collected:
                newest = max(newest, int(t.id))
            if newest:
                self._store.set_meta(state_key, str(newest))
            log.info("baselined list (%d tweets ignored)", len(collected))
            return

        # Emit new tweets oldest-first, dedup on tweet id.
        for t in sorted(collected, key=lambda x: int(x.id)):
            if self._store.is_seen(t.id):
                continue
            await self._deliver(t)
            self._store.mark_seen(t.id, t.handle)
            newest = max(newest, int(t.id))
        if collected:
            log.info("list: %d new tweet(s)", len(collected))
        if newest:
            self._store.set_meta(state_key, str(newest))

    async def run(self, list_id: str) -> None:
        interval = self._cfg.lists.poll_interval_seconds
        log.info("polling list %s every %ds", list_id, interval)
        prune_at = time.monotonic() + 3600
        while not self._stop.is_set():
            start = time.monotonic()
            try:
                await self._poll(list_id)
            except SessionBlocked as exc:
                log.error("session blocked by X, stopping: %s", exc)
                return
            except Exception as exc:
                log.warning("list poll failed: %s", exc)
            if time.monotonic() >= prune_at:
                self._store.prune_seen()
                prune_at = time.monotonic() + 3600
            elapsed = time.monotonic() - start
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, interval - elapsed))
                break
            except asyncio.TimeoutError:
                pass
        log.info("stopped. polls=%d emitted=%d", self._polls, self._emitted)


def _bottom_cursor(data: dict) -> str | None:
    """Extract the bottom cursor from a list tweets timeline (for paging)."""
    lst = (data or {}).get("list", {}) or {}
    timeline = (lst.get("tweets_timeline", {}) or {}).get("timeline", {}) or {}
    for ins in timeline.get("instructions", []) or []:
        for entry in ins.get("entries", []) or []:
            content = entry.get("content", {}) or {}
            if content.get("cursorType") == "Bottom" or entry.get("entryId", "").startswith("cursor-bottom"):
                return content.get("value")
    return None


# List ops that must be present before Lists mode can run.
_REQUIRED_LIST_OPS = [
    "CreateList", "ListAddMember", "ListRemoveMember",
    "ListMembers", "ListByRestId", "ListLatestTweetsTimeline",
]


async def _amain(args) -> int:
    import os
    import signal

    import httpx

    from .api import XApi
    from .config import load_config
    from .harvest import harvest_to_cache, missing_ops
    from .queryids import QueryIdResolver
    from .sessions import Session
    from .sinks.base import build_sinks

    cfg = load_config(args.config_dir)
    logging.basicConfig(
        level=cfg.log_level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if not cfg.auth.valid():
        log.error("auth not configured in config.yaml")
        return 2

    # List ops live in lazy chunks the static scraper can't reach; if any are
    # missing from the cache, harvest them once with the headless browser.
    need = missing_ops(args.config_dir, _REQUIRED_LIST_OPS)
    if need:
        log.info("harvesting missing List query ids via headless browser: %s", need)
        try:
            await harvest_to_cache(args.config_dir)
        except RuntimeError as exc:
            log.error("harvest failed: %s", exc)
            return 3

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
    still = [op for op in _REQUIRED_LIST_OPS if not resolver.get(op)]
    if still:
        log.error("List query ids still missing after harvest: %s", still)
        await client.aclose()
        return 3

    store = Store(cfg.db_path)
    sinks = build_sinks(cfg.sinks) if not args.dry_run else []
    api = XApi(client, session, resolver, cfg.features)

    manager = ListManager(cfg, api, store)
    monitor = ListMonitor(cfg, api, store, sinks, dry_run=args.dry_run)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, monitor.request_stop)
        except NotImplementedError:
            pass

    try:
        list_id = await manager.ensure_list()
        if not args.no_sync:
            await manager.sync_members()
        if args.setup_only:
            log.info("setup complete: list %s ready with members synced", list_id)
        else:
            await monitor.run(list_id)
    finally:
        for sink in sinks:
            await sink.close()
        await client.aclose()
        store.close()
    return 0


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="xmon.lists", description="Lists-mode monitor")
    parser.add_argument("--config-dir", default=".")
    parser.add_argument("--dry-run", action="store_true", help="don't emit to sinks")
    parser.add_argument("--no-sync", action="store_true", help="skip adding members this run")
    parser.add_argument("--setup-only", action="store_true",
                        help="create list + sync members, then exit (no polling)")
    args = parser.parse_args()
    try:
        raise SystemExit(asyncio.run(_amain(args)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
