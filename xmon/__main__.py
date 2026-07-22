"""Entrypoint: wire everything together and run the monitor.

Usage:
  python -m xmon [--config-dir DIR] [--dry-run] [--check]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys

import httpx

from .api import XApi
from .config import load_config
from .queryids import QueryIdResolver
from .scheduler import Monitor
from .sessions import Session
from .sinks.base import build_sinks
from .store import Store


def _setup_logging(level: str, as_json: bool) -> None:
    if as_json:
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                return json.dumps(
                    {
                        "level": record.levelname,
                        "logger": record.name,
                        "msg": record.getMessage(),
                    }
                )

        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logging.basicConfig(level=level, handlers=[handler])
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )


async def _amain(args: argparse.Namespace) -> int:
    cfg = load_config(args.config_dir)
    _setup_logging(cfg.log_level, cfg.log_json)
    log = logging.getLogger("xmon")

    if not cfg.auth.valid():
        log.error(
            "auth not configured: set auth_token and ct0 in %s",
            os.path.join(args.config_dir, "config.yaml"),
        )
        return 2

    client = httpx.AsyncClient(http2=True, follow_redirects=True, timeout=30)
    session = Session(
        cfg.auth.auth_token,
        cfg.auth.ct0,
        cfg.auth.bearer,
        max_requests_per_15min=cfg.rate.max_requests_per_15min,
        min_gap_ms=cfg.rate.min_gap_ms,
    )
    resolver = QueryIdResolver(
        client,
        os.path.join(args.config_dir, ".queryids.json"),
        ttl_hours=cfg.queryids.cache_ttl_hours,
        fallback=cfg.queryids.fallback,
        # Scrape must be authenticated: the operation table ships only in the
        # logged-in bundle, so feed the resolver the session's cookie headers.
        headers_provider=session.base_headers,
    )
    await resolver.ensure()
    for op in ("UserByScreenName", "UserTweets"):
        log.info("query id %s = %s", op, resolver.get(op))

    store = Store(cfg.db_path)
    sinks = build_sinks(cfg.sinks) if not args.dry_run else []
    api = XApi(client, session, resolver, cfg.features)

    if args.check:
        # Resolve the first account and fetch its tweets once, then exit.
        if not cfg.accounts:
            log.error("no accounts to check")
            return 2
        handle = cfg.accounts[0]
        info = await api.resolve_user(handle)
        log.info("check: @%s -> %s", handle, info)
        data = await api.user_tweets(info["rest_id"], 5)
        from .parse import parse_timeline

        tweets = parse_timeline(
            data, handle, include_replies=True, include_retweets=True
        )
        log.info("check: fetched %d tweets; newest=%s", len(tweets),
                 tweets[-1].id if tweets else "-")
        await client.aclose()
        store.close()
        return 0

    monitor = Monitor(cfg, api, store, sinks, dry_run=args.dry_run)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, monitor.request_stop)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass

    try:
        await monitor.run()
    finally:
        for sink in sinks:
            await sink.close()
        await client.aclose()
        store.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="xmon", description="Single-session X monitor")
    parser.add_argument("--config-dir", default=".", help="dir with config.yaml etc.")
    parser.add_argument("--dry-run", action="store_true", help="don't emit to sinks")
    parser.add_argument(
        "--check",
        action="store_true",
        help="resolve+fetch the first account once and exit (connectivity test)",
    )
    args = parser.parse_args()
    try:
        sys.exit(asyncio.run(_amain(args)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
