"""Tiny local web UI: a button that fetches each monitored account's latest tweet.

A browser can't call x.com's internal GraphQL itself (no cookie, CORS). So this
serves a static page plus one JSON endpoint, /api/latest, that reuses the same
XApi the monitor uses. One asyncio loop + httpx client live in a background
thread; each HTTP request bridges into it.

Run:
    python -m xmon.web --config-dir . --port 8787
then open http://127.0.0.1:8787
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

from .api import UserUnavailable, XApi
from .config import load_config
from .parse import parse_timeline
from .queryids import QueryIdResolver
from .sessions import Session, SessionBlocked
from .store import Store, UserRecord
from .watch import _created_epoch

log = logging.getLogger("xmon.web")


class Backend:
    """Owns the async client/session/api on a private event loop thread."""

    def __init__(self, config_dir: str) -> None:
        self.config_dir = config_dir
        self.cfg = load_config(config_dir)
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._setup_error: str | None = None
        # When this process started watching. A tweet counts as a genuine
        # latency measurement only if it was posted after this instant;
        # tweets already present at startup are "pre-existing", not detections.
        self._start = time.time()
        # handle -> {"id": int, "first_seen": float} : the moment we first saw
        # each account's newest tweet id, so detection lag is stable across polls.
        self._seen: dict[str, dict] = {}

    # -- lifecycle ----------------------------------------------------------
    def start(self) -> None:
        self._thread.start()
        self._ready.wait()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._setup())
        except Exception as exc:  # surfaced to the page instead of crashing
            self._setup_error = str(exc)
            log.error("backend setup failed: %s", exc)
        finally:
            self._ready.set()
        self._loop.run_forever()

    async def _setup(self) -> None:
        if not self.cfg.auth.valid():
            raise RuntimeError(
                "auth not configured: set auth_token and ct0 in config.yaml"
            )
        self._client = httpx.AsyncClient(http2=True, follow_redirects=True, timeout=30)
        self._session = Session(
            self.cfg.auth.auth_token,
            self.cfg.auth.ct0,
            self.cfg.auth.bearer,
            max_requests_per_15min=self.cfg.rate.max_requests_per_15min,
            min_gap_ms=self.cfg.rate.min_gap_ms,
        )
        self._resolver = QueryIdResolver(
            self._client,
            os.path.join(self.config_dir, ".queryids.json"),
            ttl_hours=self.cfg.queryids.cache_ttl_hours,
            fallback=self.cfg.queryids.fallback,
            headers_provider=self._session.base_headers,
        )
        await self._resolver.ensure()
        self._store = Store(self.cfg.db_path)
        self._api = XApi(self._client, self._session, self._resolver, self.cfg.features)

    # -- request bridge -----------------------------------------------------
    def fetch_latest_sync(self) -> dict:
        if self._setup_error:
            return {"error": self._setup_error, "count": 0, "accounts": []}
        fut = asyncio.run_coroutine_threadsafe(self._fetch_latest(), self._loop)
        return fut.result()

    async def _fetch_latest(self) -> dict:
        accounts = []
        for handle in self.cfg.accounts:
            accounts.append(await self._one(handle))
        return {"error": None, "count": len(accounts), "accounts": accounts}

    async def _one(self, handle: str) -> dict:
        item: dict = {"handle": handle, "name": handle, "tweet": None, "error": None}
        try:
            rec = self._store.get_user(handle)
            if not rec:
                info = await self._api.resolve_user(handle)
                rec = UserRecord(handle, info["rest_id"], info["name"])
                self._store.put_user(rec)
            item["name"] = rec.name
            data = await self._api.user_tweets(rec.rest_id, 5)
            # include everything so the *actual* latest post always shows.
            tweets = parse_timeline(
                data, handle, include_replies=True, include_retweets=True
            )
            if tweets:
                t = tweets[-1]  # chronological: newest is last
                cid = int(t.id)
                created = _created_epoch(t.created_at)

                # Record first-sighting time for this tweet id so the measured
                # lag stays fixed once detected (doesn't grow on later polls).
                prev = self._seen.get(handle)
                if prev is None or cid > prev["id"]:
                    self._seen[handle] = {"id": cid, "first_seen": time.time()}
                first_seen = self._seen[handle]["first_seen"]

                # Genuine detection latency only for tweets posted after we
                # started watching; otherwise it's a pre-existing tweet.
                measured = created is not None and created >= self._start
                detected_lag = round(first_seen - created, 1) if measured else None
                age = round(time.time() - created, 1) if created is not None else None

                item["tweet"] = {
                    "id": t.id,
                    "text": t.text,
                    "url": t.url,
                    "created_at": t.created_at,
                    "is_retweet": t.is_retweet,
                    "is_reply": t.is_reply,
                    "is_quote": t.is_quote,
                    "measured": measured,
                    "detected_lag_seconds": detected_lag,
                    "age_seconds": age,
                }
        except SessionBlocked as exc:
            item["error"] = f"session blocked by X: {exc}"
        except UserUnavailable as exc:
            item["error"] = f"unavailable: {exc}"
        except Exception as exc:  # keep other accounts working
            item["error"] = str(exc)
        return item


def _make_handler(backend: Backend, webroot: str):
    index_path = os.path.join(webroot, "index.html")

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                try:
                    with open(index_path, "rb") as fh:
                        self._send(200, fh.read(), "text/html; charset=utf-8")
                except OSError:
                    self._send(500, b"index.html missing", "text/plain")
            elif path == "/api/latest":
                try:
                    data = backend.fetch_latest_sync()
                    self._send(
                        200,
                        json.dumps(data).encode("utf-8"),
                        "application/json; charset=utf-8",
                    )
                except Exception as exc:  # pragma: no cover
                    self._send(
                        500,
                        json.dumps({"error": str(exc)}).encode(),
                        "application/json",
                    )
            else:
                self._send(404, b"not found", "text/plain")

        def log_message(self, *args) -> None:  # keep the console clean
            return

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(prog="xmon.web", description="xmon web UI")
    parser.add_argument("--config-dir", default=".")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    backend = Backend(args.config_dir)
    backend.start()

    webroot = os.path.join(args.config_dir, "web")
    handler = _make_handler(backend, webroot)
    httpd = ThreadingHTTPServer((args.host, args.port), handler)
    log.info("xmon web UI on http://%s:%d  (%d accounts)", args.host, args.port,
             len(backend.cfg.accounts))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")
        httpd.shutdown()


if __name__ == "__main__":
    main()
