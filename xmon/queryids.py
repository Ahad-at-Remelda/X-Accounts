"""Query-ID resolver.

X serves GraphQL under /graphql/<queryId>/<OperationName> and rotates the
queryId on every web deploy. Hardcoding it is why scrapers rot. Instead we
read x.com's own JS bundles, extract the {operationName -> queryId} table they
ship, cache it to disk with a TTL, and refresh on demand (e.g. after a 404).

Two hard-won facts about x.com's current build, both verified against live
responses rather than assumed:

  1. The operation table lives ONLY in the logged-in app bundle. The
     logged-out homepage ships an onboarding bundle with zero GraphQL
     operations. So the scrape must send the session cookie.

  2. There are two bundle layouts in the wild: the legacy
     `responsive-web/client-web/{main,api}.<hash>.js` and the newer
     `x-web/.../assets/<name>-<hash>.js` (rolldown) graph. We handle both by
     collecting every referenced .js chunk and grepping each one.

The config `fallback` map is the safety net used when a scrape yields nothing
(e.g. x.com changed the minified shape); refresh-on-404 upstream bumps a stale
id the moment it actually breaks.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Callable
from urllib.parse import urljoin

import httpx

log = logging.getLogger("xmon.queryids")

_HOME = "https://x.com/"

# Any x.com/abs.twimg JS chunk, absolute or root-relative.
_ABS_JS_RE = re.compile(r'https://abs\.twimg\.com/[\w./-]+?\.js')
# Root/relative chunk refs inside a bundle, e.g. "assets/foo-Ab12.js".
_REL_JS_RE = re.compile(r'["\'“]((?:\.?/)?assets/[\w./-]+?\.js)["\'”]')

# The operation table is a sequence of minified object literals. Build tools
# order the keys differently, so match both orderings within a bounded window.
_OP_FWD_RE = re.compile(r'queryId:"([\w-]{8,})"[^{}]{0,160}?operationName:"(\w+)"')
_OP_REV_RE = re.compile(r'operationName:"(\w+)"[^{}]{0,160}?queryId:"([\w-]{8,})"')

# Cap how many chunks we fetch per refresh so a build with hundreds of chunks
# can't turn one refresh into thousands of requests.
_MAX_CHUNKS = 400
_FETCH_CONCURRENCY = 10


class QueryIdResolver:
    def __init__(
        self,
        client: httpx.AsyncClient,
        cache_path: str,
        *,
        ttl_hours: int,
        fallback: dict[str, str],
        headers_provider: Callable[[], dict[str, str]] | None = None,
    ) -> None:
        self._client = client
        self._cache_path = cache_path
        self._ttl = ttl_hours * 3600
        self._fallback = dict(fallback)
        # Returns authenticated headers (cookie + bearer); without it the scrape
        # sees only the logged-out bundle and finds nothing.
        self._headers_provider = headers_provider
        self._map: dict[str, str] = {}
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    # -- cache --------------------------------------------------------------
    def _load_cache(self) -> None:
        if not os.path.exists(self._cache_path):
            return
        try:
            with open(self._cache_path, encoding="utf-8") as fh:
                data = json.load(fh)
            self._map = dict(data.get("map", {}))
            self._fetched_at = float(data.get("fetched_at", 0))
        except (OSError, ValueError, json.JSONDecodeError):
            log.warning("query-id cache unreadable; ignoring")

    def _save_cache(self) -> None:
        tmp = self._cache_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"map": self._map, "fetched_at": self._fetched_at}, fh)
            os.replace(tmp, self._cache_path)
        except OSError as exc:
            log.warning("could not write query-id cache: %s", exc)

    def _fresh(self) -> bool:
        return bool(self._map) and (time.time() - self._fetched_at) < self._ttl

    # -- scrape -------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        if self._headers_provider:
            src = self._headers_provider()
            # Fetch x.com/ and its JS as a browser NAVIGATION, not an API call.
            # The HTML endpoint rejects the API-only headers (Authorization
            # bearer, x-twitter-auth-type, x-csrf-token, ...) with 401 -- it
            # authenticates HTML from the cookie alone. Keep only browser-y
            # headers plus the cookie, and drop everything API-specific.
            keep = {"cookie", "user-agent", "accept-language"}
            h = {k: v for k, v in src.items() if k.lower() in keep}
            h["accept"] = (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "application/javascript,*/*;q=0.8"
            )
            return h
        return {
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "accept": "text/html,*/*",
        }

    @staticmethod
    def _extract_ops(text: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for qid, op in _OP_FWD_RE.findall(text):
            out[op] = qid
        for op, qid in _OP_REV_RE.findall(text):
            out.setdefault(op, qid)
        return out

    async def _fetch(self, url: str, headers: dict[str, str]) -> str:
        try:
            resp = await self._client.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                return resp.text
        except httpx.HTTPError:
            pass
        return ""

    def _discover_chunks(self, html: str, base: str) -> set[str]:
        urls: set[str] = set(_ABS_JS_RE.findall(html))
        for rel in _REL_JS_RE.findall(html):
            urls.add(urljoin(base, rel))
        return urls

    async def _scrape(self) -> dict[str, str]:
        headers = self._headers()
        html = await self._fetch(_HOME, headers)
        if not html:
            log.warning("query-id scrape: home fetch failed")
            return {}

        # First-level chunks from the homepage.
        chunks = self._discover_chunks(html, _HOME)

        # Entry bundles reference further `assets/*.js` chunks relative to their
        # own directory; expand one level so we reach the app/api chunk that
        # actually holds the operation table.
        second: set[str] = set()
        entry_bundles = [u for u in chunks if "entry" in u or "main" in u]
        for burl in entry_bundles[:10]:
            btext = await self._fetch(burl, headers)
            if btext:
                second |= self._discover_chunks(btext, burl)
                second |= self.__class__._maybe_early(btext)
        chunks |= second

        chunks = set(list(chunks)[:_MAX_CHUNKS])
        if not chunks:
            log.warning("query-id scrape: no JS chunks discovered")
            return {}

        found: dict[str, str] = {}
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def worker(url: str) -> None:
            async with sem:
                text = await self._fetch(url, headers)
            if text and "operationName" in text:
                ops = self._extract_ops(text)
                if ops:
                    found.update(ops)

        await asyncio.gather(*(worker(u) for u in chunks))

        if found:
            log.info(
                "scraped %d GraphQL query ids from x.com JS (%d chunks scanned)",
                len(found),
                len(chunks),
            )
        else:
            log.warning(
                "query-id scrape scanned %d chunks but found no operations "
                "(cookie set? logged-in bundle needed)",
                len(chunks),
            )
        return found

    @staticmethod
    def _maybe_early(text: str) -> set[str]:
        # Some builds embed the full absolute chunk list in the entry bundle.
        return set(_ABS_JS_RE.findall(text))

    # -- public -------------------------------------------------------------
    async def refresh(self) -> None:
        async with self._lock:
            scraped = await self._scrape()
            if scraped:
                self._map.update(scraped)
                self._fetched_at = time.time()
                self._save_cache()

    async def ensure(self) -> None:
        """Ensure a usable map exists, via cache -> scrape -> fallback."""
        if self._fresh():
            return
        self._load_cache()
        if self._fresh():
            return
        await self.refresh()
        if not self._map:
            log.warning("using fallback query ids from config (may be stale)")
            self._map = dict(self._fallback)

    def get(self, operation: str) -> str | None:
        return self._map.get(operation) or self._fallback.get(operation)
