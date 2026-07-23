"""Headless-browser query-id harvester.

Static scraping reaches only the boot bundles, which carry the "hot" ops
(UserTweets, UserByScreenName). The List-management ops live in lazily-loaded
chunks whose URLs x.com builds at runtime — unreachable without executing the
app. So for those we drive a real (headless) Chromium: log in with the session
cookie, visit the pages that trigger the lazy chunks, and read the query-id
table straight out of every JS file the browser loads.

The captured map is merged into the same .queryids.json the static resolver
uses, so the rest of xmon transparently gains the List ops.

Requires:  pip install playwright  &&  python -m playwright install chromium

Run:  python -m xmon.harvest --config-dir .
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import time

from .config import load_config

log = logging.getLogger("xmon.harvest")

# Same operation-table patterns the static resolver uses.
_OP_FWD_RE = re.compile(r'queryId:"([\w-]{8,})"[^{}]{0,160}?operationName:"(\w+)"')
_OP_REV_RE = re.compile(r'operationName:"(\w+)"[^{}]{0,160}?queryId:"([\w-]{8,})"')

# Pages whose chunks collectively define the ops we care about. /i/lists and the
# create flow pull in the list-management chunk (CreateList, ListAddMember, ...).
_VISIT = [
    "https://x.com/home",
    "https://x.com/i/lists",
    "https://x.com/i/lists/create",
]

# Ops we specifically need for Lists mode; used only to report coverage.
_WANTED = [
    "UserByScreenName", "UserTweets",
    "ListLatestTweetsTimeline", "ListMembers", "ListByRestId",
    "CreateList", "ListAddMember", "ListRemoveMember", "UpdateList",
]


def _extract(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for qid, op in _OP_FWD_RE.findall(text):
        out[op] = qid
    for op, qid in _OP_REV_RE.findall(text):
        out.setdefault(op, qid)
    return out


async def harvest(auth_token: str, ct0: str, *, dwell: float = 6.0,
                  headless: bool = True) -> dict[str, str]:
    """Drive Chromium and return the {operationName: queryId} map it exposes."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "playwright not installed. Run:\n"
            "  pip install playwright\n"
            "  python -m playwright install chromium"
        ) from exc

    ops: dict[str, str] = {}
    js_count = 0

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=headless)
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                f"could not launch chromium ({exc}). "
                "Run: python -m playwright install chromium"
            ) from exc

        ctx = await browser.new_context()
        # Cookie on .x.com covers x.com and i.api subrequests.
        await ctx.add_cookies([
            {"name": "auth_token", "value": auth_token, "domain": ".x.com", "path": "/"},
            {"name": "ct0", "value": ct0, "domain": ".x.com", "path": "/"},
        ])
        page = await ctx.new_page()

        async def on_response(resp) -> None:
            nonlocal js_count
            url = resp.url
            if url.endswith(".js") and "abs.twimg.com" in url:
                try:
                    body = await resp.text()
                except Exception:
                    return
                js_count += 1
                found = _extract(body)
                if found:
                    ops.update(found)

        page.on("response", on_response)

        for url in _VISIT:
            try:
                # networkidle never settles on x.com (long-lived streams), so
                # load the DOM then dwell to let lazy chunks stream in.
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception as exc:
                log.warning("nav %s: %s", url, exc)
            await asyncio.sleep(dwell)

        await browser.close()

    log.info("harvest scanned %d JS files, captured %d ops", js_count, len(ops))
    return ops


def _merge_into_cache(cache_path: str, ops: dict[str, str]) -> dict[str, str]:
    existing: dict[str, str] = {}
    fetched_at = 0.0
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as fh:
                data = json.load(fh)
            existing = dict(data.get("map", {}))
        except (OSError, ValueError):
            pass
    existing.update(ops)
    fetched_at = time.time()
    tmp = cache_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"map": existing, "fetched_at": fetched_at}, fh)
    os.replace(tmp, cache_path)
    return existing


async def harvest_to_cache(config_dir: str, *, headless: bool = True) -> dict[str, str]:
    """Harvest and merge into <config_dir>/.queryids.json. Returns the full map."""
    cfg = load_config(config_dir)
    if not cfg.auth.valid():
        raise RuntimeError("auth not configured in config.yaml")
    ops = await harvest(cfg.auth.auth_token, cfg.auth.ct0, headless=headless)
    if not ops:
        raise RuntimeError("harvest captured no operations (cookie expired?)")
    cache_path = os.path.join(config_dir, ".queryids.json")
    return _merge_into_cache(cache_path, ops)


def missing_ops(config_dir: str, required: list[str]) -> list[str]:
    """Which required ops are absent from the on-disk cache."""
    cache_path = os.path.join(config_dir, ".queryids.json")
    have: dict[str, str] = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as fh:
                have = json.load(fh).get("map", {})
        except (OSError, ValueError):
            pass
    return [op for op in required if op not in have]


async def _amain(args) -> int:
    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    full = await harvest_to_cache(args.config_dir, headless=not args.headful)
    print(f"\nCaptured {len(full)} query ids -> "
          f"{os.path.join(args.config_dir, '.queryids.json')}\n")
    print("Coverage of ops xmon needs:")
    for op in _WANTED:
        print(f"  {op:28} {full.get(op, '-- MISSING --')}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="xmon.harvest",
                                     description="Harvest GraphQL query ids via headless browser")
    parser.add_argument("--config-dir", default=".")
    parser.add_argument("--headful", action="store_true",
                        help="show the browser window (debugging)")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
