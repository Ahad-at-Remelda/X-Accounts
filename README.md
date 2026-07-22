# xmon — single-session X account monitor

Watches a set of X accounts via X's internal GraphQL endpoints (the same
operations x.com's own web app calls) and emits new tweets to pluggable sinks.

Two modes, one session:

- **Per-account** (`python -m xmon`) — polls `UserTweets` once per account.
  Simple, tightest per-account freshness. Rate limit caps this at **tens** of
  accounts (see Capacity).
- **Lists** (`python -m xmon.lists`) — puts every account into one X List and
  polls `ListLatestTweetsTimeline`: **one request returns all members' recent
  tweets**. Scales to **hundreds–thousands** of accounts on the same session.

## Capacity (measured, not assumed)

The `UserTweets` / `ListLatestTweetsTimeline` rate limit is **50 requests per
15 min per session** (read live from the `x-rate-limit-limit` header).

- Per-account: `max_accounts = 50 × interval_seconds ÷ 900`. So ~3 at 1-min,
  ~16 at 5-min, ~50 at 15-min freshness.
- Lists: 1 request covers the whole List (up to 5,000 members). A List of
  thousands usually costs 1–3 requests per poll, so one session monitors
  thousands at a steady cadence. **This is the free way to scale.**

There is no free push/streaming from X; polling is the only free mechanism, and
Lists batching is how you make polling cheap.

## Scope / design line

This uses **one** logged-in session — your cookie, one rate-limit bucket. There
is **no account pool and no cookie rotation**. If X blocks the session, xmon
logs it and exits cleanly rather than swapping in another identity to keep
access alive. That boundary is deliberate; everything here is sized so a single
session comfortably carries the target load.

Using undocumented endpoints is against X's Terms of Service, and a session used
this way can be rate-limited or suspended. Use an account you are willing to
lose.

## Layout

```
config.yaml        auth + polling + rate + sinks
accounts.txt       one handle per line
features.json      GraphQL feature flags (edit when X adds a required one)
xmon/
  config.py        loads the three files above
  sessions.py      single session + rate governor (spacing, soft cap, 429 backoff)
  queryids.py      scrapes x.com JS for /graphql/<id>/<op>, caches 12h, refresh-on-404
  api.py           UserByScreenName + UserTweets, header/rate plumbing
  parse.py         timeline tree -> Tweet objects (skips pinned; filters RT/reply)
  store.py         SQLite (WAL): user-id cache, last_seen, seen-id dedup
  scheduler.py     per-account timer loop
  sinks/           console (default), file (NDJSON), webhook
systemd/xmon.service
```

## Setup

1. Install deps:
   ```
   pip install -r requirements.txt
   ```
2. Put your credentials in `config.yaml` under `auth:` — from a browser logged
   in to x.com (DevTools → Application → Cookies → https://x.com):
   - `auth_token` (the session cookie)
   - `ct0` (the CSRF token)

   The `bearer` value is already filled in; it is the public web bearer x.com
   ships to every visitor, not a secret.
3. List handles in `accounts.txt` (one per line, `@` optional).

## Run

```
python -m xmon --check        # resolve + fetch the first account once, then exit
python -m xmon --dry-run      # full loop, but don't deliver to sinks
python -m xmon                # run for real
```

`--check` is the fastest way to confirm your cookie works and the query-id
scrape succeeded. On success it logs the resolved user id and the newest tweet
id it can see.

## Lists mode (scaling to hundreds/thousands)

Configured under `lists:` in `config.yaml` (`enabled: true` by default). xmon
creates one private X List, adds every `accounts.txt` handle to it, and polls
the List timeline as a unit.

```
pip install playwright && python -m playwright install chromium   # one-time
python -m xmon.harvest         # capture List query ids via headless browser
python -m xmon.lists --setup-only   # create list + add all members, then exit
python -m xmon.lists                 # run the monitor for real
python -m xmon.lists --dry-run       # poll but don't deliver
```

- **Query ids for List ops** live in x.com chunks that only load when you open
  the Lists UI, so static scraping can't reach them. `python -m xmon.harvest`
  drives a headless Chromium (logged in with your cookie), visits the Lists
  pages, and reads every operation's query id straight out of the JS the browser
  loads — fully automatic and self-healing. `xmon.lists` runs it on its own if
  any List op is missing from the cache.
- **List creation is programmatic** (`CreateList`). The created List id is
  remembered in the DB (`meta` table), so restarts reuse it. To use an existing
  List instead, set `lists.list_id` in config.
- **Member sync**: every run adds any `accounts.txt` handle not yet in the List.
  Set `lists.prune_extra: true` to also remove members no longer in the file.
- The monitored accounts are **not notified** — the List is private, and
  membership of a private List is not visible to the people in it.

## Dashboard (web UI)

```
python -m xmon.web          # then open http://127.0.0.1:8787
```

A one-page dashboard: a card per monitored account showing its **latest tweet**,
auto-refreshing, with a green "⚡ detected Ns after posting" latency badge when a
new post is caught live. When `lists.enabled` is true it reads from the List —
**one request covers every account per refresh** (`lists.web_pages`, default 1).
Accounts not in the most recent page keep their last-known tweet.

**Adding accounts is one step:** edit `accounts.txt`, then run
`python -m xmon.web`. On startup the dashboard auto-adds any new handles to the
List in the background (harvesting List query-ids first if needed), so you never
run a separate sync command. The page starts serving immediately and shows
"⏳ adding members…" while it works; new members appear once added and once they
post. Disable this with `lists.sync_on_web_start: false` (then use
`python -m xmon.lists --setup-only` to sync manually).

Mind the **50 req / 15 min** budget: the dashboard's auto-refresh spends it.
The default 20s interval ≈ 45 req/15 min. Lower it only for short latency-test
bursts, and don't run the dashboard and `xmon.lists` monitor against the same
session non-stop — they share the budget.

## How the fragile parts stay alive

- **Query IDs** (`/graphql/<queryId>/UserTweets`) rotate on every x.com deploy.
  xmon scrapes them from x.com's own JS at boot — **authenticated**, because the
  operation table ships only in the logged-in bundle — caches for 12h, and
  re-scrapes automatically on a 404. The `queryids.fallback` block in
  `config.yaml` is only a first-boot safety net.
- **Feature flags** are in `features.json`. When a response errors with a
  missing-feature message, add the named flag (usually `true`) — no code change.
- **Baseline on first sight**: a newly added account's existing tweets are
  recorded as "seen" without emitting, so adding an account doesn't flood your
  sink. Set `poll.backfill_on_first_seen: true` to emit them instead.

## Adding a delivery channel

Implement `emit(tweet)` in a new file under `xmon/sinks/`, register it in
`sinks/base.py::build_sinks`, and reference it in `config.yaml`. The `Tweet`
object (see `parse.py`) already carries id, handle, text, url, media, and the
RT/reply/quote flags.

## Deploy as a service

```
sudo cp systemd/xmon.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now xmon
journalctl -u xmon -f
```
