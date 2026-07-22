# xmon — single-session X account monitor

Watches a set of X accounts via X's internal GraphQL endpoints
(`UserByScreenName` + `UserTweets`, the same operations x.com's own web app
calls) and emits new tweets to pluggable sinks. Built for **< 200 accounts at
1–2 minute freshness on one session**.

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
