"""Configuration loading: config.yaml, accounts.txt, features.json."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class AuthConfig:
    auth_token: str
    ct0: str
    bearer: str

    def valid(self) -> bool:
        placeholder = ("", None)
        return (
            self.auth_token not in placeholder
            and not self.auth_token.startswith("PUT_YOUR_")
            and self.ct0 not in placeholder
            and not self.ct0.startswith("PUT_YOUR_")
            and bool(self.bearer)
        )


@dataclass
class PollConfig:
    interval_seconds: int = 90
    jitter_seconds: int = 20
    tweets_per_request: int = 20
    include_replies: bool = False
    include_retweets: bool = False
    backfill_on_first_seen: bool = False


@dataclass
class RateConfig:
    max_requests_per_15min: int = 450
    min_gap_ms: int = 800


@dataclass
class QueryIdConfig:
    cache_ttl_hours: int = 12
    fallback: dict[str, str] = field(default_factory=dict)


@dataclass
class Config:
    auth: AuthConfig
    poll: PollConfig
    rate: RateConfig
    queryids: QueryIdConfig
    sinks: list[dict[str, Any]]
    db_path: str
    log_level: str
    log_json: bool
    features: dict[str, dict[str, Any]]
    accounts: list[str]

    @property
    def base_dir(self) -> str:
        return self._base_dir

    _base_dir: str = "."


def _load_accounts(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    out: list[str] = []
    seen: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            handle = line.lstrip("@").strip().lower()
            if handle and handle not in seen:
                seen.add(handle)
                out.append(handle)
    return out


def _load_features(path: str) -> dict[str, dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    # Drop documentation-only keys.
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def load_config(base_dir: str = ".") -> Config:
    cfg_path = os.path.join(base_dir, "config.yaml")
    with open(cfg_path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    auth_raw = raw.get("auth", {})
    auth = AuthConfig(
        auth_token=str(auth_raw.get("auth_token", "")),
        ct0=str(auth_raw.get("ct0", "")),
        bearer=str(auth_raw.get("bearer", "")),
    )

    poll_raw = raw.get("poll", {})
    poll = PollConfig(
        interval_seconds=int(poll_raw.get("interval_seconds", 90)),
        jitter_seconds=int(poll_raw.get("jitter_seconds", 20)),
        tweets_per_request=int(poll_raw.get("tweets_per_request", 20)),
        include_replies=bool(poll_raw.get("include_replies", False)),
        include_retweets=bool(poll_raw.get("include_retweets", False)),
        backfill_on_first_seen=bool(poll_raw.get("backfill_on_first_seen", False)),
    )

    rate_raw = raw.get("rate", {})
    rate = RateConfig(
        max_requests_per_15min=int(rate_raw.get("max_requests_per_15min", 450)),
        min_gap_ms=int(rate_raw.get("min_gap_ms", 800)),
    )

    q_raw = raw.get("queryids", {})
    queryids = QueryIdConfig(
        cache_ttl_hours=int(q_raw.get("cache_ttl_hours", 12)),
        fallback=dict(q_raw.get("fallback", {})),
    )

    storage_raw = raw.get("storage", {})
    log_raw = raw.get("log", {})

    features = _load_features(os.path.join(base_dir, "features.json"))
    accounts = _load_accounts(os.path.join(base_dir, "accounts.txt"))

    cfg = Config(
        auth=auth,
        poll=poll,
        rate=rate,
        queryids=queryids,
        sinks=list(raw.get("sinks", []) or [{"type": "console"}]),
        db_path=os.path.join(base_dir, str(storage_raw.get("db_path", "xmon.db"))),
        log_level=str(log_raw.get("level", "INFO")).upper(),
        log_json=bool(log_raw.get("json", False)),
        features=features,
        accounts=accounts,
    )
    cfg._base_dir = base_dir
    return cfg
