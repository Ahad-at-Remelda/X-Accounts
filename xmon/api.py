"""GraphQL API layer: UserByScreenName (handle -> rest_id) and UserTweets.

Wraps the httpx client, injects auth headers, applies the rate governor, and
retries once after refreshing query ids when X returns 404 (deploy rotated the
id) or complains about a missing feature flag.
"""
from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import quote

import httpx

from .queryids import QueryIdResolver
from .sessions import Session, SessionBlocked

log = logging.getLogger("xmon.api")

_GQL = "https://x.com/i/api/graphql"


class ApiError(Exception):
    pass


class UserUnavailable(Exception):
    """Handle does not exist / is suspended / protected."""


class XApi:
    def __init__(
        self,
        client: httpx.AsyncClient,
        session: Session,
        resolver: QueryIdResolver,
        features: dict[str, dict[str, Any]],
    ) -> None:
        self._client = client
        self._session = session
        self._resolver = resolver
        self._features = features

    async def _graphql_get(
        self, operation: str, variables: dict[str, Any], _retried: bool = False
    ) -> dict[str, Any]:
        qid = self._resolver.get(operation)
        if not qid:
            await self._resolver.refresh()
            qid = self._resolver.get(operation)
        if not qid:
            raise ApiError(f"no query id available for {operation}")

        features = self._features.get(operation, {})
        params = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(features, separators=(",", ":")),
        }
        url = f"{_GQL}/{qid}/{operation}"

        await self._session.acquire()
        resp = await self._client.get(
            url, params=params, headers=self._session.base_headers()
        )
        self._session.note_headers(resp.headers)

        if resp.status_code == 200:
            return self._unwrap(resp.json(), operation)

        if resp.status_code == 429:
            await self._session.backoff_429()
            return await self._graphql_get(operation, variables, _retried)

        if resp.status_code in (401, 403):
            # Distinguish a dead session from a per-user restriction.
            body = resp.text[:500]
            if "Could not authenticate" in body or resp.status_code == 401:
                raise SessionBlocked(f"{operation}: {resp.status_code} {body}")
            raise UserUnavailable(f"{operation}: {resp.status_code} {body}")

        if resp.status_code == 404 and not _retried:
            # Deploy likely rotated the query id; refresh once and retry.
            log.info("%s 404; refreshing query ids and retrying", operation)
            await self._resolver.refresh()
            return await self._graphql_get(operation, variables, _retried=True)

        raise ApiError(f"{operation}: HTTP {resp.status_code}: {resp.text[:300]}")

    def _unwrap(self, payload: dict[str, Any], operation: str) -> dict[str, Any]:
        errors = payload.get("errors")
        if errors:
            msgs = "; ".join(e.get("message", "?") for e in errors)
            # A missing feature flag shows up here; make it actionable.
            if "feature" in msgs.lower():
                raise ApiError(
                    f"{operation} feature error (add the named flag to features.json): {msgs}"
                )
            # Some errors coexist with usable data (e.g. one bad tweet); only
            # raise if there is no data at all.
            if not payload.get("data"):
                raise ApiError(f"{operation} errors: {msgs}")
            log.debug("%s partial errors: %s", operation, msgs)
        return payload.get("data", {})

    # -- operations ---------------------------------------------------------
    async def resolve_user(self, handle: str) -> dict[str, Any]:
        data = await self._graphql_get(
            "UserByScreenName",
            {
                "screen_name": handle,
                "withSafetyModeUserFields": True,
            },
        )
        user = (data or {}).get("user", {}).get("result")
        if not user or user.get("__typename") == "UserUnavailable":
            reason = (user or {}).get("reason", "unavailable")
            raise UserUnavailable(f"@{handle}: {reason}")
        rest_id = user.get("rest_id")
        legacy = user.get("legacy", {})
        core = user.get("core", {})
        name = core.get("name") or legacy.get("name") or handle
        if not rest_id:
            raise UserUnavailable(f"@{handle}: no rest_id in response")
        return {"rest_id": rest_id, "name": name}

    async def user_tweets(self, rest_id: str, count: int) -> dict[str, Any]:
        return await self._graphql_get(
            "UserTweets",
            {
                "userId": rest_id,
                "count": count,
                "includePromotedContent": False,
                "withQuickPromoteEligibilityTweetFields": False,
                "withVoice": True,
                "withV2Timeline": True,
            },
        )
