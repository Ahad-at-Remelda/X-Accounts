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

    # -- GraphQL mutations (POST) -------------------------------------------
    async def _graphql_post(
        self, operation: str, variables: dict[str, Any], _retried: bool = False
    ) -> dict[str, Any]:
        qid = self._resolver.get(operation)
        if not qid:
            await self._resolver.refresh()
            qid = self._resolver.get(operation)
        if not qid:
            raise ApiError(f"no query id available for {operation}")

        # Mutations take a JSON body and, verified live, do NOT want a features
        # blob (sending one is harmless but we omit it for a clean request).
        body = {"variables": variables, "queryId": qid}
        url = f"{_GQL}/{qid}/{operation}"

        await self._session.acquire()
        resp = await self._client.post(
            url, json=body, headers=self._session.base_headers()
        )
        self._session.note_headers(resp.headers)

        if resp.status_code == 200:
            return self._unwrap(resp.json(), operation)
        if resp.status_code == 429:
            await self._session.backoff_429()
            return await self._graphql_post(operation, variables, _retried)
        if resp.status_code in (401, 403):
            body_txt = resp.text[:500]
            if "Could not authenticate" in body_txt or resp.status_code == 401:
                raise SessionBlocked(f"{operation}: {resp.status_code} {body_txt}")
            raise UserUnavailable(f"{operation}: {resp.status_code} {body_txt}")
        if resp.status_code == 404 and not _retried:
            await self._resolver.refresh()
            return await self._graphql_post(operation, variables, _retried=True)
        raise ApiError(f"{operation}: HTTP {resp.status_code}: {resp.text[:300]}")

    # -- List operations ----------------------------------------------------
    async def create_list(
        self, name: str, description: str = "", private: bool = True
    ) -> str:
        data = await self._graphql_post(
            "CreateList",
            {"isPrivate": private, "name": name, "description": description},
        )
        lst = (data or {}).get("list") or {}
        list_id = lst.get("id_str") or lst.get("rest_id")
        if not list_id:
            raise ApiError(f"CreateList returned no id: {json.dumps(data)[:300]}")
        return str(list_id)

    async def add_list_member(self, list_id: str, user_id: str) -> None:
        # Note: X returns a cosmetic 'DecodeException' error alongside a valid
        # data.list on success; _unwrap ignores errors when data is present.
        await self._graphql_post(
            "ListAddMember", {"listId": str(list_id), "userId": str(user_id)}
        )

    async def remove_list_member(self, list_id: str, user_id: str) -> None:
        await self._graphql_post(
            "ListRemoveMember", {"listId": str(list_id), "userId": str(user_id)}
        )

    async def list_meta(self, list_id: str) -> dict[str, Any]:
        data = await self._graphql_get("ListByRestId", {"listId": str(list_id)})
        return (data or {}).get("list") or {}

    async def list_members_page(
        self, list_id: str, count: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        variables: dict[str, Any] = {
            "listId": str(list_id),
            "count": count,
            "withSafetyModeUserFields": True,
        }
        if cursor:
            variables["cursor"] = cursor
        return await self._graphql_get("ListMembers", variables)

    async def list_tweets(
        self, list_id: str, count: int = 20, cursor: str | None = None
    ) -> dict[str, Any]:
        variables: dict[str, Any] = {"listId": str(list_id), "count": count}
        if cursor:
            variables["cursor"] = cursor
        return await self._graphql_get("ListLatestTweetsTimeline", variables)
