"""Webhook sink: POST each new tweet as JSON to a configured URL."""
from __future__ import annotations

import logging

import httpx

from ..parse import Tweet
from .base import Sink

log = logging.getLogger("xmon.sink.webhook")


class WebhookSink(Sink):
    def __init__(self, url: str, *, timeout: float = 10.0) -> None:
        self._url = url
        self._client = httpx.AsyncClient(timeout=timeout)

    async def emit(self, tweet: Tweet) -> None:
        try:
            resp = await self._client.post(self._url, json=tweet.to_dict())
            if resp.status_code >= 300:
                log.warning("webhook %s -> HTTP %s", self._url, resp.status_code)
        except httpx.HTTPError as exc:
            log.warning("webhook post failed: %s", exc)

    async def close(self) -> None:
        await self._client.aclose()
