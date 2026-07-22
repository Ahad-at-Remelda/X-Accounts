"""Sink interface. A sink receives new tweets and delivers them somewhere."""
from __future__ import annotations

from typing import Any

from ..parse import Tweet


class Sink:
    async def emit(self, tweet: Tweet) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def close(self) -> None:
        pass


def build_sinks(specs: list[dict[str, Any]]) -> list[Sink]:
    """Instantiate sinks from config specs. Imports are local to keep optional
    deps (e.g. httpx for webhook) from being required when unused."""
    sinks: list[Sink] = []
    for spec in specs:
        stype = spec.get("type")
        if stype == "console":
            from .console import ConsoleSink

            sinks.append(ConsoleSink())
        elif stype == "file":
            from .file import FileSink

            sinks.append(FileSink(spec["path"]))
        elif stype == "webhook":
            from .webhook import WebhookSink

            sinks.append(
                WebhookSink(spec["url"], timeout=spec.get("timeout_seconds", 10))
            )
        else:
            raise ValueError(f"unknown sink type: {stype!r}")
    return sinks
