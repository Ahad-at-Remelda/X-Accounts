"""File sink: append each new tweet as one JSON object per line (NDJSON)."""
from __future__ import annotations

import json

from ..parse import Tweet
from .base import Sink


class FileSink(Sink):
    def __init__(self, path: str) -> None:
        self._fh = open(path, "a", encoding="utf-8")

    async def emit(self, tweet: Tweet) -> None:
        self._fh.write(json.dumps(tweet.to_dict(), ensure_ascii=False) + "\n")
        self._fh.flush()

    async def close(self) -> None:
        self._fh.close()
