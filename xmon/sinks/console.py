"""Console sink: print each new tweet to stdout."""
from __future__ import annotations

from ..parse import Tweet
from .base import Sink


class ConsoleSink(Sink):
    async def emit(self, tweet: Tweet) -> None:
        tags = []
        if tweet.is_retweet:
            tags.append("RT")
        if tweet.is_reply:
            tags.append("reply")
        if tweet.is_quote:
            tags.append("quote")
        tag = f" [{','.join(tags)}]" if tags else ""
        text = " ".join(tweet.text.split())
        if len(text) > 240:
            text = text[:237] + "..."
        print(
            f"@{tweet.handle}{tag}  {tweet.created_at}\n"
            f"  {text}\n"
            f"  {tweet.url}",
            flush=True,
        )
