"""Parse a UserTweets timeline response into Tweet objects.

The response is a tree of "instructions" -> "entries" -> "content". Each
top-level tweet entry carries a tweet_results.result we normalize. We also
skip pinned entries (they sort out of chronological order) and, per config,
retweets and replies.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("xmon.parse")


@dataclass
class Tweet:
    id: str
    handle: str
    author_id: str
    author_name: str
    text: str
    created_at: str
    url: str
    is_retweet: bool
    is_reply: bool
    is_quote: bool
    lang: str = ""
    media: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "handle": self.handle,
            "author_id": self.author_id,
            "author_name": self.author_name,
            "text": self.text,
            "created_at": self.created_at,
            "url": self.url,
            "is_retweet": self.is_retweet,
            "is_reply": self.is_reply,
            "is_quote": self.is_quote,
            "lang": self.lang,
            "media": self.media,
        }


def _iter_instructions(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Locate the timeline instruction list across known response shapes."""
    user = (data or {}).get("user", {}).get("result", {})
    # v2 timeline
    timeline = (
        user.get("timeline_v2", {}).get("timeline")
        or user.get("timeline", {}).get("timeline")
        or {}
    )
    return timeline.get("instructions", []) or []


def _entries(instructions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ins in instructions:
        itype = ins.get("type")
        if itype == "TimelineAddEntries":
            out.extend(ins.get("entries", []))
        elif itype == "TimelinePinEntry":
            # Keep track but mark; caller skips pinned.
            entry = ins.get("entry")
            if entry:
                entry = dict(entry)
                entry["__pinned__"] = True
                out.append(entry)
    return out


def _tweet_result(entry: dict[str, Any]) -> dict[str, Any] | None:
    content = entry.get("content", {})
    item = content.get("itemContent") or {}
    tr = item.get("tweet_results", {}).get("result")
    if not tr:
        return None
    # TweetWithVisibilityResults wraps the real tweet.
    if tr.get("__typename") == "TweetWithVisibilityResults":
        tr = tr.get("tweet", tr)
    return tr


def _extract_media(legacy: dict[str, Any]) -> list[str]:
    media_out: list[str] = []
    entities = legacy.get("extended_entities") or legacy.get("entities") or {}
    for m in entities.get("media", []) or []:
        url = m.get("media_url_https") or m.get("media_url")
        if url:
            media_out.append(url)
    return media_out


def _author(tr: dict[str, Any], legacy: dict[str, Any]) -> tuple[str, str]:
    core = tr.get("core", {}).get("user_results", {}).get("result", {})
    author_id = core.get("rest_id", "")
    ucore = core.get("core", {})
    ulegacy = core.get("legacy", {})
    name = ucore.get("screen_name") or ulegacy.get("screen_name") or ""
    return author_id, name


def _build_tweet(
    entry: dict[str, Any],
    default_handle: str,
    *,
    include_replies: bool,
    include_retweets: bool,
    handle_from_author: bool,
) -> Tweet | None:
    """Turn one 'tweet-*' entry into a Tweet, or None if filtered/invalid."""
    if entry.get("__pinned__"):
        return None
    tr = _tweet_result(entry)
    if not tr or tr.get("__typename") == "TweetTombstone":
        return None

    legacy = tr.get("legacy", {})
    tweet_id = tr.get("rest_id") or legacy.get("id_str")
    if not tweet_id:
        return None

    is_retweet = "retweeted_status_result" in legacy or legacy.get(
        "full_text", ""
    ).startswith("RT @")
    is_reply = bool(legacy.get("in_reply_to_status_id_str"))
    is_quote = bool(legacy.get("is_quote_status"))

    if is_retweet and not include_retweets:
        return None
    if is_reply and not include_replies:
        return None

    # note_tweet holds the full text of long-form (>280) posts.
    text = legacy.get("full_text", "")
    note = tr.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {})
    if note.get("text"):
        text = note["text"]

    author_id, author_name = _author(tr, legacy)
    author_name = author_name or default_handle
    # For list timelines each tweet is from a different member, so key the
    # Tweet on its own author; for a user timeline, keep the queried handle.
    handle = (author_name if handle_from_author else default_handle) or default_handle

    return Tweet(
        id=str(tweet_id),
        handle=handle,
        author_id=author_id,
        author_name=author_name,
        text=text,
        created_at=legacy.get("created_at", ""),
        url=f"https://x.com/{author_name or handle}/status/{tweet_id}",
        is_retweet=is_retweet,
        is_reply=is_reply,
        is_quote=is_quote,
        lang=legacy.get("lang", ""),
        media=_extract_media(legacy),
    )


def parse_timeline(
    data: dict[str, Any],
    handle: str,
    *,
    include_replies: bool,
    include_retweets: bool,
) -> list[Tweet]:
    tweets: list[Tweet] = []
    for entry in _entries(_iter_instructions(data)):
        if not entry.get("entryId", "").startswith("tweet-"):
            continue
        tw = _build_tweet(
            entry, handle,
            include_replies=include_replies,
            include_retweets=include_retweets,
            handle_from_author=False,
        )
        if tw:
            tweets.append(tw)
    tweets.sort(key=lambda t: int(t.id))  # oldest first
    return tweets


def _iter_list_instructions(data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    """Locate instructions under data.list.<key>.timeline for list responses."""
    lst = (data or {}).get("list", {}) or {}
    timeline = (lst.get(key, {}) or {}).get("timeline", {}) or {}
    return timeline.get("instructions", []) or []


def parse_list_timeline(
    data: dict[str, Any],
    *,
    include_replies: bool,
    include_retweets: bool,
) -> list[Tweet]:
    """Parse ListLatestTweetsTimeline: tweets from all members, mixed."""
    tweets: list[Tweet] = []
    for entry in _entries(_iter_list_instructions(data, "tweets_timeline")):
        if not entry.get("entryId", "").startswith("tweet-"):
            continue
        tw = _build_tweet(
            entry, "",
            include_replies=include_replies,
            include_retweets=include_retweets,
            handle_from_author=True,
        )
        if tw:
            tweets.append(tw)
    tweets.sort(key=lambda t: int(t.id))  # oldest first
    return tweets


def parse_list_members(data: dict[str, Any]) -> tuple[list[dict[str, str]], str | None]:
    """Parse ListMembers -> ([{rest_id, screen_name, name}], next_cursor)."""
    members: list[dict[str, str]] = []
    next_cursor: str | None = None
    for entry in _entries(_iter_list_instructions(data, "members_timeline")):
        entry_id = entry.get("entryId", "")
        content = entry.get("content", {}) or {}
        if entry_id.startswith("cursor-bottom") or content.get("cursorType") == "Bottom":
            next_cursor = content.get("value") or next_cursor
            continue
        if not entry_id.startswith("user-"):
            continue
        res = ((content.get("itemContent") or {}).get("user_results") or {}).get("result") or {}
        rest_id = res.get("rest_id")
        core = res.get("core") or {}
        legacy = res.get("legacy") or {}
        screen_name = core.get("screen_name") or legacy.get("screen_name")
        name = core.get("name") or legacy.get("name") or screen_name
        if rest_id and screen_name:
            members.append(
                {"rest_id": str(rest_id), "screen_name": screen_name, "name": name or screen_name}
            )
    return members, next_cursor
