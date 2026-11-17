"""NodeBB platform adapter for Hermes Agent.

Milestone 1, ticket #1: plugin skeleton and registration. This module makes
the platform installable and discoverable by the gateway. The NodeBB REST
client, socket listener, and message pipelines land in later tickets; the
methods below say so plainly instead of pretending to work.

Design notes (from the project's build research):
- NodeBB sockets authenticate with a session login (username/password ->
  cookies), not with the Write API token. The token is used for REST calls.
- NodeBB caps chat messages at 1,000 characters; the gateway chunks for us
  because registration declares max_message_length=1000.
- Secrets are read through the gateway's scoped-secret helpers, never raw
  os.environ.
"""

from __future__ import annotations

import logging

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 1000


def _extra(config: PlatformConfig, key: str, default=None):
    extra = getattr(config, "extra", None) or {}
    return extra.get(key, default)


def check_nodebb_requirements() -> bool:
    """Passive availability check. Never installs anything."""
    try:
        import httpx  # noqa: F401
        import socketio  # noqa: F401
    except Exception:
        return False
    return True


def validate_config(config: PlatformConfig) -> bool:
    return bool(
        _extra(config, "base_url")
        and _extra(config, "username")
        and _extra(config, "password")
    )


def env_enablement_fn():
    """Seed PlatformConfig.extra from NODEBB_* env vars, when present."""
    import os

    mapping = {
        "NODEBB_URL": "base_url",
        "NODEBB_USERNAME": "username",
        "NODEBB_PASSWORD": "password",
        "NODEBB_TOKEN": "token",
        "NODEBB_ALLOWED_USERS": "allowed_users",
        "NODEBB_HOME_CHANNEL": "home_channel",
        "NODEBB_WATCHED_CATEGORIES": "watched_categories",
    }
    extra = {dest: os.environ[src] for src, dest in mapping.items() if os.environ.get(src)}
    home = extra.get("home_channel")
    return (extra, home) if home else (extra, None)


def html_to_text(content: str) -> str:
    """NodeBB chat content is stored as parsed HTML (verified live:
    '<p dir="auto">text</p>'). Convert to plain text for the agent."""
    import html
    import re

    text = re.sub(r"<br\s*/?>", "\n", content or "")
    text = re.sub(r"</p>\s*<p[^>]*>", "\n\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def chunk_text(text: str, limit: int = MAX_MESSAGE_LENGTH) -> list:
    """Split text into <= limit chunks, preferring newline/space breaks."""
    chunks = []
    rest = text or ""
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit + 1)
        if cut < limit // 2:
            cut = rest.rfind(" ", 0, limit + 1)
        if cut < 1:
            cut = limit
        chunks.append(rest[:cut])
        rest = rest[cut:].lstrip("\n ")
    if rest:
        chunks.append(rest)
    return chunks


async def standalone_sender(extra, chat_id, text):
    """Cron / send_message entry point: build a short-lived client from
    the platform config extra, log in, and send (chunked at the NodeBB
    1,000-char cap)."""
    try:
        from .nodebb_client import NodeBBClient
    except ImportError:
        from nodebb_client import NodeBBClient

    extra = extra or {}
    client = NodeBBClient(
        base_url=(extra.get("base_url") or "").rstrip("/"),
        username=extra.get("username") or "",
        password=extra.get("password") or "",
        token=extra.get("token"),
    )
    try:
        await client.login()
        result = None
        for chunk in chunk_text(text):
            result = await client.send_chat_message(chat_id, chunk)
        mid = (result or {}).get("mid")
        return SendResult(success=True, message_id=str(mid) if mid is not None else None)
    except Exception as exc:
        return SendResult(success=False, error=f"nodebb: {exc}")
    finally:
        await client.close()


class NodeBBAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nodebb"))
        self._config = config
        self._base_url = (_extra(config, "base_url") or "").rstrip("/")
        self.client = None
        self.listener = None
        self._bot_uid = None
        self._pipeline_task = None
        self._refresh_task = None
        # Whether a catch-up state file already existed at startup.
        # Only a first-ever run seeds unknown rooms (install-time
        # backlog); afterwards, a room with no watermark replays from
        # the start so first messages in new rooms are not skipped.
        self._state_existed = self._state_path().exists()
        _state = self._load_state()
        # Ticket #10: per-topic poll watermarks live in the same state
        # file under a reserved key, apart from the room watermarks.
        self._topic_seen: dict = _state.pop("topics", {})
        self._topics_seeded: bool = bool(self._topic_seen)
        self._last_seen: dict = _state
        import asyncio

        self.inbound_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # Ticket #2: build the REST client and do the session login.
        # The socket listener joins in ticket #3.
        try:
            from .nodebb_client import NodeBBClient
        except ImportError:
            from nodebb_client import NodeBBClient

        self.client = NodeBBClient(
            base_url=self._base_url,
            username=_extra(self._config, "username") or "",
            password=_extra(self._config, "password") or "",
            token=_extra(self._config, "token"),
        )
        try:
            await self.client.login()
            self._bot_uid = await self.client.my_uid()
        except Exception as exc:
            logger.error("nodebb: login failed: %s", exc)
            return False
        # Ticket #3: start the socket listener. Inbound events land on
        # self.inbound_queue; the pipeline that turns them into Hermes
        # turns is ticket #4.
        try:
            from .nodebb_socket import NodeBBSocketListener
        except ImportError:
            from nodebb_socket import NodeBBSocketListener

        self.listener = NodeBBSocketListener(
            self.client,
            self._on_socket_chat,
            on_reconnect=self._on_socket_reconnect,
            room_provider=self._room_ids,
            on_notification=self._on_socket_notification,
        )
        try:
            await self.listener.start()
        except Exception as exc:
            logger.error("nodebb: socket connect failed: %s", exc)
            return False
        self._mark_connected()
        logger.info("nodebb adapter connected (login + socket ok)")
        import asyncio

        self._pipeline_task = asyncio.create_task(self._pipeline_loop())
        # Ticket #6: replay anything that arrived while we were down.
        asyncio.create_task(self._catchup())
        # Rooms created while connected are not joined automatically:
        # periodically re-enter all rooms and run the REST catch-up so
        # a first message in a brand-new room is still answered.
        self._refresh_task = asyncio.create_task(self._room_refresh_loop())
        return True

    async def _room_ids(self):
        if self.client is None:
            return []
        rooms = await self.client.get_chats()
        return [r.get("roomId") for r in rooms if r.get("roomId") is not None]

    async def _room_refresh_loop(self) -> None:
        import asyncio

        while True:
            # 30s discovery cadence for brand-new rooms (was 120s until
            # 2026-11-15 — a first message in a new room waited up to two
            # minutes for a reply; Ignacio asked for the faster cycle).
            await asyncio.sleep(30)
            try:
                if self.listener is not None:
                    await self.listener.enter_rooms(await self._room_ids())
                await self._catchup()
                await self._topic_catchup()
            except Exception as exc:
                logger.error("nodebb: room refresh failed: %s", exc)

    async def _on_socket_reconnect(self) -> None:
        import asyncio

        asyncio.create_task(self._catchup())

    # -- offline catch-up (ticket #6) -------------------------------------

    def _state_path(self):
        import os
        from pathlib import Path

        home = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
        return home / "nodebb_catchup.json"

    def _load_state(self) -> dict:
        import json

        try:
            return json.loads(self._state_path().read_text())
        except Exception:
            return {}

    def _save_state(self) -> None:
        import json

        try:
            path = self._state_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps({**self._last_seen, "topics": self._topic_seen})
            )
        except Exception as exc:
            logger.debug("nodebb: catch-up state save failed: %s", exc)

    async def _catchup(self) -> None:
        """Fetch messages newer than the per-room watermark and feed
        them into the inbound pipeline, oldest first. First run for a
        room only seeds the watermark — history is not replayed."""
        if self.client is None:
            return
        try:
            rooms = await self.client.get_chats()
        except Exception as exc:
            logger.error("nodebb: catch-up room list failed: %s", exc)
            return
        for room in rooms:
            room_id = str(room.get("roomId"))
            try:
                messages = await self.client.get_chat_messages(room_id, per_page=50)
            except Exception as exc:
                logger.error("nodebb: catch-up fetch room %s failed: %s", room_id, exc)
                continue
            if not messages:
                continue
            max_mid = max(int(m.get("mid", 0)) for m in messages)
            watermark = self._last_seen.get(room_id)
            if watermark is None and not self._state_existed:
                # First-ever run: seed only, do not replay history.
                self._last_seen[room_id] = max_mid
                continue
            if watermark is None:
                watermark = 0
            missed = [
                m
                for m in messages
                if int(m.get("mid", 0)) > watermark
                and not m.get("deleted")
                and str(m.get("fromuid")) != str(self._bot_uid)
            ]
            for m in sorted(missed, key=lambda x: int(x.get("mid", 0))):
                await self.inbound_queue.put((room_id, m.get("fromuid"), m))
            self._last_seen[room_id] = max_mid
        self._save_state()
        # State now exists on disk: any room discovered later is new,
        # not install-time backlog, and must replay from watermark 0.
        self._state_existed = True

    def _watched_categories(self) -> list:
        raw = _extra(self._config, "watched_categories") or []
        if isinstance(raw, str):
            raw = raw.replace(";", ",").split(",")
        categories = []
        for item in raw:
            try:
                categories.append(int(item))
            except (TypeError, ValueError):
                continue
        return categories

    async def _topic_catchup(self) -> None:
        """Polling fallback for thread wake (ticket #10).

        Sockets are the primary wake path; this catches watched-category
        posts the socket missed. Per-topic watermarks (postcount + pid)
        live in the catch-up state file; first sight of a topic seeds
        the watermark without replaying it. New posts are fed through
        the same notification path as socket events, so the allowlist
        gate and thread session keying apply identically.
        """
        if self.client is None:
            return
        categories = self._watched_categories()
        if not categories:
            return
        # Until the first pass completes, every topic is "first sight"
        # and seeds silently (no replay flood on install or upgrade).
        seed_only = not self._topics_seeded
        changed = False
        for cid in categories:
            try:
                topics = await self.client.get_category_topics(cid)
            except Exception as exc:
                logger.error(
                    "nodebb: topic catch-up list cid %s failed: %s", cid, exc
                )
                continue
            for topic in topics:
                tid = topic.get("tid")
                if tid is None:
                    continue
                key = str(tid)
                seen = self._topic_seen.get(key) or {}
                postcount = int(topic.get("postcount") or 0)
                if seen and postcount <= int(seen.get("count") or 0):
                    continue
                try:
                    posts = await self.client.get_topic_posts(tid)
                except Exception as exc:
                    logger.error(
                        "nodebb: topic catch-up fetch tid %s failed: %s",
                        tid,
                        exc,
                    )
                    continue
                if not posts:
                    continue
                max_pid = max(int(p.get("pid", 0)) for p in posts)
                if not seen:
                    # First sight: seed the watermark. During the seed
                    # pass nothing is emitted; afterwards, a newly seen
                    # topic is genuine new activity, so its latest post
                    # (only that one — never the whole history) turns.
                    self._topic_seen[key] = {
                        "count": postcount,
                        "pid": max_pid,
                    }
                    changed = True
                    if seed_only:
                        continue
                    latest = max(posts, key=lambda x: int(x.get("pid", 0)))
                    if not latest.get(
                        "deleted"
                    ) and str(latest.get("uid")) != str(self._bot_uid):
                        await self._on_socket_notification(
                            {
                                "nid": f"poll-{tid}-{latest.get('pid')}",
                                "type": "poll",
                                "from": latest.get("uid"),
                                "tid": tid,
                                "pid": latest.get("pid"),
                                "bodyLong": latest.get("content"),
                                "bodyShort": "",
                            }
                        )
                    continue
                watermark = int(seen.get("pid") or 0)
                missed = [
                    p
                    for p in posts
                    if int(p.get("pid", 0)) > watermark
                    and not p.get("deleted")
                    and str(p.get("uid")) != str(self._bot_uid)
                ]
                for p in sorted(missed, key=lambda x: int(x.get("pid", 0))):
                    await self._on_socket_notification(
                        {
                            "nid": f"poll-{tid}-{p.get('pid')}",
                            "type": "poll",
                            "from": p.get("uid"),
                            "tid": tid,
                            "pid": p.get("pid"),
                            "bodyLong": p.get("content"),
                            "bodyShort": "",
                        }
                    )
                self._topic_seen[key] = {"count": postcount, "pid": max_pid}
                changed = True
        self._topics_seeded = True
        if changed:
            self._save_state()

    async def _on_socket_chat(self, room_id, from_uid, message) -> None:
        logger.info("nodebb: chat event room=%s from_uid=%s", room_id, from_uid)
        await self.inbound_queue.put((room_id, from_uid, message))

    async def _on_socket_notification(self, notification) -> None:
        """Ticket #10: a forum notification (a mention, or activity in a
        watched category) becomes a turn in the thread's own session."""
        if not isinstance(notification, dict):
            return
        tid = notification.get("tid")
        if tid is None:
            return  # not thread activity (follows and the like are not turns)
        from_uid = notification.get("from")
        if str(from_uid) not in self._allowed_uids():
            logger.info("nodebb: dropped thread notification from non-allowlisted uid %s", from_uid)
            return
        body = notification.get("bodyLong") or ""
        text = html_to_text(body) if body else (notification.get("bodyShort") or "").strip()
        if not text:
            return
        logger.info(
            "nodebb: thread notification type=%s tid=%s pid=%s from_uid=%s",
            notification.get("type"), tid, notification.get("pid"), from_uid,
        )
        source = self.build_source(
            chat_id=f"topic:{tid}",
            chat_type="group",
            user_id=str(from_uid),
            user_name=str(from_uid),
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(notification.get("pid") or notification.get("nid") or ""),
        )
        await self.handle_message(event)

    # -- inbound pipeline (ticket #4) ------------------------------------

    def _allowed_uids(self) -> set:
        raw = _extra(self._config, "allowed_users") or ""
        if isinstance(raw, (list, tuple)):
            return {str(u).strip() for u in raw if str(u).strip()}
        return {u.strip() for u in str(raw).split(",") if u.strip()}

    async def _pipeline_loop(self) -> None:
        while True:
            room_id, from_uid, message = await self.inbound_queue.get()
            try:
                await self._process_inbound(room_id, from_uid, message)
            except Exception as exc:
                logger.error("nodebb: inbound processing failed: %s", exc)

    async def _process_inbound(self, room_id, from_uid, message) -> None:
        if not isinstance(message, dict):
            return
        # Advance the catch-up watermark for everything seen, including
        # messages the filters below drop — "seen" is not "answered".
        try:
            mid_int = int(message.get("mid", 0))
            if mid_int > int(self._last_seen.get(str(room_id), 0)):
                self._last_seen[str(room_id)] = mid_int
                self._save_state()
        except (TypeError, ValueError):
            pass
        if self._bot_uid is not None and str(from_uid) == str(self._bot_uid):
            return  # own message (the socket listener also self-filters)
        if message.get("system"):
            return  # join/leave and other system messages are not turns
        content = html_to_text(message.get("content") or "")
        if not content:
            return
        # Default-deny allowlist: with no allowlist configured, nobody is
        # answered. The deployment sets NODEBB_ALLOWED_USERS to the
        # owner's uid (the spec: Hermes answers only the owner's DMs).
        if str(from_uid) not in self._allowed_uids():
            logger.info("nodebb: dropped DM from non-allowlisted uid %s", from_uid)
            return
        from_user = message.get("fromUser") or {}
        user_name = from_user.get("username") or message.get("username") or str(from_uid)
        source = self.build_source(
            chat_id=str(room_id),
            chat_type="dm",
            user_id=str(from_uid),
            user_name=user_name,
        )
        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(message.get("mid", "")),
        )
        await self.handle_message(event)

    async def disconnect(self) -> None:
        if getattr(self, "_pipeline_task", None) is not None:
            self._pipeline_task.cancel()
            self._pipeline_task = None
        if getattr(self, "listener", None) is not None:
            await self.listener.stop()
            self.listener = None
        if self.client is not None:
            await self.client.close()
            self.client = None
        self._mark_disconnected()

    async def send(self, chat_id, content, **kwargs) -> SendResult:
        # The gateway core calls send(chat_id, content, reply_to=None,
        # metadata=None) — the second parameter MUST be named `content`;
        # naming it `text` made every reply fail with a TypeError.
        # The gateway chunks long replies at the registered
        # max_message_length (1,000); the client paces sends per room.
        if self.client is None:
            return SendResult(success=False, error="nodebb: not connected")
        if str(chat_id).startswith("topic:"):
            # Thread session (ticket #10): the reply posts into the
            # topic; reply_to names the post being quoted (ticket #12).
            tid = str(chat_id).split(":", 1)[1]
            try:
                result = await self.client.reply_to_topic(
                    tid, content, to_pid=kwargs.get("reply_to")
                )
            except Exception as exc:
                return SendResult(success=False, error=f"nodebb: {exc}")
            pid = (result or {}).get("pid")
            return SendResult(
                success=True, message_id=str(pid) if pid is not None else None
            )
        try:
            result = await self.client.send_chat_message(chat_id, content)
        except Exception as exc:
            return SendResult(success=False, error=f"nodebb: {exc}")
        mid = (result or {}).get("mid")
        return SendResult(success=True, message_id=str(mid) if mid is not None else None)

    async def send_typing(self, chat_id) -> None:
        # Best-effort: the typing endpoint's live behaviour is unverified,
        # so failures are swallowed — a reply must never fail because a
        # typing indicator did.
        if self.client is None:
            return None
        try:
            await self.client.send_typing(chat_id)
        except Exception as exc:
            logger.debug("nodebb: typing failed for room %s: %s", chat_id, exc)
        return None

    async def get_chat_info(self, chat_id) -> dict:
        if str(chat_id).startswith("topic:"):
            return {
                "name": f"topic {str(chat_id).split(':', 1)[1]}",
                "type": "group",
                "chat_id": str(chat_id),
            }
        if self.client is not None:
            try:
                for room in await self.client.get_chats():
                    if str(room.get("roomId")) == str(chat_id):
                        return {
                            "name": room.get("roomName") or str(chat_id),
                            "type": "group" if room.get("groupChat") else "dm",
                            "chat_id": str(chat_id),
                        }
            except Exception as exc:
                logger.debug("nodebb: get_chat_info lookup failed: %s", exc)
        return {"name": str(chat_id), "type": "dm", "chat_id": str(chat_id)}


def register(ctx) -> None:
    kwargs = dict(
        name="nodebb",
        label="NodeBB",
        adapter_factory=lambda config: NodeBBAdapter(config),
        check_fn=check_nodebb_requirements,
        validate_config=validate_config,
        required_env=["NODEBB_URL", "NODEBB_USERNAME", "NODEBB_PASSWORD"],
        install_hint="Install with: hermes plugins install ignamiranda/hermes-plugin-nodebb --enable",
        env_enablement_fn=env_enablement_fn,
        cron_deliver_env_var="NODEBB_HOME_CHANNEL",
        allowed_users_env="NODEBB_ALLOWED_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint="NodeBB forum chat. Messages are capped at 1,000 characters and arrive as individual chat messages.",
        emoji="🌀",
    )
    # Older Hermes cores may not accept every kwarg (the register_platform
    # API is stable since v0.12.0, but newer kwargs are not present in
    # every version), so registration falls back gracefully below.
    try:
        ctx.register_platform(standalone_sender_fn=standalone_sender, **kwargs)
    except TypeError:
        # Older core without standalone_sender_fn support: register without
        # it (cron delivery to this platform then reports no live adapter,
        # which is accurate for the skeleton).
        ctx.register_platform(**kwargs)
