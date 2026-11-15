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
        self._last_seen: dict = self._load_state()
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
            await asyncio.sleep(120)
            try:
                if self.listener is not None:
                    await self.listener.enter_rooms(await self._room_ids())
                await self._catchup()
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
            path.write_text(json.dumps(self._last_seen))
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

    async def _on_socket_chat(self, room_id, from_uid, message) -> None:
        logger.info("nodebb: chat event room=%s from_uid=%s", room_id, from_uid)
        await self.inbound_queue.put((room_id, from_uid, message))

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
