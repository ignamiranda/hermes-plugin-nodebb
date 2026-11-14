"""NodeBB Socket.IO listener for the Hermes NodeBB plugin.

Ticket #3. Connects with the session cookies from the REST client's
login (NodeBB sockets authenticate by session cookie; the Write API
token does not work for sockets) and forwards incoming chat messages to
a callback. The inbound pipeline (allowlist, sessions, handle_message)
is ticket #4 — this module's job is a reliable event stream:

- event name: event:chats.receive, emitted to the user's personal room
- payload (verified from NodeBB source/reports): {roomId, fromUid,
  message, self} — events with self == 1 are the bot's own messages and
  are dropped here
- reconnect replays are deduped by message id
- event:invalid_session triggers a fresh login and reconnect

The socket.io path under a NodeBB subpath install is
"<relative_path>/socket.io"; for a root install it is "socket.io".
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import socketio

logger = logging.getLogger(__name__)

CHATS_RECEIVE = "event:chats.receive"
INVALID_SESSION = "event:invalid_session"


class NodeBBSocketListener:
    def __init__(self, client, on_chat_message, on_reconnect=None):
        """client: NodeBBClient (already logged in).
        on_chat_message: async callable(room_id, from_uid, message_dict).
        message_dict is the payload's message object when present; when
        the payload carries only an id, the message is fetched via REST.
        on_reconnect: optional async callable run on every connect after
        the first (the adapter hooks its offline catch-up there).
        """
        self._client = client
        self._on_chat_message = on_chat_message
        self._on_reconnect = on_reconnect
        self._connect_count = 0
        self._sio = socketio.AsyncClient(reconnection=True, logger=False)
        self._seen_mids: set = set()
        self._register_handlers()

    def _socketio_path(self) -> str:
        path = urlparse(self._client.base_url).path.strip("/")
        return f"{path}/socket.io" if path else "socket.io"

    def _register_handlers(self) -> None:
        sio = self._sio

        @sio.on(CHATS_RECEIVE)
        async def _on_receive(data):
            await self._handle_receive(data or {})

        @sio.on(INVALID_SESSION)
        async def _on_invalid_session(*_args):
            logger.info("nodebb socket: invalid session, re-logging in")
            try:
                await self._client.login()
            except Exception as exc:
                logger.error("nodebb socket: re-login failed: %s", exc)

        @sio.on("connect")
        async def _on_connect():
            self._connect_count += 1
            logger.info("nodebb socket: connected (x%s)", self._connect_count)
            if self._connect_count > 1 and self._on_reconnect is not None:
                await self._on_reconnect()

        @sio.on("disconnect")
        async def _on_disconnect():
            logger.info("nodebb socket: disconnected")

    async def _handle_receive(self, data: dict) -> None:
        if data.get("self") in (1, "1", True):
            return  # the bot's own message, echoed back
        room_id = data.get("roomId")
        from_uid = data.get("fromUid")
        message = data.get("message")
        if isinstance(message, dict):
            mid = message.get("mid")
            if mid is not None:
                if mid in self._seen_mids:
                    return  # reconnect replay
                self._seen_mids.add(mid)
                if len(self._seen_mids) > 500:
                    self._seen_mids = {mid}
            await self._on_chat_message(room_id, from_uid, message)
        else:
            # Payload without the message body: fetch the room's latest
            # messages via REST and hand over the newest one.
            try:
                messages = await self._client.get_chat_messages(room_id, per_page=1)
            except Exception as exc:
                logger.error("nodebb socket: REST fetch for room %s failed: %s", room_id, exc)
                return
            if messages:
                await self._on_chat_message(room_id, from_uid, messages[-1])

    async def start(self) -> None:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(self._client.base_url))
        await self._sio.connect(
            origin,
            socketio_path=self._socketio_path(),
            headers={
                "Cookie": self._client.cookie_header(),
                "Origin": self._client.base_url,
            },
            transports=["websocket", "polling"],
        )

    async def stop(self) -> None:
        if self._sio.connected:
            await self._sio.disconnect()
