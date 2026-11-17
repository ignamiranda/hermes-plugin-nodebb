"""NodeBB Socket.IO listener for the Hermes NodeBB plugin.

Ticket #3. Connects with the session cookies from the REST client's
login (NodeBB sockets authenticate by session cookie; the Write API
token does not work for sockets) and forwards incoming chat messages to
a callback. The inbound pipeline (allowlist, sessions, handle_message)
is ticket #4 — this module's job is a reliable event stream:

- event name: event:chats.receive. Delivery model verified against the
  deployed NodeBB v4.16.8 source (src/messaging/notifications.js):
  the event is emitted ONLY to sockets joined to `chat_room_<roomId>`.
  A socket joins by emitting `modules.chats.enter` with the room id(s)
  (src/socket.io/modules.js), so after every connect this listener
  enters all rooms the bot belongs to (via the room_provider callback)
- payload: {roomId, fromUid, message, public} — the bot's own messages
  are filtered in the adapter pipeline by sender uid
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
NEW_NOTIFICATION = "event:new_notification"


class NodeBBSocketListener:
    def __init__(self, client, on_chat_message, on_reconnect=None, room_provider=None, on_notification=None):
        """client: NodeBBClient (already logged in).
        on_chat_message: async callable(room_id, from_uid, message_dict).
        message_dict is the payload's message object when present; when
        the payload carries only an id, the message is fetched via REST.
        on_reconnect: optional async callable run on every connect after
        the first (the adapter hooks its offline catch-up there).
        room_provider: optional async callable returning the room ids
        the bot belongs to; used to join chat_room_<id> socket rooms.
        on_notification: optional async callable(notification_dict).
        Called when a notification event is received; the notification
        dict is unwrapped from the event payload.
        """
        self._client = client
        self._on_chat_message = on_chat_message
        self._on_reconnect = on_reconnect
        self._room_provider = room_provider
        self._on_notification = on_notification
        self._connect_count = 0
        self._sio = socketio.AsyncClient(reconnection=True, logger=False)
        self._seen_mids: set = set()
        self._seen_nids: set = set()
        self._bot_uid: int | None = None
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
            await self._enter_known_rooms()
            if self._connect_count > 1 and self._on_reconnect is not None:
                await self._on_reconnect()

        @sio.on("disconnect")
        async def _on_disconnect():
            logger.info("nodebb socket: disconnected")

        @sio.on(NEW_NOTIFICATION)
        async def _on_new_notification(data):
            await self._handle_notification(data or {})

    async def enter_rooms(self, room_ids) -> None:
        """Join chat_room_<id> socket rooms so event:chats.receive is
        delivered (NodeBB emits it only to joined sockets)."""
        ids = [int(r) for r in room_ids if str(r).isdigit()]
        if not ids:
            return
        # No self._sio.connected gate here: during the connect handler
        # the flag may not have flipped yet, which silently skipped the
        # join. A failed emit raises and is logged below instead.
        try:
            await self._sio.emit("modules.chats.enter", ids)
            logger.info("nodebb socket: entered chat rooms %s", ids)
        except Exception as exc:
            logger.error("nodebb socket: enter rooms failed: %s", exc)

    async def _enter_known_rooms(self) -> None:
        if self._room_provider is None:
            return
        try:
            room_ids = await self._room_provider()
        except Exception as exc:
            logger.error("nodebb socket: room list for enter failed: %s", exc)
            return
        await self.enter_rooms(room_ids)

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

    async def _get_bot_uid(self) -> int | None:
        """Lazily resolve and cache the bot's uid. Never let failures
        break the notification handler - if unknown, forward anyway."""
        if self._bot_uid is None:
            try:
                self._bot_uid = await self._client.my_uid()
            except Exception as exc:
                logger.debug("nodebb socket: could not resolve bot uid: %s", exc)
                self._bot_uid = 0  # Will never match any notification "from"
        return self._bot_uid

    async def _handle_notification(self, data: dict) -> None:
        """Handle incoming notification events.
        
        Payload format: {"notification": {nid, type, from, tid, pid, path, bodyShort, bodyLong, datetime}, "unreadCount": n}
        Or bare: {notification: {nid, ...}} or just {nid, ...}
        """
        if self._on_notification is None:
            return
        
        notification = data.get("notification")
        if notification is None:
            # Tolerate a bare notification dict
            notification = data if data else None
        
        if notification is None:
            return
        
        nid = notification.get("nid")
        if nid is not None:
            if nid in self._seen_nids:
                return  # Already seen, dedupe
            self._seen_nids.add(nid)
            if len(self._seen_nids) > 500:
                self._seen_nids = {nid}
        
        # Drop notifications from the bot's own uid
        from_uid = notification.get("from")
        bot_uid = await self._get_bot_uid()
        if bot_uid is not None and from_uid == bot_uid:
            return
        
        try:
            await self._on_notification(notification)
        except Exception as exc:
            logger.error("nodebb socket: notification handler failed: %s", exc)

    async def start(self) -> None:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(self._client.base_url))
        headers = {
            "Cookie": self._client.cookie_header(),
            "Origin": origin,
        }
        # NodeBB v4's socket allowRequest also runs a CSRF check on the
        # handshake (src/socket.io/index.js -> middleware/csrf
        # isRequestValid): without the session's csrf token in the
        # x-csrf-token header (or _csrf query) the handshake is 403.
        csrf = self._client.csrf_token  # property, not a method
        if csrf:
            headers["x-csrf-token"] = csrf
        await self._sio.connect(
            origin,
            socketio_path=self._socketio_path(),
            headers=headers,
            transports=["websocket", "polling"],
        )

    async def stop(self) -> None:
        if self._sio.connected:
            await self._sio.disconnect()"""NodeBB Socket.IO listener for the Hermes NodeBB plugin.

Ticket #3. Connects with the session cookies from the REST client's
login (NodeBB sockets authenticate by session cookie; the Write API
token does not work for sockets) and forwards incoming chat messages to
a callback. The inbound pipeline (allowlist, sessions, handle_message)
is ticket #4 — this module's job is a reliable event stream:

- event name: event:chats.receive. Delivery model verified against the
  deployed NodeBB v4.16.8 source (src/messaging/notifications.js):
  the event is emitted ONLY to sockets joined to `chat_room_<roomId>`.
  A socket joins by emitting `modules.chats.enter` with the room id(s)
  (src/socket.io/modules.js), so after every connect this listener
  enters all rooms the bot belongs to (via the room_provider callback)
- payload: {roomId, fromUid, message, public} — the bot's own messages
  are filtered in the adapter pipeline by sender uid
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
NEW_NOTIFICATION = "event:new_notification"


class NodeBBSocketListener:
    def __init__(self, client, on_chat_message, on_reconnect=None, room_provider=None, on_notification=None):
        """client: NodeBBClient (already logged in).
        on_chat_message: async callable(room_id, from_uid, message_dict).
        message_dict is the payload's message object when present; when
        the payload carries only an id, the message is fetched via REST.
        on_reconnect: optional async callable run on every connect after
        the first (the adapter hooks its offline catch-up there).
        room_provider: optional async callable returning the room ids
        the bot belongs to; used to join chat_room_<id> socket rooms.
        on_notification: optional async callable(notification_dict).
        Called when a notification event is received; the notification
        dict is unwrapped from the event payload.
        """
        self._client = client
        self._on_chat_message = on_chat_message
        self._on_reconnect = on_reconnect
        self._room_provider = room_provider
        self._on_notification = on_notification
        self._connect_count = 0
        self._sio = socketio.AsyncClient(reconnection=True, logger=False)
        self._seen_mids: set = set()
        self._seen_nids: set = set()
        self._bot_uid: int | None = None
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
            await self._enter_known_rooms()
            if self._connect_count > 1 and self._on_reconnect is not None:
                await self._on_reconnect()

        @sio.on("disconnect")
        async def _on_disconnect():
            logger.info("nodebb socket: disconnected")

        @sio.on(NEW_NOTIFICATION)
        async def _on_new_notification(data):
            await self._handle_notification(data or {})

    async def enter_rooms(self, room_ids) -> None:
        """Join chat_room_<id> socket rooms so event:chats.receive is
        delivered (NodeBB emits it only to joined sockets)."""
        ids = [int(r) for r in room_ids if str(r).isdigit()]
        if not ids:
            return
        # No self._sio.connected gate here: during the connect handler
        # the flag may not have flipped yet, which silently skipped the
        # join. A failed emit raises and is logged below instead.
        try:
            await self._sio.emit("modules.chats.enter", ids)
            logger.info("nodebb socket: entered chat rooms %s", ids)
        except Exception as exc:
            logger.error("nodebb socket: enter rooms failed: %s", exc)

    async def _enter_known_rooms(self) -> None:
        if self._room_provider is None:
            return
        try:
            room_ids = await self._room_provider()
        except Exception as exc:
            logger.error("nodebb socket: room list for enter failed: %s", exc)
            return
        await self.enter_rooms(room_ids)

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

    async def _get_bot_uid(self) -> int | None:
        """Lazily resolve and cache the bot's uid. Never let failures
        break the notification handler - if unknown, forward anyway."""
        if self._bot_uid is None:
            try:
                self._bot_uid = await self._client.my_uid()
            except Exception as exc:
                logger.debug("nodebb socket: could not resolve bot uid: %s", exc)
                self._bot_uid = 0  # Will never match any notification "from"
        return self._bot_uid

    async def _handle_notification(self, data: dict) -> None:
        """Handle incoming notification events.
        
        Payload format: {"notification": {nid, type, from, tid, pid, path, bodyShort, bodyLong, datetime}, "unreadCount": n}
        Or bare: {notification: {nid, ...}} or just {nid, ...}
        """
        if self._on_notification is None:
            return
        
        notification = data.get("notification")
        if notification is None:
            # Tolerate a bare notification dict
            notification = data if data else None
        
        if notification is None:
            return
        
        nid = notification.get("nid")
        if nid is not None:
            if nid in self._seen_nids:
                return  # Already seen, dedupe
            self._seen_nids.add(nid)
            if len(self._seen_nids) > 500:
                self._seen_nids = {nid}
        
        # Drop notifications from the bot's own uid
        from_uid = notification.get("from")
        bot_uid = await self._get_bot_uid()
        if bot_uid is not None and from_uid == bot_uid:
            return
        
        try:
            await self._on_notification(notification)
        except Exception as exc:
            logger.error("nodebb socket: notification handler failed: %s", exc)

    async def start(self) -> None:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(self._client.base_url))
        headers = {
            "Cookie": self._client.cookie_header(),
            "Origin": origin,
        }
        # NodeBB v4's socket allowRequest also runs a CSRF check on the
        # handshake (src/socket.io/index.js -> middleware/csrf
        # isRequestValid): without the session's csrf token in the
        # x-csrf-token header (or _csrf query) the handshake is 403.
        csrf = self._client.csrf_token  # property, not a method
        if csrf:
            headers["x-csrf-token"] = csrf
        await self._sio.connect(
            origin,
            socketio_path=self._socketio_path(),
            headers=headers,
            transports=["websocket", "polling"],
        )

    async def stop(self) -> None:
        if self._sio.connected:
            await self._sio.disconnect()
