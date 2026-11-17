"""A fake in-process NodeBB for tests (ticket #7).

Implements just the surface the plugin uses, with the real server's
shapes (verified live against NodeBB v4.16.8):

- GET  /api/config                      -> {csrf_token, uid}
- POST /api/v3/utilities/login          -> session cookie; CSRF header
  required (403 "Forbidden" without it); bad credentials return the
  plain-text body "[[error:invalid-login-credentials]]"
- GET  /api/v3/chats                    -> {response: {rooms: [...]}}
- GET  /api/v3/chats/<id>/messages      -> {response: {messages: [...]}}
- POST /api/v3/chats/<id>               -> {response: {mid, ...}}
- POST /api/v3/chats/<id>/typing        -> {response: {}}
- Socket.IO emitting event:chats.receive on demand.

Test controls:
- POST /__test__/emit  {roomId, fromUid, message, self}
- POST /__test__/emit_notification  {notification, unreadCount}
- GET  /__test__/sent  -> messages the plugin sent

Credentials: username "hermes", password "test-pass", token "test-token".
The bot's uid is 3; the other chat user is uid 1.
"""

from __future__ import annotations

import socketio
from aiohttp import web

CSRF = "test-csrf"
BOT_UID = 3
OWNER_UID = 1
ROOM_ID = 10

MESSAGES = [
    {
        "mid": 1,
        "fromuid": OWNER_UID,
        "content": "<p dir=\"auto\">hello from the owner</p>\n",
        "timestamp": 1700000000000,
        "system": 0,
        "deleted": 0,
        "fromUser": {"username": "owner"},
    },
    {
        "mid": 2,
        "fromuid": BOT_UID,
        "content": "<p dir=\"auto\">the bot's own earlier message</p>\n",
        "timestamp": 1700000001000,
        "system": 0,
        "deleted": 0,
        "fromUser": {"username": "hermes"},
    },
]


class FakeNodeBB:
    def __init__(self):
        self.sent: list = []
        self._next_mid = 100
        self.sio = socketio.AsyncServer(async_mode="aiohttp", cors_allowed_origins="*")
        self.app = web.Application()
        self.sio.attach(self.app, socketio_path="socket.io")
        r = self.app.router
        r.add_get("/api/config", self._config)
        r.add_post("/api/v3/utilities/login", self._login)
        r.add_get("/api/v3/chats", self._chats)
        r.add_get("/api/v3/chats/{room}/messages", self._messages)
        r.add_post("/api/v3/chats/{room}", self._send)
        r.add_post("/api/v3/chats/{room}/typing", self._typing)
        r.add_post("/__test__/emit", self._emit)
        r.add_post("/__test__/emit_notification", self._emit_notification)
        r.add_get("/__test__/sent", self._sent)

    async def _config(self, request):
        return web.json_response({"csrf_token": CSRF, "uid": BOT_UID})

    async def _login(self, request):
        if request.headers.get("x-csrf-token") != CSRF:
            return web.Response(status=403, text="Forbidden")
        body = await request.json()
        if body.get("username") == "hermes" and body.get("password") == "test-pass":
            resp = web.json_response({})
            resp.set_cookie("sessionid", "fake-session")
            return resp
        return web.Response(status=403, text="[[error:invalid-login-credentials]]")

    def _authed(self, request) -> bool:
        if request.headers.get("Authorization") == "Bearer test-token":
            return True
        return request.cookies.get("sessionid") == "fake-session"

    async def _chats(self, request):
        if not self._authed(request):
            return web.Response(status=403, text="Forbidden")
        room = {
            "roomId": ROOM_ID,
            "groupChat": False,
            "roomName": "",
            "users": [{"uid": OWNER_UID, "username": "owner"}],
        }
        return web.json_response({"response": {"rooms": [room]}})

    async def _messages(self, request):
        if not self._authed(request):
            return web.Response(status=403, text="Forbidden")
        return web.json_response({"response": {"messages": MESSAGES}})

    async def _send(self, request):
        if not self._authed(request):
            return web.Response(status=403, text="Forbidden")
        body = await request.json()
        self._next_mid += 1
        self.sent.append({"room": request.match_info["room"], "message": body.get("message")})
        return web.json_response(
            {"response": {"mid": self._next_mid, "content": body.get("message"), "fromuid": BOT_UID}}
        )

    async def _typing(self, request):
        if not self._authed(request):
            return web.Response(status=403, text="Forbidden")
        return web.json_response({"response": {}})

    async def _emit(self, request):
        payload = await request.json()
        await self.sio.emit("event:chats.receive", payload)
        return web.json_response({"emitted": True})

    async def _emit_notification(self, request):
        payload = await request.json()
        await self.sio.emit("event:new_notification", payload)
        return web.json_response({"emitted": True})

    async def _sent(self, request):
        return web.json_response({"sent": self.sent})
