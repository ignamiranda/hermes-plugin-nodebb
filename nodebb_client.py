"""NodeBB REST client + session login for the Hermes NodeBB plugin.

Verified live against NodeBB v4.16.8 (2026-11-14):
- GET /api/config (with a cookie jar) returns a csrf_token.
- POST /api/v3/utilities/login with the x-csrf-token header and the
  session cookies performs the username/password login; a bad password
  returns the plain-text body "[[error:invalid-login-credentials]]".
  A POST without the CSRF header is rejected with "Forbidden".
- The Write API user token authenticates REST calls (Bearer header),
  e.g. GET /api/v3/chats returns {"response": {"rooms": [...]}}.
- The session (cookies + CSRF) is what the Socket.IO connection needs;
  the token alone does not authenticate sockets.

Chat sends are paced at >= 2 seconds per room (NodeBB's chatMessageDelay
default is 2000 ms).
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

logger = logging.getLogger(__name__)

SEND_PACING_SECONDS = 2.0


class NodeBBError(Exception):
    pass


class NodeBBAuthError(NodeBBError):
    pass


class NodeBBClient:
    def __init__(self, base_url: str, username: str, password: str, token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self._password = password
        self._token = token
        # trust_env=False: the gateway host's proxy env vars must not
        # reroute (or break) requests to the forum.
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=30.0, trust_env=False)
        self._csrf: str | None = None
        self._logged_in = False
        self._last_send_by_room: dict[str, float] = {}

    # -- session ---------------------------------------------------------

    async def login(self) -> None:
        """Session login: config (CSRF + cookies) -> utilities/login."""
        resp = await self._http.get("/api/config")
        resp.raise_for_status()
        self._csrf = resp.json().get("csrf_token")
        if not self._csrf:
            raise NodeBBAuthError("nodebb: /api/config returned no csrf_token")
        resp = await self._http.post(
            "/api/v3/utilities/login",
            json={"username": self.username, "password": self._password},
            headers={"x-csrf-token": self._csrf},
        )
        body = resp.text.strip()
        if resp.status_code >= 400 or body.startswith("[[error"):
            raise NodeBBAuthError(f"nodebb: login failed ({resp.status_code}): {body[:120]}")
        self._logged_in = True
        logger.info("nodebb: session login ok for %s", self.username)

    async def my_uid(self):
        """The logged-in account's uid. Verified live: GET /api/config
        includes a top-level 'uid' for the authenticated user."""
        resp = await self._http.get("/api/config", headers=self._headers())
        resp.raise_for_status()
        return resp.json().get("uid")

    @property
    def logged_in(self) -> bool:
        return self._logged_in

    @property
    def csrf_token(self) -> str | None:
        return self._csrf

    def cookie_header(self) -> str:
        """Cookie header value for the Socket.IO handshake (ticket #3)."""
        return "; ".join(f"{c.name}={c.value}" for c in self._http.cookies.jar)

    # -- REST ------------------------------------------------------------

    def _headers(self) -> dict:
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if self._csrf:
            headers["x-csrf-token"] = self._csrf
        return headers

    async def request(self, method: str, path: str, **kwargs) -> dict:
        resp = await self._http.request(method, path, headers=self._headers(), **kwargs)
        if resp.status_code in (401, 403) and self._logged_in:
            # Session may have expired; re-login once and retry.
            logger.info("nodebb: %s on %s, re-logging in", resp.status_code, path)
            self._logged_in = False
            await self.login()
            resp = await self._http.request(method, path, headers=self._headers(), **kwargs)
        if resp.status_code >= 400:
            raise NodeBBError(f"nodebb: {method} {path} -> {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        return data.get("response", data)

    async def get_chats(self) -> list:
        data = await self.request("GET", "/api/v3/chats")
        return data.get("rooms", []) if isinstance(data, dict) else []

    async def get_chat_messages(self, room_id, per_page: int = 50) -> list:
        data = await self.request(
            "GET", f"/api/v3/chats/{room_id}/messages", params={"perPage": per_page}
        )
        return data.get("messages", []) if isinstance(data, dict) else []

    # -- forum reading (ticket #11) -------------------------------------
    # Shapes verified against the NodeBB v4.16.8 OpenAPI specs: the v3
    # routes wrap payloads in {"response": ...} (handled by request());
    # the Read API routes (/api/topic, /api/search) return the bare
    # object, which request() also tolerates.

    async def get_categories(self) -> list:
        data = await self.request("GET", "/api/v3/categories")
        return data.get("categories", []) if isinstance(data, dict) else []

    async def get_category_topics(self, cid, after: int = 0) -> list:
        data = await self.request(
            "GET", f"/api/v3/categories/{cid}/topics", params={"after": after}
        )
        return data.get("topics", []) if isinstance(data, dict) else []

    async def get_topic(self, tid) -> dict:
        data = await self.request("GET", f"/api/v3/topics/{tid}")
        return data if isinstance(data, dict) else {}

    async def get_topic_posts(self, tid) -> list:
        # No v3 endpoint lists a topic's posts; the Read API topic route
        # returns the topic object with its posts embedded.
        data = await self.request("GET", f"/api/topic/{tid}")
        return data.get("posts", []) if isinstance(data, dict) else []

    async def get_post(self, pid) -> dict:
        data = await self.request("GET", f"/api/v3/posts/{pid}")
        return data if isinstance(data, dict) else {}

    async def search(self, query: str) -> dict:
        # Search exists only in the Read API (session or Bearer auth).
        data = await self.request("GET", "/api/search", params={"query": query})
        return data if isinstance(data, dict) else {}

    async def send_chat_message(self, room_id, message: str) -> dict:
        await self._pace(room_id)
        return await self.request(
            "POST", f"/api/v3/chats/{room_id}", json={"message": message}
        )

    async def create_chat(self, uids: list[int]) -> dict:
        return await self.request("POST", "/api/v3/chats", json={"uids": uids})

    async def send_typing(self, room_id) -> None:
        """Typing indicator. Path from the NodeBB OpenAPI surface; the
        live behaviour is verified in ticket #5's work."""
        await self.request("POST", f"/api/v3/chats/{room_id}/typing")

    async def _pace(self, room_id) -> None:
        now = time.monotonic()
        wait = SEND_PACING_SECONDS - (now - self._last_send_by_room.get(str(room_id), 0.0))
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_send_by_room[str(room_id)] = time.monotonic()

    async def close(self) -> None:
        await self._http.aclose()
        self._logged_in = False
