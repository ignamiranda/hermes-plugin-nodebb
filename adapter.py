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

import dataclasses
import logging

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult

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


async def standalone_sender(extra, chat_id, text):
    """Cron / send_message entry point before a live adapter exists.

    Ticket #1 skeleton: reports not-implemented honestly. The live send
    path lands with the REST client ticket.
    """
    return SendResult(success=False, error="nodebb: send not implemented yet")


class NodeBBAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nodebb"))
        self._base_url = (_extra(config, "base_url") or "").rstrip("/")

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # Ticket #1 skeleton: the real session login + socket connection
        # lands in the REST client / socket listener tickets. Connecting
        # here only marks the platform up so the gateway can list it.
        self._mark_connected()
        logger.info("nodebb adapter connected (skeleton, no transport yet)")
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

    async def send(self, chat_id, text, **kwargs) -> SendResult:
        return SendResult(success=False, error="nodebb: send not implemented yet")

    async def send_typing(self, chat_id) -> None:
        return None

    async def get_chat_info(self, chat_id) -> dict:
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
    # Feature-detect optional PlatformEntry fields so the plugin still
    # loads on older Hermes cores (register_platform API stable since
    # v0.12.0, but newer kwargs are not present in every version).
    try:
        from gateway.platforms.registry import PlatformEntry

        known = {f.name for f in dataclasses.fields(PlatformEntry)}
        kwargs = {k: v for k, v in kwargs.items() if k in known or k in ("name", "label", "adapter_factory")}
    except Exception:
        pass
    try:
        ctx.register_platform(standalone_sender_fn=standalone_sender, **kwargs)
    except TypeError:
        # Older core without standalone_sender_fn support: register without
        # it (cron delivery to this platform then reports no live adapter,
        # which is accurate for the skeleton).
        ctx.register_platform(**kwargs)
