"""Client tests against the fake NodeBB (ticket #7). No live server."""

import asyncio

import httpx
import pytest
from aiohttp import web

from fake_nodebb import FakeNodeBB
from nodebb_client import NodeBBAuthError, NodeBBClient


async def _run(scenario):
    fake = FakeNodeBB()
    runner = web.AppRunner(fake.app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    try:
        await scenario(fake, base)
    finally:
        await runner.cleanup()


def run(scenario):
    asyncio.run(_run(scenario))


def test_login_ok_and_cookie_header():
    async def scenario(fake, base):
        client = NodeBBClient(base, "hermes", "test-pass", "test-token")
        await client.login()
        assert client.logged_in
        assert "sessionid=fake-session" in client.cookie_header()
        assert await client.my_uid() == 3
        await client.close()

    run(scenario)


def test_login_bad_password_raises():
    async def scenario(fake, base):
        client = NodeBBClient(base, "hermes", "wrong", "test-token")
        with pytest.raises(NodeBBAuthError):
            await client.login()
        await client.close()

    run(scenario)


def test_chats_messages_and_send():
    async def scenario(fake, base):
        client = NodeBBClient(base, "hermes", "test-pass", "test-token")
        await client.login()
        rooms = await client.get_chats()
        assert [r["roomId"] for r in rooms] == [10]
        messages = await client.get_chat_messages(10)
        assert [m["mid"] for m in messages] == [1, 2]
        result = await client.send_chat_message(10, "reply text")
        assert result["mid"] == 101
        assert fake.sent == [{"room": "10", "message": "reply text"}]
        await client.send_typing(10)  # must not raise
        await client.close()

    run(scenario)

def test_forum_reading_and_search():
    """Ticket #11: categories, topics, posts, and search via the fake."""
    async def scenario(fake, base):
        client = NodeBBClient(base, "hermes", "test-pass", "test-token")
        await client.login()
        cats = await client.get_categories()
        assert [c["cid"] for c in cats] == [5, 6]
        topics = await client.get_category_topics(5)
        assert [t["tid"] for t in topics] == [42, 43]
        topic = await client.get_topic(42)
        assert topic["title"] == "Hello thread"
        posts = await client.get_topic_posts(42)
        assert [p["pid"] for p in posts] == [1001, 1002]
        post = await client.get_post(1002)
        assert post["uid"] == 3 and "cats" in post["content"]
        results = await client.search("cats")
        assert [p["pid"] for p in results["posts"]] == [1002]
        assert results["topics"] == []
        results2 = await client.search("hello")
        assert [t["tid"] for t in results2["topics"]] == [42]
        await client.close()

    run(scenario)
