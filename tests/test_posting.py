"""Ticket #12: posting in threads - reply, quote, create, edit, delete."""

import asyncio

from aiohttp import web

from fake_nodebb import FakeNodeBB
from nodebb_client import NodeBBClient


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


def make_client(base):
    return NodeBBClient(base, "hermes", "test-pass", "test-token")


def test_reply_and_quote():
    async def scenario(fake, base):
        client = make_client(base)
        await client.login()
        reply = await client.reply_to_topic(42, "a brand new reply")
        assert reply["uid"] == 3 and reply["tid"] == 42
        assert reply["content"] == "a brand new reply"
        posts = await client.get_topic_posts(42)
        assert reply["pid"] in [p["pid"] for p in posts]
        quote = await client.reply_to_topic(42, "quoting you", to_pid=1001)
        assert quote["toPid"] == 1001
        await client.close()

    run(scenario)


def test_create_topic():
    async def scenario(fake, base):
        client = make_client(base)
        await client.login()
        created = await client.create_topic(5, "Fresh thread", "opening post body")
        assert created["title"] == "Fresh thread" and created["cid"] == 5
        assert created["mainPost"]["content"] == "opening post body"
        topics = await client.get_category_topics(5)
        assert created["tid"] in [t["tid"] for t in topics]
        await client.close()

    run(scenario)


def test_edit_delete_restore_own_post():
    async def scenario(fake, base):
        client = make_client(base)
        await client.login()
        reply = await client.reply_to_topic(42, "before edit")
        pid = reply["pid"]
        edited = await client.edit_post(pid, "after edit")
        assert edited["content"] == "after edit"
        got = await client.get_post(pid)
        assert got["content"] == "after edit" and got.get("edited")
        await client.delete_post(pid)
        got = await client.get_post(pid)
        assert got["deleted"] == 1
        await client.restore_post(pid)
        got = await client.get_post(pid)
        assert got["deleted"] == 0
        await client.close()

    run(scenario)