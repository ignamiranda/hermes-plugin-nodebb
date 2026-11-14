"""Socket listener tests against the fake NodeBB (ticket #7)."""

import asyncio

import httpx
from aiohttp import web

from fake_nodebb import FakeNodeBB
from nodebb_client import NodeBBClient
from nodebb_socket import NodeBBSocketListener


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


def test_listener_receives_filters_self_and_dedupes():
    async def scenario(fake, base):
        client = NodeBBClient(base, "hermes", "test-pass", "test-token")
        await client.login()
        received = []

        async def on_message(room_id, from_uid, message):
            received.append((room_id, from_uid, message))

        listener = NodeBBSocketListener(client, on_message)
        await listener.start()
        try:
            async with httpx.AsyncClient(trust_env=False) as http:
                payload = {
                    "roomId": 10,
                    "fromUid": 1,
                    "self": 0,
                    "message": {"mid": 5, "content": "<p>hi</p>", "fromuid": 1},
                }
                await http.post(f"{base}/__test__/emit", json=payload)
                await asyncio.sleep(0.4)
                assert len(received) == 1
                assert received[0][0] == 10 and received[0][1] == 1

                # Reconnect replay of the same mid is dropped.
                await http.post(f"{base}/__test__/emit", json=payload)
                await asyncio.sleep(0.4)
                assert len(received) == 1

                # The bot's own message (self=1) is dropped.
                own = dict(payload, self=1)
                own["message"] = {"mid": 6, "content": "<p>mine</p>", "fromuid": 3}
                await http.post(f"{base}/__test__/emit", json=own)
                await asyncio.sleep(0.4)
                assert len(received) == 1
        finally:
            await listener.stop()
            await client.close()

    run(scenario)
