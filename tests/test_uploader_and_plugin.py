from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
import unittest

try:
    from . import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # noqa: F401
from aiohttp import web

from astrbot.api.message_components import File, Plain

from astrbot_plugin_download_center.main import DownloadCenterPlugin
from astrbot_plugin_download_center.uploader import STREAM_CHUNK_SIZE, upload_file_to_session


class _FakeBot:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call_action(self, action: str, **params):
        self.calls.append((action, params))
        if action == "upload_file_stream":
            if params.get("is_complete"):
                return {"file_path": "/app/.config/QQ/napcat/temp/uploaded.bin"}
            return {}
        return {"message_id": 42}


class _FakeAdapter:
    def __init__(self, bot: _FakeBot) -> None:
        self._bot = bot

    def get_client(self):
        return self._bot


class _FakeContext:
    def __init__(self, bot: _FakeBot | None = None) -> None:
        self.sent: list[tuple[str, list]] = []
        self.bot = bot or _FakeBot()
        self.adapter = _FakeAdapter(self.bot)

    async def send_message(self, session: str, message_chain) -> bool:
        self.sent.append((session, list(message_chain.chain)))
        return True

    def get_platform_inst(self, platform_id: str):
        return self.adapter


class _FakeEvent:
    def __init__(self, umo: str) -> None:
        self.unified_msg_origin = umo


class UploaderAndPluginTests(unittest.IsolatedAsyncioTestCase):
    async def test_tool_notifies_with_dl_hint_before_upload_and_returns_none(self) -> None:
        payload = b"fast-tool-payload-" * 5_000

        async def handle_file(request: web.Request) -> web.StreamResponse:
            if request.method == "HEAD":
                return web.Response(
                    status=200,
                    headers={
                        "Content-Length": str(len(payload)),
                        "Accept-Ranges": "bytes",
                        "ETag": '"fast-v1"',
                    },
                )
            return web.Response(status=200, body=payload)

        app = web.Application()
        app.router.add_route("*", "/fast.zip", handle_file)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        try:
            with tempfile.TemporaryDirectory() as tmp:
                ctx = _FakeContext()
                cfg = {
                    "enabled": True,
                    "max_file_size_mb": 300,
                    "cache_ttl_hours": 72,
                    "cache_max_total_mb": 1024,
                    "block_private_network": False,
                }
                plugin = DownloadCenterPlugin(ctx, cfg, data_dir=Path(tmp))
                await plugin.initialize()
                try:
                    ev = _FakeEvent("ai小猪:GroupMessage:530698338")
                    url = f"http://127.0.0.1:{port}/fast.zip"

                    res1 = await plugin.tool_download_and_send_file(ev, url)
                    self.assertIsNone(res1)
                    for _ in range(50):
                        if len(ctx.sent) >= 2 and not plugin._jobs_by_id:
                            break
                        await asyncio.sleep(0.05)

                    self.assertEqual(len(ctx.sent), 2)
                    self.assertIsInstance(ctx.sent[0][1][0], Plain)
                    notice1 = ctx.sent[0][1][0].text
                    self.assertIn("已开始后台下载", notice1)
                    self.assertIn("查进度：/dl list", notice1)
                    self.assertIn("取消：/dl cancel ", notice1)
                    self.assertIsInstance(ctx.sent[1][1][0], File)

                    # Cache hit: notice must still arrive before uploaded File, and tool returns None
                    res2 = await plugin.tool_download_and_send_file(ev, url)
                    self.assertIsNone(res2)
                    for _ in range(30):
                        if len(ctx.sent) >= 4:
                            break
                        await asyncio.sleep(0.05)

                    self.assertEqual(len(ctx.sent), 4)
                    self.assertIsInstance(ctx.sent[2][1][0], Plain)
                    self.assertIn("命中本地缓存", ctx.sent[2][1][0].text)
                    self.assertIsInstance(ctx.sent[3][1][0], File)

                    # Error: returns error text to LLM without sending a start notice
                    res3 = await plugin.tool_download_and_send_file(ev, "ftp://invalid.example/a.bin")
                    self.assertIsInstance(res3, str)
                    self.assertTrue(res3.startswith("无法开始下载："))
                    self.assertEqual(len(ctx.sent), 4)
                finally:
                    await plugin.terminate()
        finally:
            await runner.cleanup()

    async def test_shared_path_and_napcat_stream_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sample = Path(tmp) / "demo.bin"
            sample.write_bytes(b"X" * (STREAM_CHUNK_SIZE + 123))

            ctx_shared = _FakeContext()
            await upload_file_to_session(
                ctx_shared,
                "ai小猪:GroupMessage:530698338",
                sample,
                "demo.bin",
                upload_mode="shared_path",
            )
            self.assertEqual(len(ctx_shared.sent), 1)
            umo, chain = ctx_shared.sent[0]
            self.assertEqual(umo, "ai小猪:GroupMessage:530698338")
            self.assertIsInstance(chain[0], File)
            self.assertEqual(chain[0].name, "demo.bin")

            ctx_stream = _FakeContext()
            await upload_file_to_session(
                ctx_stream,
                "ai小猪:GroupMessage:530698338",
                sample,
                "demo.bin",
                upload_mode="napcat_stream",
            )
            calls = ctx_stream.bot.calls
            self.assertEqual([c[0] for c in calls], [
                "upload_file_stream",
                "upload_file_stream",
                "upload_file_stream",
                "send_group_msg",
            ])
            self.assertEqual(calls[0][1]["chunk_index"], 0)
            self.assertEqual(calls[0][1]["total_chunks"], 2)
            self.assertEqual(calls[1][1]["chunk_index"], 1)
            self.assertTrue(calls[2][1]["is_complete"])
            self.assertEqual(calls[3][1]["group_id"], 530698338)
            self.assertEqual(
                calls[3][1]["message"][0]["data"]["file"],
                "/app/.config/QQ/napcat/temp/uploaded.bin",
            )

    async def test_plugin_singleflight_and_cache_dedup(self) -> None:
        payload = b"hello-plugin-" * 20_000
        get_hits = 0

        async def handle_file(request: web.Request) -> web.StreamResponse:
            nonlocal get_hits
            if request.method == "HEAD":
                return web.Response(
                    status=200,
                    headers={"Content-Length": str(len(payload)), "Accept-Ranges": "bytes"},
                )
            get_hits += 1
            await asyncio.sleep(0.15)
            rng = request.headers.get("Range")
            if rng:
                start_s, end_s = rng.replace("bytes=", "").split("-", 1)
                start = int(start_s)
                end = min(int(end_s) if end_s else len(payload) - 1, len(payload) - 1)
                return web.Response(
                    status=206,
                    body=payload[start : end + 1],
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                        "Content-Length": str(end - start + 1),
                    },
                )
            return web.Response(status=200, body=payload)

        app = web.Application()
        app.router.add_route("*", "/bundle.zip", handle_file)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        try:
            with tempfile.TemporaryDirectory() as tmp:
                ctx = _FakeContext()
                cfg = {
                    "enabled": True,
                    "max_file_size_mb": 300,
                    "cache_ttl_hours": 72,
                    "cache_max_total_mb": 1024,
                    "connections_per_download": 2,
                    "chunk_size_mb": 1,
                    "block_private_network": False,
                }
                plugin = DownloadCenterPlugin(ctx, cfg, data_dir=Path(tmp))
                await plugin.initialize()
                try:
                    url = f"http://127.0.0.1:{port}/bundle.zip"
                    r1, r2, r3 = await asyncio.gather(
                        plugin.submit_download("ai小猪:GroupMessage:530698338", url),
                        plugin.submit_download("ai小猪:GroupMessage:530698338", url),
                        plugin.submit_download("ai小猪:FriendMessage:284517534", url),
                    )
                    started = [r for r in (r1, r2, r3) if "已开始后台下载" in r]
                    joined = [r for r in (r1, r2, r3) if "已在下载中" in r]
                    self.assertEqual(len(started), 1, msg=(r1, r2, r3))
                    self.assertEqual(len(joined), 2, msg=(r1, r2, r3))
                    for reply in (r1, r2, r3):
                        self.assertIn("查进度：/dl list", reply)
                        self.assertIn("取消：/dl cancel ", reply)

                    for _ in range(50):
                        if len(ctx.sent) >= 2 and not plugin._jobs_by_id:
                            break
                        await asyncio.sleep(0.05)

                    # Downloaded once; uploaded once per unique UMO (2 total)
                    self.assertEqual(get_hits, 1)
                    self.assertEqual(len(ctx.sent), 2)
                    sent_umos = {u for u, _ in ctx.sent}
                    self.assertEqual(
                        sent_umos,
                        {"ai小猪:GroupMessage:530698338", "ai小猪:FriendMessage:284517534"},
                    )

                    # Subsequent request hits cache with zero extra HTTP GETs
                    r4 = await plugin.submit_download("ai小猪:GroupMessage:530698338", url)
                    self.assertIn("命中本地缓存", r4)
                    for _ in range(30):
                        if len(ctx.sent) >= 3:
                            break
                        await asyncio.sleep(0.05)
                    self.assertEqual(get_hits, 1)
                    self.assertEqual(len(ctx.sent), 3)
                finally:
                    await plugin.terminate()
        finally:
            await runner.cleanup()
