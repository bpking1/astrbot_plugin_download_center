from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

try:
    from . import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # noqa: F401

from aiohttp import web

from astrbot.api.message_components import File

from astrbot_plugin_download_center.baidu_pan import (
    BaiduPanError,
    extract_baidu_share_info,
    is_baidu_pan_url,
    parse_pick_indices,
)
from astrbot_plugin_download_center.main import DownloadCenterPlugin


class _FakeContext:
    def __init__(self) -> None:
        self.sent: list[tuple[str, list]] = []

    async def send_message(self, session: str, message_chain) -> bool:
        self.sent.append((session, list(message_chain.chain)))
        return True


class _MockBaiduAndOpenListServer:
    def __init__(self) -> None:
        self.app = web.Application()
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.port: int = 0

        self.files_by_surl: dict[str, list[dict]] = {}
        self.sub_lists: dict[str, list[dict]] = {}
        self.payloads: dict[int, bytes] = {}
        self.transfer_calls: list[dict] = []
        self.openlist_mkdirs: list[str] = []
        self.openlist_removes: list[tuple[str, list[str]]] = []
        self.openlist_dirs: dict[str, list[dict]] = {}
        self.verify_errno: int = 0
        self.transfer_errno: int = 0
        self.require_pwd_for: set[str] = set()
        self.seen_cdn_uas: list[str] = []

        self.app.router.add_post("/share/verify", self._handle_verify)
        self.app.router.add_get("/s/{surl}", self._handle_share_page)
        self.app.router.add_get("/api/gettemplatevariable", self._handle_tv)
        self.app.router.add_get("/share/list", self._handle_share_list)
        self.app.router.add_post("/share/transfer", self._handle_transfer)
        self.app.router.add_post("/api/fs/mkdir", self._handle_ol_mkdir)
        self.app.router.add_post("/api/fs/list", self._handle_ol_list)
        self.app.router.add_post("/api/fs/get", self._handle_ol_get)
        self.app.router.add_post("/api/fs/remove", self._handle_ol_remove)
        self.app.router.add_route("*", "/cdn/{fs_id}", self._handle_cdn)

    async def __aenter__(self) -> _MockBaiduAndOpenListServer:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        assert self.site._server is not None
        self.port = self.site._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args) -> None:
        if self.runner is not None:
            await self.runner.cleanup()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _handle_verify(self, request: web.Request) -> web.Response:
        if self.verify_errno != 0:
            return web.json_response({"errno": self.verify_errno})
        post = await request.post()
        if post.get("pwd") != "1234":
            return web.json_response({"errno": -9})
        return web.json_response({"errno": 0, "randsk": "MOCK_RANDSK"})

    async def _handle_share_page(self, request: web.Request) -> web.Response:
        surl = request.match_info["surl"]
        cookie = request.headers.get("Cookie", "")
        if surl in self.require_pwd_for and "BDCLND=MOCK_RANDSK" not in cookie:
            raise web.HTTPFound(location=f"/share/init?surl={surl[1:]}")
        html = '<script>locals.mset({"shareid":987654,"uk":112233,"bdstoken":"mock_bds_token"});</script>'
        return web.Response(status=200, text=html, content_type="text/html")

    async def _handle_tv(self, request: web.Request) -> web.Response:
        return web.json_response({"errno": 0, "result": {"bdstoken": "mock_bds_token"}})

    async def _handle_share_list(self, request: web.Request) -> web.Response:
        shorturl = request.query.get("shorturl", "")
        dir_param = request.query.get("dir", "")
        if dir_param:
            items = self.sub_lists.get(dir_param, [])
        else:
            items = self.files_by_surl.get(f"1{shorturl}", [])
        return web.json_response({"errno": 0, "list": items})

    async def _handle_transfer(self, request: web.Request) -> web.Response:
        post = await request.post()
        fsidlist = json.loads(post.get("fsidlist", "[]"))
        dest_path = post.get("path", "")
        self.transfer_calls.append({"fsidlist": fsidlist, "path": dest_path})
        if self.transfer_errno != 0:
            return web.json_response({"errno": self.transfer_errno})

        fs_id = int(fsidlist[0])
        name = "unknown.bin"
        for items in list(self.files_by_surl.values()) + list(self.sub_lists.values()):
            for it in items:
                if int(it["fs_id"]) == fs_id:
                    name = it["server_filename"]
                    break
        ol_dir = f"/baidu{dest_path}"
        self.openlist_dirs[ol_dir] = [
            {"name": name, "size": len(self.payloads.get(fs_id, b"")), "is_dir": False}
        ]
        return web.json_response({"errno": 0})

    async def _handle_ol_mkdir(self, request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != "ol-secret":
            return web.json_response({"code": 401, "message": "unauthorized"})
        body = await request.json()
        path = body.get("path", "")
        self.openlist_mkdirs.append(path)
        self.openlist_dirs.setdefault(path, [])
        return web.json_response({"code": 200, "message": "success"})

    async def _handle_ol_list(self, request: web.Request) -> web.Response:
        body = await request.json()
        path = body.get("path", "")
        return web.json_response(
            {"code": 200, "message": "success", "data": {"content": self.openlist_dirs.get(path, [])}}
        )

    async def _handle_ol_get(self, request: web.Request) -> web.Response:
        body = await request.json()
        path = body.get("path", "")
        dir_p, _, name = path.rpartition("/")
        for it in self.openlist_dirs.get(dir_p, []):
            if it["name"] == name:
                for fs_id, data in self.payloads.items():
                    if len(data) == it["size"]:
                        return web.json_response(
                            {
                                "code": 200,
                                "message": "success",
                                "data": {
                                    "name": name,
                                    "size": len(data),
                                    "is_dir": False,
                                    "raw_url": f"{self.base_url}/cdn/{fs_id}",
                                    "header": {"User-Agent": ["pan.baidu.com"]},
                                },
                            }
                        )
        return web.json_response({"code": 500, "message": "object not found"})

    async def _handle_ol_remove(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.openlist_removes.append((body.get("dir", ""), body.get("names", [])))
        return web.json_response({"code": 200, "message": "success"})

    async def _handle_cdn(self, request: web.Request) -> web.StreamResponse:
        ua = request.headers.get("User-Agent", "")
        self.seen_cdn_uas.append(ua)
        if ua != "pan.baidu.com":
            return web.Response(status=403, text="bad user-agent")
        fs_id = int(request.match_info["fs_id"])
        data = self.payloads[fs_id]
        if request.method == "HEAD":
            return web.Response(
                status=200,
                headers={"Content-Length": str(len(data)), "Accept-Ranges": "bytes"},
            )
        rng = request.headers.get("Range")
        if rng:
            s_str, e_str = rng.replace("bytes=", "").split("-", 1)
            start = int(s_str)
            end = min(int(e_str) if e_str else len(data) - 1, len(data) - 1)
            return web.Response(
                status=206,
                body=data[start : end + 1],
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(data)}",
                    "Content-Length": str(end - start + 1),
                },
            )
        return web.Response(status=200, body=data)


class BaiduPanTests(unittest.IsolatedAsyncioTestCase):
    def test_url_and_pick_parsers(self) -> None:
        text = "链接: https://pan.baidu.com/s/1abcDEF234?pwd=ab12 提取码: ab12"
        self.assertTrue(is_baidu_pan_url(text))
        full_surl, short_surl, pwd = extract_baidu_share_info(text)  # type: ignore[misc]
        self.assertEqual(full_surl, "1abcDEF234")
        self.assertEqual(short_surl, "abcDEF234")
        self.assertEqual(pwd, "ab12")

        text2 = "https://pan.baidu.com/share/init?surl=xyz999 提取码：Q9w8"
        full2, short2, pwd2 = extract_baidu_share_info(text2)  # type: ignore[misc]
        self.assertEqual(full2, "1xyz999")
        self.assertEqual(short2, "xyz999")
        self.assertEqual(pwd2, "Q9w8")

        self.assertEqual(parse_pick_indices("1, 3-4", max_index=5, max_pick=3), [1, 3, 4])
        with self.assertRaises(BaiduPanError):
            parse_pick_indices("1-4", max_index=5, max_pick=3)

    async def test_single_file_baidu_flow_and_cache_dedup(self) -> None:
        async with _MockBaiduAndOpenListServer() as srv:
            payload = b"BAIDU-SVIP-DATA-" * 10_000  # ~160 KB
            srv.payloads[1001] = payload
            srv.require_pwd_for.add("1singleDemo")
            srv.files_by_surl["1singleDemo"] = [
                {"fs_id": 1001, "server_filename": "tool.zip", "isdir": 0, "size": len(payload), "md5": "abc"}
            ]

            with tempfile.TemporaryDirectory() as tmp:
                ctx = _FakeContext()
                cfg = {
                    "enabled": True,
                    "max_file_size_mb": 300,
                    "cache_ttl_hours": 72,
                    "block_private_network": False,
                    "baidu_pan": {
                        "enabled": True,
                        "cookie": "BDUSS=demo; STOKEN=demo",
                        "openlist_url": srv.base_url,
                        "openlist_token": "ol-secret",
                        "openlist_mount_path": "/baidu",
                        "temp_dir": "/bot_tmp",
                        "connections": 2,
                        "chunk_size_mb": 1,
                    },
                }
                plugin = DownloadCenterPlugin(ctx, cfg, data_dir=Path(tmp), baidu_api_base=srv.base_url)
                await plugin.initialize()
                try:
                    umo = "ai小猪:GroupMessage:530698338"
                    reply = await plugin.submit_download(
                        umo,
                        f"https://pan.baidu.com/s/1singleDemo 提取码: 1234",
                    )
                    self.assertIn("已开始后台下载百度网盘「tool.zip」", reply)
                    self.assertIn("查进度：/dl list", reply)

                    for _ in range(50):
                        if len(ctx.sent) >= 1 and not plugin._jobs_by_id:
                            break
                        await asyncio.sleep(0.05)

                    self.assertEqual(len(ctx.sent), 1)
                    self.assertIsInstance(ctx.sent[0][1][0], File)
                    self.assertEqual(ctx.sent[0][1][0].name, "tool.zip")
                    self.assertEqual(Path(ctx.sent[0][1][0].file_).read_bytes(), payload)
                    self.assertTrue(all(ua == "pan.baidu.com" for ua in srv.seen_cdn_uas))
                    self.assertEqual(len(srv.transfer_calls), 1)
                    self.assertEqual(len(srv.openlist_removes), 1)
                    self.assertEqual(srv.openlist_removes[0][0], "/baidu/bot_tmp")

                    # Second request hits canonical baidu:// cache without transfer or OpenList
                    reply2 = await plugin.submit_download(
                        "ai小猪:FriendMessage:284517534",
                        "https://pan.baidu.com/s/1singleDemo?pwd=1234",
                    )
                    self.assertIn("命中本地缓存", reply2)
                    for _ in range(30):
                        if len(ctx.sent) >= 2:
                            break
                        await asyncio.sleep(0.05)
                    self.assertEqual(len(srv.transfer_calls), 1)
                    self.assertEqual(len(ctx.sent), 2)
                finally:
                    await plugin.terminate()

    async def test_multi_file_list_pick_and_error_cleanup(self) -> None:
        async with _MockBaiduAndOpenListServer() as srv:
            small_data = b"PATCH-OK-" * 4000  # 36 KB
            big_size = 500 * 1024 * 1024  # 500 MB (> 300 MB limit)
            srv.payloads[2001] = small_data
            srv.files_by_surl["1multiDemo"] = [
                {"fs_id": 2000, "server_filename": "docs", "isdir": 1, "path": "/docs", "size": 0},
                {"fs_id": 2002, "server_filename": "big.iso", "isdir": 0, "size": big_size},
            ]
            srv.sub_lists["/docs"] = [
                {"fs_id": 2001, "server_filename": "patch.zip", "isdir": 0, "size": len(small_data)},
            ]

            with tempfile.TemporaryDirectory() as tmp:
                ctx = _FakeContext()
                cfg = {
                    "enabled": True,
                    "max_file_size_mb": 300,
                    "block_private_network": False,
                    "baidu_pan": {
                        "enabled": True,
                        "cookie": "BDUSS=demo; STOKEN=demo",
                        "openlist_url": srv.base_url,
                        "openlist_token": "ol-secret",
                        "max_pick_files": 2,
                    },
                }
                plugin = DownloadCenterPlugin(ctx, cfg, data_dir=Path(tmp), baidu_api_base=srv.base_url)
                await plugin.initialize()
                try:
                    umo = "ai小猪:GroupMessage:530698338"
                    list_reply = await plugin.submit_download(umo, "https://pan.baidu.com/s/1multiDemo")
                    self.assertIn("清单编号", list_reply)
                    self.assertIn("1. docs/patch.zip", list_reply)
                    self.assertIn("2. big.iso (500.00 MB) ❌超限", list_reply)

                    # Picking oversized + valid file: oversized rejected inline, valid starts
                    pick_reply = await plugin.pick_baidu_files(umo, "1 2")
                    self.assertIn("已开始后台下载百度网盘「patch.zip」", pick_reply)
                    self.assertIn("超过上限", pick_reply)

                    for _ in range(50):
                        if len(ctx.sent) >= 1 and not plugin._jobs_by_id:
                            break
                        await asyncio.sleep(0.05)
                    self.assertEqual(len(ctx.sent), 1)
                    self.assertEqual(ctx.sent[0][1][0].name, "patch.zip")

                    # Transfer captcha error still cleans up OpenList temp folder
                    srv.transfer_errno = -62
                    srv.openlist_removes.clear()
                    await plugin.pick_baidu_files(umo, "1", force=True)
                    for _ in range(50):
                        if len(ctx.sent) >= 2 and not plugin._jobs_by_id:
                            break
                        await asyncio.sleep(0.05)
                    self.assertEqual(len(srv.openlist_removes), 1)
                    self.assertIn("验证码", ctx.sent[-1][1][0].text)

                    # Missing password prompt
                    srv.transfer_errno = 0
                    srv.require_pwd_for.add("1needPwd")
                    srv.files_by_surl["1needPwd"] = [
                        {"fs_id": 3001, "server_filename": "secret.zip", "isdir": 0, "size": 100}
                    ]
                    with self.assertRaises(BaiduPanError) as cm:
                        await plugin.submit_download(umo, "https://pan.baidu.com/s/1needPwd")
                    self.assertIn("提取码", str(cm.exception))

                    # Expired cookie (-6) error translation during verify
                    srv.verify_errno = -6
                    with self.assertRaises(BaiduPanError) as cm2:
                        await plugin.submit_download(umo, "https://pan.baidu.com/s/1needPwd?pwd=1234")
                    self.assertIn("Cookie", str(cm2.exception))
                finally:
                    await plugin.terminate()
