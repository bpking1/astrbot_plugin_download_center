from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

try:
    from . import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # noqa: F401
from aiohttp import web

from astrbot_plugin_download_center.downloader import (
    DownloadCancelledError,
    DownloadOptions,
    FileTooLargeError,
    HtmlPageError,
    SsrfBlockedError,
    create_http_session,
    download,
    probe,
    validate_url_and_ssrf,
)


class _LocalTestServer:
    def __init__(self, app: web.Application) -> None:
        self.app = app
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.port: int = 0

    async def __aenter__(self) -> _LocalTestServer:
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


class DownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_multipart_download_and_resume(self) -> None:
        payload = os.urandom(320 * 1024)  # 320 KB across 5 x 64 KB chunks
        expected_sha = hashlib.sha256(payload).hexdigest()
        etag = '"v1-payload"'
        range_requests: list[str] = []

        async def handle_file(request: web.Request) -> web.StreamResponse:
            headers = {
                "Accept-Ranges": "bytes",
                "ETag": etag,
                "Content-Disposition": "attachment; filename*=UTF-8''hello%20world.bin",
            }
            if request.method == "HEAD":
                headers["Content-Length"] = str(len(payload))
                return web.Response(status=200, headers=headers)

            rng = request.headers.get("Range")
            if rng:
                range_requests.append(rng)
                spec = rng.replace("bytes=", "")
                start_s, end_s = spec.split("-", 1)
                start = int(start_s)
                end = int(end_s) if end_s else len(payload) - 1
                end = min(end, len(payload) - 1)
                headers["Content-Range"] = f"bytes {start}-{end}/{len(payload)}"
                headers["Content-Length"] = str(end - start + 1)
                return web.Response(status=206, body=payload[start : end + 1], headers=headers)

            headers["Content-Length"] = str(len(payload))
            return web.Response(status=200, body=payload, headers=headers)

        app = web.Application()
        app.router.add_route("*", "/file.bin", handle_file)

        async with _LocalTestServer(app) as srv:
            url = f"http://127.0.0.1:{srv.port}/file.bin"
            opts = DownloadOptions(
                max_bytes=2 * 1024 * 1024,
                connections=4,
                chunk_size_bytes=64 * 1024,
                block_private_network=False,
            )

            with tempfile.TemporaryDirectory() as tmp:
                tmp_dir = Path(tmp)
                res = await download(url, "job1", tmp_dir, opts)
                self.assertTrue(res.multipart)
                self.assertFalse(res.resumed)
                self.assertEqual(res.filename, "hello world.bin")
                self.assertEqual(res.file_size, len(payload))
                self.assertEqual(res.sha256, expected_sha)
                self.assertEqual(res.part_path.read_bytes(), payload)

                # Pre-populate partial download (first 2 chunks completed) to test resume
                range_requests.clear()
                part_p = tmp_dir / "job_resume.part"
                meta_p = tmp_dir / "job_resume.meta.json"
                with open(part_p, "wb") as f:
                    f.truncate(len(payload))
                    f.seek(0)
                    f.write(payload[: 2 * 64 * 1024])
                meta_p.write_text(
                    json.dumps(
                        {
                            "mode": "multipart",
                            "final_url": url,
                            "etag": etag,
                            "last_modified": None,
                            "total_size": len(payload),
                            "chunk_size": 64 * 1024,
                            "completed_chunks": [0, 1],
                            "updated_at": 9999999999.0,
                        }
                    ),
                    encoding="utf-8",
                )

                res2 = await download(url, "job_resume", tmp_dir, opts)
                self.assertTrue(res2.resumed)
                self.assertEqual(res2.sha256, expected_sha)
                self.assertEqual(len(range_requests), 3)
                self.assertNotIn("bytes=0-65535", range_requests)

                # Change ETag -> should invalidate old resume and re-download all chunks
                etag = '"v2-changed"'
                range_requests.clear()
                part_p.write_bytes(b"\x00" * len(payload))
                meta_p.write_text(
                    json.dumps(
                        {
                            "mode": "multipart",
                            "final_url": url,
                            "etag": '"v1-payload"',
                            "last_modified": None,
                            "total_size": len(payload),
                            "chunk_size": 64 * 1024,
                            "completed_chunks": [0, 1, 2],
                            "updated_at": 9999999999.0,
                        }
                    ),
                    encoding="utf-8",
                )
                res3 = await download(url, "job_resume", tmp_dir, opts)
                self.assertFalse(res3.resumed)
                self.assertEqual(res3.sha256, expected_sha)
                self.assertEqual(len(range_requests), 5)

    async def test_retry_on_503_and_429(self) -> None:
        payload = b"ABCD" * 50_000  # 200 KB
        expected_sha = hashlib.sha256(payload).hexdigest()
        attempts_by_range: dict[str, int] = {}

        async def handle_flaky(request: web.Request) -> web.StreamResponse:
            headers = {"Accept-Ranges": "bytes", "ETag": '"flaky"'}
            if request.method == "HEAD":
                headers["Content-Length"] = str(len(payload))
                return web.Response(status=200, headers=headers)

            rng = request.headers.get("Range", "")
            attempts_by_range[rng] = attempts_by_range.get(rng, 0) + 1
            count = attempts_by_range[rng]

            if rng.startswith("bytes=0-") and count == 1:
                return web.Response(status=503, text="busy")
            if count == 1 and not rng.startswith("bytes=0-"):
                return web.Response(status=429, headers={"Retry-After": "0.05"}, text="slow down")

            spec = rng.replace("bytes=", "")
            start_s, end_s = spec.split("-", 1)
            start, end = int(start_s), min(int(end_s), len(payload) - 1)
            headers["Content-Range"] = f"bytes {start}-{end}/{len(payload)}"
            headers["Content-Length"] = str(end - start + 1)
            return web.Response(status=206, body=payload[start : end + 1], headers=headers)

        app = web.Application()
        app.router.add_route("*", "/flaky.bin", handle_flaky)

        async with _LocalTestServer(app) as srv:
            url = f"http://127.0.0.1:{srv.port}/flaky.bin"
            opts = DownloadOptions(
                max_bytes=2 * 1024 * 1024,
                connections=4,
                chunk_size_bytes=64 * 1024,
                retry_count=3,
                block_private_network=False,
            )
            with tempfile.TemporaryDirectory() as tmp:
                res = await download(url, "flaky", Path(tmp), opts)
                self.assertEqual(res.sha256, expected_sha)
                self.assertEqual(res.file_size, len(payload))

    async def test_fallback_to_single_stream_without_range(self) -> None:
        payload = b"NO-RANGE-" * 15_000
        expected_sha = hashlib.sha256(payload).hexdigest()

        async def handle_no_range(request: web.Request) -> web.StreamResponse:
            if request.method == "HEAD":
                return web.Response(status=405)
            return web.Response(status=200, body=payload, headers={"Content-Length": str(len(payload))})

        app = web.Application()
        app.router.add_route("*", "/stream.zip", handle_no_range)

        async with _LocalTestServer(app) as srv:
            url = f"http://127.0.0.1:{srv.port}/stream.zip"
            opts = DownloadOptions(
                max_bytes=1024 * 1024,
                connections=6,
                chunk_size_bytes=64 * 1024,
                block_private_network=False,
            )
            with tempfile.TemporaryDirectory() as tmp:
                res = await download(url, "norange", Path(tmp), opts)
                self.assertFalse(res.multipart)
                self.assertEqual(res.filename, "stream.zip")
                self.assertEqual(res.sha256, expected_sha)

    async def test_size_limits_head_and_chunked_abort(self) -> None:
        async def handle_big_head(request: web.Request) -> web.StreamResponse:
            return web.Response(status=200, headers={"Content-Length": str(500 * 1024 * 1024)})

        async def handle_chunked_big(request: web.Request) -> web.StreamResponse:
            if request.method == "HEAD":
                return web.Response(status=405)
            if request.headers.get("Range") == "bytes=0-0":
                return web.Response(status=200, body=b"x")
            resp = web.StreamResponse(status=200, headers={"Content-Type": "application/octet-stream"})
            await resp.prepare(request)
            for _ in range(10):
                await resp.write(b"Z" * (64 * 1024))
            await resp.write_eof()
            return resp

        app = web.Application()
        app.router.add_route("*", "/big.iso", handle_big_head)
        app.router.add_route("*", "/chunked.bin", handle_chunked_big)

        async with _LocalTestServer(app) as srv:
            opts = DownloadOptions(
                max_bytes=200 * 1024,
                connections=4,
                chunk_size_bytes=64 * 1024,
                block_private_network=False,
            )
            with tempfile.TemporaryDirectory() as tmp:
                tmp_dir = Path(tmp)
                with self.assertRaises(FileTooLargeError):
                    await download(f"http://127.0.0.1:{srv.port}/big.iso", "big1", tmp_dir, opts)

                with self.assertRaises(FileTooLargeError):
                    await download(f"http://127.0.0.1:{srv.port}/chunked.bin", "big2", tmp_dir, opts)
                self.assertFalse((tmp_dir / "big2.part").exists())

    async def test_html_and_ssrf_blocking(self) -> None:
        async def handle_html(request: web.Request) -> web.StreamResponse:
            return web.Response(
                status=200,
                text="<html><body>Login required</body></html>",
                content_type="text/html",
            )

        async def handle_redir(request: web.Request) -> web.StreamResponse:
            port = request.url.port
            raise web.HTTPFound(location=f"http://127.0.0.1:{port}/secret.bin")

        app = web.Application()
        app.router.add_route("*", "/login", handle_html)
        app.router.add_route("*", "/jump", handle_redir)

        async with _LocalTestServer(app) as srv:
            opts_html = DownloadOptions(block_private_network=False, block_html_pages=True)
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(HtmlPageError):
                    await download(f"http://127.0.0.1:{srv.port}/login", "html", Path(tmp), opts_html)

            opts_ssrf = DownloadOptions(block_private_network=True, allowed_private_hosts=())
            async with create_http_session(opts_ssrf) as session:
                with self.assertRaises(SsrfBlockedError):
                    await probe(session, f"http://127.0.0.1:{srv.port}/login", opts_ssrf)

            # Allow localhost only, redirect to 127.0.0.1 must still be blocked
            opts_redir = DownloadOptions(block_private_network=True, allowed_private_hosts=("localhost",))
            async with create_http_session(opts_redir) as session:
                with self.assertRaises(SsrfBlockedError):
                    await probe(session, f"http://localhost:{srv.port}/jump", opts_redir)

    async def test_transparent_proxy_fake_ip_allowed_for_domains_only(self) -> None:
        opts = DownloadOptions(block_private_network=True, allowed_private_hosts=())
        dns_table = {
            "release-assets.githubusercontent.com": "198.18.7.112",
            "evil-lan.example.com": "192.168.10.2",
            "evil-loop.example.com": "127.0.0.1",
            "openlist": "198.18.0.5",
            "nas.local": "198.18.0.5",
            "svc.internal": "198.18.0.5",
        }

        async def fake_getaddrinfo(host: str, port: int, type: int = 0):
            ip = dns_table.get(host)
            if not ip:
                raise OSError(f"unmapped host {host}")
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))]

        loop = asyncio.get_running_loop()
        with patch.object(loop, "getaddrinfo", side_effect=fake_getaddrinfo):
            allowed_url = "https://release-assets.githubusercontent.com/a.apk"
            self.assertEqual(await validate_url_and_ssrf(allowed_url, opts), allowed_url)

            for blocked_url in (
                "http://198.18.7.112/a.apk",
                "https://evil-lan.example.com/a.apk",
                "https://evil-loop.example.com/a.apk",
                "http://openlist/a.apk",
                "http://nas.local/a.apk",
                "http://svc.internal/a.apk",
            ):
                with self.subTest(url=blocked_url):
                    with self.assertRaises(SsrfBlockedError):
                        await validate_url_and_ssrf(blocked_url, opts)
