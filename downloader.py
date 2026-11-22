"""Async HTTP downloader with multi-connection Range chunking, resume, and retries."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
import hashlib
import ipaddress
import json
import math
import mimetypes
import os
from pathlib import Path
import re
import socket
import time
from urllib.parse import unquote, urljoin, urlsplit

import aiohttp

MAX_REDIRECTS = 10
STREAM_READ_BYTES = 64 * 1024
META_MAX_AGE_SECONDS = 24 * 3600
NON_RETRYABLE_STATUSES = {400, 401, 403, 404, 410, 414, 416, 451}
RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class DownloadError(Exception):
    """Base error raised by the downloader."""


class FileTooLargeError(DownloadError):
    """Raised when the remote file exceeds configured size limits."""


class HtmlPageError(DownloadError):
    """Raised when the target responds with an HTML page instead of a file."""


class SsrfBlockedError(DownloadError):
    """Raised when a URL resolves to a blocked private/loopback network address."""


class DownloadCancelledError(DownloadError):
    """Raised when a download task is cancelled."""


@dataclass(slots=True)
class DownloadOptions:
    """Runtime options for probing and downloading a URL."""

    max_bytes: int = 300 * 1024 * 1024
    connections: int = 6
    chunk_size_bytes: int = 4 * 1024 * 1024
    timeout_sec: int = 600
    retry_count: int = 4
    block_html_pages: bool = True
    block_private_network: bool = True
    allowed_private_hosts: tuple[str, ...] = ()
    proxy: str = ""
    user_agent: str = DEFAULT_USER_AGENT
    extra_headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ProbeResult:
    """Metadata gathered before starting the full download."""

    final_url: str
    total_size: int | None
    accept_ranges: bool
    filename: str
    content_type: str
    etag: str | None
    last_modified: str | None


@dataclass(slots=True)
class DownloadResult:
    """Result returned upon completing a download to a temporary .part file."""

    part_path: Path
    meta_path: Path
    final_url: str
    filename: str
    file_size: int
    sha256: str
    etag: str | None
    last_modified: str | None
    resumed: bool
    multipart: bool


def format_bytes(num_bytes: int | None) -> str:
    """Format byte count into a human-readable string."""
    if num_bytes is None or num_bytes < 0:
        return "未知大小"
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit in ("B", "KB"):
                return f"{int(round(size))} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def sanitize_filename(name: str, default_name: str = "download.bin") -> str:
    """Sanitize a remote filename for safe local storage and QQ upload."""
    raw = unquote(str(name or "")).strip()
    raw = raw.replace("\\", "/").split("/")[-1]
    raw = re.sub(r"[\x00-\x1f\x7f]+", "", raw)
    raw = re.sub(r'[<>:"/\\|?*]+', "_", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" .")
    if not raw or raw in {".", ".."}:
        raw = default_name

    if len(raw) > 120:
        stem, dot, ext = raw.rpartition(".")
        if dot and ext and len(ext) <= 16:
            keep = max(1, 120 - len(ext) - 1)
            raw = f"{stem[:keep]}.{ext}"
        else:
            raw = raw[:120]
    return raw


def parse_content_disposition(header_val: str | None) -> str | None:
    """Extract filename from a Content-Disposition header if present."""
    if not header_val:
        return None
    ext_match = re.search(
        r"""filename\*\s*=\s*(?:[^']*)'[^']*'([^;]+)""",
        header_val,
        flags=re.IGNORECASE,
    )
    if ext_match:
        candidate = unquote(ext_match.group(1).strip().strip('"'))
        if candidate:
            return candidate

    plain_match = re.search(
        r"""filename\s*=\s*(?:"([^"]+)"|'([^']+)'|([^;\s]+))""",
        header_val,
        flags=re.IGNORECASE,
    )
    if plain_match:
        candidate = plain_match.group(1) or plain_match.group(2) or plain_match.group(3)
        if candidate:
            return unquote(candidate.strip())
    return None


def parse_content_range_total(header_val: str | None) -> int | None:
    """Parse total file size from a Content-Range header like 'bytes 0-0/12345'."""
    if not header_val:
        return None
    m = re.match(r"^\s*bytes\s+(?:\d+-\d+|\*)/(\d+|\*)\s*$", header_val, re.IGNORECASE)
    if not m or m.group(1) == "*":
        return None
    return int(m.group(1))


def _parse_content_length(header_val: str | None) -> int | None:
    if not header_val:
        return None
    try:
        val = int(header_val.strip())
    except ValueError:
        return None
    return val if val >= 0 else None


def _extract_content_type(header_val: str | None) -> str:
    if not header_val:
        return ""
    return header_val.split(";", 1)[0].strip().lower()


def _infer_filename(
    content_disposition: str | None,
    final_url: str,
    content_type: str,
    custom_filename: str | None = None,
) -> str:
    if custom_filename and custom_filename.strip():
        return sanitize_filename(custom_filename)

    cd_name = parse_content_disposition(content_disposition)
    if cd_name:
        return sanitize_filename(cd_name)

    path = urlsplit(final_url).path
    tail = unquote(path.rstrip("/").rsplit("/", 1)[-1]) if path else ""
    if tail:
        return sanitize_filename(tail)

    ext = mimetypes.guess_extension(content_type) if content_type else None
    if ext == ".jpe":
        ext = ".jpg"
    return sanitize_filename(f"download{ext or '.bin'}")


_TRANSPARENT_PROXY_FAKE_IP_NET = ipaddress.ip_network("198.18.0.0/15")


def _is_blocked_ip(ip_str: str, *, allow_fake_ip: bool = False) -> bool:
    clean = ip_str.split("%", 1)[0]
    ip = ipaddress.ip_address(clean)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if (
        allow_fake_ip
        and isinstance(ip, ipaddress.IPv4Address)
        and ip in _TRANSPARENT_PROXY_FAKE_IP_NET
    ):
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


async def validate_url_and_ssrf(url: str, options: DownloadOptions) -> str:
    """Validate URL scheme/host and reject private/loopback targets unless allowlisted."""
    raw = str(url or "").strip()
    parsed = urlsplit(raw)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise DownloadError("仅支持有效的 http/https 下载链接。")

    host = parsed.hostname.strip().lower()
    if not options.block_private_network:
        return raw

    allowed = {h.strip().lower() for h in options.allowed_private_hosts if h.strip()}
    if host in allowed:
        return raw

    try:
        ipaddress.ip_address(host)
    except ValueError:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise DownloadError(f"域名解析失败（{host}）：{exc}") from exc
        if not infos:
            raise DownloadError(f"域名未解析到可用地址：{host}")
        normalized_host = host.rstrip(".")
        # 透明代理 Fake-IP 模式通常会将外网域名解析到 198.18.0.0/15，仅针对公网域名结果放行
        allow_fake_ip = (
            "." in normalized_host
            and not normalized_host.endswith((".local", ".localhost", ".internal"))
        )
        for info in infos:
            sockaddr = info[4]
            if sockaddr and _is_blocked_ip(sockaddr[0], allow_fake_ip=allow_fake_ip):
                raise SsrfBlockedError(f"已拦截内网或本机地址：{host} ({sockaddr[0]})")
        return raw

    if _is_blocked_ip(host):
        raise SsrfBlockedError(f"已拦截内网或本机 IP：{host}")
    return raw


def _check_html_block(content_type: str, filename: str, options: DownloadOptions) -> None:
    if not options.block_html_pages:
        return
    if content_type in {"text/html", "application/xhtml+xml"}:
        lower = filename.lower()
        if not lower.endswith((".html", ".htm", ".xhtml")):
            raise HtmlPageError(
                "目标返回的是 HTML 网页（可能需要登录、人机验证或链接已失效），已取消下载。"
            )


def _retry_delay_seconds(attempt: int, retry_after_header: str | None = None) -> float:
    if retry_after_header:
        try:
            val = float(retry_after_header.strip())
            if val > 0:
                return min(val, 30.0)
        except ValueError:
            pass
    return float(min(2**attempt, 16))


async def _open_following_redirects(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    options: DownloadOptions,
    extra_headers: dict[str, str] | None = None,
    cancel_event: asyncio.Event | None = None,
) -> tuple[aiohttp.ClientResponse, str]:
    current_url = url
    req_method = method
    headers = {"User-Agent": options.user_agent}
    if options.extra_headers:
        headers.update(options.extra_headers)
    if extra_headers:
        headers.update(extra_headers)

    for _ in range(MAX_REDIRECTS + 1):
        if cancel_event is not None and cancel_event.is_set():
            raise DownloadCancelledError("下载任务已取消。")
        await validate_url_and_ssrf(current_url, options)
        resp = await session.request(
            req_method,
            current_url,
            headers=headers,
            proxy=options.proxy or None,
            allow_redirects=False,
        )
        if resp.status in {301, 302, 303, 307, 308}:
            location = resp.headers.get("Location")
            await resp.release()
            if not location:
                raise DownloadError(f"重定向响应缺少 Location（HTTP {resp.status}）。")
            current_url = urljoin(current_url, location)
            if resp.status in {301, 302, 303} and req_method not in {"GET", "HEAD"}:
                req_method = "GET"
            continue
        return resp, current_url

    raise DownloadError("重定向次数过多（超过 10 次）。")


async def probe(
    session: aiohttp.ClientSession,
    url: str,
    options: DownloadOptions,
    custom_filename: str | None = None,
    cancel_event: asyncio.Event | None = None,
) -> ProbeResult:
    """Probe remote metadata (size, Range support, filename, validators)."""
    await validate_url_and_ssrf(url, options)
    last_exc: Exception | None = None

    for attempt in range(max(1, options.retry_count + 1)):
        try:
            return await _probe_once(
                session,
                url,
                options,
                custom_filename=custom_filename,
                cancel_event=cancel_event,
            )
        except (DownloadCancelledError, SsrfBlockedError, HtmlPageError, FileTooLargeError):
            raise
        except DownloadError:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt >= options.retry_count:
                break
            await asyncio.sleep(_retry_delay_seconds(attempt))

    raise DownloadError(f"探测下载链接失败：{last_exc}")


async def _probe_once(
    session: aiohttp.ClientSession,
    url: str,
    options: DownloadOptions,
    custom_filename: str | None = None,
    cancel_event: asyncio.Event | None = None,
) -> ProbeResult:
    final_url = url
    total_size: int | None = None
    accept_ranges = False
    content_type = ""
    content_disposition: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    need_range_probe = True

    resp, final_url = await _open_following_redirects(
        session, "HEAD", url, options, cancel_event=cancel_event
    )
    try:
        if 200 <= resp.status < 300:
            total_size = _parse_content_length(resp.headers.get("Content-Length"))
            accept_ranges = "bytes" in (resp.headers.get("Accept-Ranges", "").lower())
            content_type = _extract_content_type(resp.headers.get("Content-Type"))
            content_disposition = resp.headers.get("Content-Disposition")
            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")
            need_range_probe = (total_size is None) or (not accept_ranges)
        elif resp.status in {400, 403, 405, 501}:
            need_range_probe = True
        elif resp.status in RETRYABLE_STATUSES:
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=resp.reason or "",
                headers=resp.headers,
            )
        else:
            raise DownloadError(f"远端返回 HTTP {resp.status}。")
    finally:
        await resp.release()

    if total_size is not None and total_size > options.max_bytes:
        raise FileTooLargeError(
            f"文件大小为 {format_bytes(total_size)}，超过上限 {format_bytes(options.max_bytes)}。"
        )

    if need_range_probe:
        r_resp, r_final_url = await _open_following_redirects(
            session,
            "GET",
            final_url,
            options,
            extra_headers={"Range": "bytes=0-0"},
            cancel_event=cancel_event,
        )
        try:
            final_url = r_final_url
            if r_resp.status == 206:
                accept_ranges = True
                cr_total = parse_content_range_total(r_resp.headers.get("Content-Range"))
                if cr_total is not None:
                    total_size = cr_total
                content_type = _extract_content_type(r_resp.headers.get("Content-Type")) or content_type
                content_disposition = r_resp.headers.get("Content-Disposition") or content_disposition
                etag = r_resp.headers.get("ETag") or etag
                last_modified = r_resp.headers.get("Last-Modified") or last_modified
            elif 200 <= r_resp.status < 300:
                if total_size is None:
                    total_size = _parse_content_length(r_resp.headers.get("Content-Length"))
                if "bytes" in r_resp.headers.get("Accept-Ranges", "").lower():
                    accept_ranges = True
                content_type = _extract_content_type(r_resp.headers.get("Content-Type")) or content_type
                content_disposition = r_resp.headers.get("Content-Disposition") or content_disposition
                etag = r_resp.headers.get("ETag") or etag
                last_modified = r_resp.headers.get("Last-Modified") or last_modified
            elif r_resp.status == 416:
                cr_total = parse_content_range_total(r_resp.headers.get("Content-Range"))
                if cr_total is not None:
                    total_size = cr_total
                accept_ranges = True
            elif r_resp.status in RETRYABLE_STATUSES:
                raise aiohttp.ClientResponseError(
                    r_resp.request_info,
                    r_resp.history,
                    status=r_resp.status,
                    message=r_resp.reason or "",
                    headers=r_resp.headers,
                )
            else:
                raise DownloadError(f"远端返回 HTTP {r_resp.status}。")
        finally:
            r_resp.close()

    filename = _infer_filename(
        content_disposition,
        final_url,
        content_type,
        custom_filename=custom_filename,
    )
    _check_html_block(content_type, filename, options)

    if total_size is not None and total_size > options.max_bytes:
        raise FileTooLargeError(
            f"文件大小为 {format_bytes(total_size)}，超过上限 {format_bytes(options.max_bytes)}。"
        )

    return ProbeResult(
        final_url=final_url,
        total_size=total_size,
        accept_ranges=accept_ranges,
        filename=filename,
        content_type=content_type,
        etag=etag,
        last_modified=last_modified,
    )


def _validators_match(
    meta: dict,
    probe_res: ProbeResult,
    mode: str,
    resume_key: str | None = None,
) -> bool:
    if meta.get("mode") != mode:
        return False
    if meta.get("total_size") != probe_res.total_size:
        return False
    if resume_key:
        if meta.get("resume_key") != resume_key:
            return False
        return (time.time() - float(meta.get("updated_at", 0))) < META_MAX_AGE_SECONDS
    if probe_res.etag and meta.get("etag"):
        return probe_res.etag == meta.get("etag")
    if probe_res.last_modified and meta.get("last_modified"):
        return probe_res.last_modified == meta.get("last_modified")
    if meta.get("final_url") != probe_res.final_url:
        return False
    return (time.time() - float(meta.get("updated_at", 0))) < META_MAX_AGE_SECONDS


def _write_meta_atomic(meta_path: Path, payload: dict) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_meta = meta_path.with_suffix(".meta.json.tmp")
    payload["updated_at"] = time.time()
    with open(tmp_meta, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_meta, meta_path)


def _read_meta(meta_path: Path) -> dict | None:
    if not meta_path.exists():
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _compute_sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _cleanup_temp_files(part_path: Path, meta_path: Path) -> None:
    for p in (part_path, meta_path, meta_path.with_suffix(".meta.json.tmp")):
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass


def create_http_session(options: DownloadOptions) -> aiohttp.ClientSession:
    """Create a configured aiohttp ClientSession for probing and downloading."""
    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_connect=20, sock_read=60)
    connector = aiohttp.TCPConnector(limit=max(16, options.connections * 4))
    return aiohttp.ClientSession(timeout=timeout, connector=connector)


async def download(
    url: str,
    url_key: str,
    tmp_dir: Path,
    options: DownloadOptions,
    probe_result: ProbeResult | None = None,
    custom_filename: str | None = None,
    progress_cb: Callable[[int, int | None], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    session: aiohttp.ClientSession | None = None,
    resume_key: str | None = None,
) -> DownloadResult:
    """Download a URL to tmp_dir/<url_key>.part with chunking, resume, and retries."""
    tmp_dir.mkdir(parents=True, exist_ok=True)
    part_path = tmp_dir / f"{url_key}.part"
    meta_path = tmp_dir / f"{url_key}.meta.json"

    async def _run(active_session: aiohttp.ClientSession) -> DownloadResult:
        res_probe = probe_result or await probe(
            active_session,
            url,
            options,
            custom_filename=custom_filename,
            cancel_event=cancel_event,
        )
        if custom_filename and custom_filename.strip():
            res_probe.filename = sanitize_filename(custom_filename)

        can_multipart = (
            res_probe.accept_ranges
            and res_probe.total_size is not None
            and res_probe.total_size >= max(1, options.chunk_size_bytes)
            and options.connections > 1
        )
        if can_multipart:
            result = await _download_multipart(
                active_session,
                res_probe,
                part_path,
                meta_path,
                options,
                progress_cb=progress_cb,
                cancel_event=cancel_event,
                resume_key=resume_key,
            )
            if result is not None:
                return result

        return await _download_single_stream(
            active_session,
            res_probe,
            part_path,
            meta_path,
            options,
            progress_cb=progress_cb,
            cancel_event=cancel_event,
            resume_key=resume_key,
        )

    async def _run_with_session() -> DownloadResult:
        if session is not None:
            return await _run(session)
        async with create_http_session(options) as owned_session:
            return await _run(owned_session)

    try:
        return await asyncio.wait_for(_run_with_session(), timeout=options.timeout_sec)
    except asyncio.TimeoutError as exc:
        raise DownloadError(f"下载超时（超过 {options.timeout_sec} 秒），断点进度已保留。") from exc


async def _download_multipart(
    session: aiohttp.ClientSession,
    probe_res: ProbeResult,
    part_path: Path,
    meta_path: Path,
    options: DownloadOptions,
    progress_cb: Callable[[int, int | None], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    resume_key: str | None = None,
) -> DownloadResult | None:
    total_size = probe_res.total_size
    assert total_size is not None
    chunk_size = max(64 * 1024, options.chunk_size_bytes)
    total_chunks = math.ceil(total_size / chunk_size)

    def _chunk_bounds(idx: int) -> tuple[int, int]:
        start = idx * chunk_size
        end = min(total_size - 1, start + chunk_size - 1)
        return start, end

    meta = _read_meta(meta_path)
    resumed = False
    completed_set: set[int] = set()

    if (
        meta is not None
        and part_path.exists()
        and meta.get("chunk_size") == chunk_size
        and _validators_match(meta, probe_res, mode="multipart", resume_key=resume_key)
        and os.path.getsize(part_path) == total_size
    ):
        raw_completed = meta.get("completed_chunks", [])
        if isinstance(raw_completed, list):
            completed_set = {
                int(x) for x in raw_completed if isinstance(x, int) and 0 <= x < total_chunks
            }
            resumed = bool(completed_set)
    else:
        with open(part_path, "wb") as f:
            f.truncate(total_size)
        _write_meta_atomic(
            meta_path,
            {
                "mode": "multipart",
                "resume_key": resume_key,
                "final_url": probe_res.final_url,
                "etag": probe_res.etag,
                "last_modified": probe_res.last_modified,
                "total_size": total_size,
                "chunk_size": chunk_size,
                "completed_chunks": [],
            },
        )

    pending = deque(idx for idx in range(total_chunks) if idx not in completed_set)
    completed_bytes = sum((_chunk_bounds(i)[1] - _chunk_bounds(i)[0] + 1) for i in completed_set)
    if progress_cb is not None:
        progress_cb(completed_bytes, total_size)

    if not pending:
        sha256 = await asyncio.to_thread(_compute_sha256_file, part_path)
        return DownloadResult(
            part_path=part_path,
            meta_path=meta_path,
            final_url=probe_res.final_url,
            filename=probe_res.filename,
            file_size=total_size,
            sha256=sha256,
            etag=probe_res.etag,
            last_modified=probe_res.last_modified,
            resumed=True,
            multipart=True,
        )

    state_lock = asyncio.Lock()
    active_limit = min(max(1, options.connections), len(pending))
    active_workers = 0
    fallback_to_stream = False
    fatal_error: BaseException | None = None

    fd = os.open(part_path, os.O_RDWR)
    try:

        async def _worker(worker_idx: int) -> None:
            nonlocal active_limit, active_workers, completed_bytes, fallback_to_stream, fatal_error
            active_workers += 1
            try:
                while True:
                    if fatal_error is not None or fallback_to_stream:
                        return
                    if cancel_event is not None and cancel_event.is_set():
                        fatal_error = DownloadCancelledError("下载任务已取消。")
                        return
                    if worker_idx >= active_limit:
                        return
                    try:
                        chunk_idx = pending.popleft()
                    except IndexError:
                        return

                    start, end = _chunk_bounds(chunk_idx)
                    expected_len = end - start + 1
                    ok = await _download_single_chunk(chunk_idx, start, end, expected_len)
                    if not ok:
                        return
            finally:
                active_workers -= 1

        async def _download_single_chunk(
            chunk_idx: int,
            start: int,
            end: int,
            expected_len: int,
        ) -> bool:
            nonlocal active_limit, completed_bytes, fallback_to_stream, fatal_error

            for attempt in range(max(1, options.retry_count + 1)):
                if fatal_error is not None or fallback_to_stream:
                    return False
                if cancel_event is not None and cancel_event.is_set():
                    fatal_error = DownloadCancelledError("下载任务已取消。")
                    return False

                resp = None
                try:
                    resp, _ = await _open_following_redirects(
                        session,
                        "GET",
                        probe_res.final_url,
                        options,
                        extra_headers={"Range": f"bytes={start}-{end}"},
                        cancel_event=cancel_event,
                    )
                    if resp.status == 200:
                        fallback_to_stream = True
                        return False
                    if resp.status in NON_RETRYABLE_STATUSES:
                        fatal_error = DownloadError(f"分片下载被拒绝（HTTP {resp.status}）。")
                        return False
                    if resp.status in RETRYABLE_STATUSES:
                        retry_after = resp.headers.get("Retry-After")
                        if resp.status == 429:
                            active_limit = max(1, active_limit // 2)
                        if attempt >= options.retry_count:
                            fatal_error = DownloadError(
                                f"分片 #{chunk_idx} 重试耗尽（HTTP {resp.status}）。"
                            )
                            return False
                        await asyncio.sleep(_retry_delay_seconds(attempt, retry_after))
                        continue
                    if resp.status != 206:
                        fatal_error = DownloadError(f"分片请求返回异常状态码 HTTP {resp.status}。")
                        return False

                    ctype = _extract_content_type(resp.headers.get("Content-Type"))
                    _check_html_block(ctype, probe_res.filename, options)

                    written = 0
                    async for data in resp.content.iter_chunked(STREAM_READ_BYTES):
                        if cancel_event is not None and cancel_event.is_set():
                            fatal_error = DownloadCancelledError("下载任务已取消。")
                            return False
                        if written + len(data) > expected_len:
                            data = data[: expected_len - written]
                        if data:
                            await asyncio.to_thread(os.pwrite, fd, data, start + written)
                            written += len(data)
                        if written >= expected_len:
                            break

                    if written != expected_len:
                        raise aiohttp.ClientPayloadError(
                            f"分片 #{chunk_idx} 长度不完整：{written}/{expected_len}"
                        )

                    async with state_lock:
                        completed_set.add(chunk_idx)
                        completed_bytes += expected_len
                        _write_meta_atomic(
                            meta_path,
                            {
                                "mode": "multipart",
                                "resume_key": resume_key,
                                "final_url": probe_res.final_url,
                                "etag": probe_res.etag,
                                "last_modified": probe_res.last_modified,
                                "total_size": total_size,
                                "chunk_size": chunk_size,
                                "completed_chunks": sorted(completed_set),
                            },
                        )
                        if progress_cb is not None:
                            progress_cb(completed_bytes, total_size)
                    return True
                except (DownloadCancelledError, SsrfBlockedError, HtmlPageError, FileTooLargeError) as exc:
                    fatal_error = exc
                    return False
                except DownloadError as exc:
                    fatal_error = exc
                    return False
                except (aiohttp.ClientError, TimeoutError, OSError) as exc:
                    if attempt >= options.retry_count:
                        fatal_error = DownloadError(f"分片 #{chunk_idx} 下载失败：{exc}")
                        return False
                    await asyncio.sleep(_retry_delay_seconds(attempt))
                finally:
                    if resp is not None:
                        resp.close()

            return False

        workers = [asyncio.create_task(_worker(i)) for i in range(active_limit)]
        await asyncio.gather(*workers)
    finally:
        os.close(fd)

    if fallback_to_stream:
        return None
    if fatal_error is not None:
        if isinstance(fatal_error, (FileTooLargeError, HtmlPageError, SsrfBlockedError)):
            _cleanup_temp_files(part_path, meta_path)
        raise fatal_error

    actual_size = os.path.getsize(part_path)
    if actual_size > options.max_bytes:
        _cleanup_temp_files(part_path, meta_path)
        raise FileTooLargeError(
            f"文件大小为 {format_bytes(actual_size)}，超过上限 {format_bytes(options.max_bytes)}。"
        )
    sha256 = await asyncio.to_thread(_compute_sha256_file, part_path)
    return DownloadResult(
        part_path=part_path,
        meta_path=meta_path,
        final_url=probe_res.final_url,
        filename=probe_res.filename,
        file_size=actual_size,
        sha256=sha256,
        etag=probe_res.etag,
        last_modified=probe_res.last_modified,
        resumed=resumed,
        multipart=True,
    )


async def _download_single_stream(
    session: aiohttp.ClientSession,
    probe_res: ProbeResult,
    part_path: Path,
    meta_path: Path,
    options: DownloadOptions,
    progress_cb: Callable[[int, int | None], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    resume_key: str | None = None,
) -> DownloadResult:
    meta = _read_meta(meta_path)
    existing_bytes = 0
    resumed = False

    if (
        probe_res.accept_ranges
        and meta is not None
        and part_path.exists()
        and _validators_match(meta, probe_res, mode="stream", resume_key=resume_key)
    ):
        size_on_disk = os.path.getsize(part_path)
        if 0 < size_on_disk and (probe_res.total_size is None or size_on_disk <= probe_res.total_size):
            existing_bytes = size_on_disk
            resumed = True

    if existing_bytes > options.max_bytes:
        _cleanup_temp_files(part_path, meta_path)
        raise FileTooLargeError(
            f"文件超过上限 {format_bytes(options.max_bytes)}，已取消下载。"
        )

    if not resumed:
        with open(part_path, "wb"):
            pass
        _write_meta_atomic(
            meta_path,
            {
                "mode": "stream",
                "resume_key": resume_key,
                "final_url": probe_res.final_url,
                "etag": probe_res.etag,
                "last_modified": probe_res.last_modified,
                "total_size": probe_res.total_size,
            },
        )

    if probe_res.total_size is not None and existing_bytes == probe_res.total_size:
        sha256 = await asyncio.to_thread(_compute_sha256_file, part_path)
        return DownloadResult(
            part_path=part_path,
            meta_path=meta_path,
            final_url=probe_res.final_url,
            filename=probe_res.filename,
            file_size=existing_bytes,
            sha256=sha256,
            etag=probe_res.etag,
            last_modified=probe_res.last_modified,
            resumed=True,
            multipart=False,
        )

    last_exc: Exception | None = None
    for attempt in range(max(1, options.retry_count + 1)):
        if cancel_event is not None and cancel_event.is_set():
            raise DownloadCancelledError("下载任务已取消。")

        current_offset = os.path.getsize(part_path) if part_path.exists() else 0
        if not probe_res.accept_ranges:
            current_offset = 0

        extra_headers = {"Range": f"bytes={current_offset}-"} if (probe_res.accept_ranges and current_offset > 0) else None
        resp = None
        try:
            resp, final_url = await _open_following_redirects(
                session,
                "GET",
                probe_res.final_url,
                options,
                extra_headers=extra_headers,
                cancel_event=cancel_event,
            )
            probe_res.final_url = final_url

            if resp.status in NON_RETRYABLE_STATUSES:
                raise DownloadError(f"远端返回 HTTP {resp.status}。")
            if resp.status in RETRYABLE_STATUSES:
                retry_after = resp.headers.get("Retry-After")
                if attempt >= options.retry_count:
                    raise DownloadError(f"下载重试耗尽（HTTP {resp.status}）。")
                await asyncio.sleep(_retry_delay_seconds(attempt, retry_after))
                continue
            if resp.status not in {200, 206}:
                raise DownloadError(f"远端返回异常状态码 HTTP {resp.status}。")

            ctype = _extract_content_type(resp.headers.get("Content-Type"))
            _check_html_block(ctype, probe_res.filename, options)

            clen = _parse_content_length(resp.headers.get("Content-Length"))
            cr_total = (
                parse_content_range_total(resp.headers.get("Content-Range"))
                if resp.status == 206
                else clen
            )
            expected_total = cr_total if cr_total is not None else probe_res.total_size
            if expected_total is not None and expected_total > options.max_bytes:
                _cleanup_temp_files(part_path, meta_path)
                raise FileTooLargeError(
                    f"文件大小为 {format_bytes(expected_total)}，超过上限 {format_bytes(options.max_bytes)}。"
                )

            append_mode = resp.status == 206 and current_offset > 0
            written_total = current_offset if append_mode else 0
            if append_mode:
                resumed = True

            hasher = None if append_mode else hashlib.sha256()
            file_mode = "ab" if append_mode else "wb"

            if progress_cb is not None:
                progress_cb(written_total, expected_total)

            with open(part_path, file_mode) as f:
                async for chunk in resp.content.iter_chunked(STREAM_READ_BYTES):
                    if cancel_event is not None and cancel_event.is_set():
                        raise DownloadCancelledError("下载任务已取消。")
                    if not chunk:
                        continue
                    written_total += len(chunk)
                    if written_total > options.max_bytes:
                        f.close()
                        _cleanup_temp_files(part_path, meta_path)
                        raise FileTooLargeError(
                            f"文件实际传输体积超过上限 {format_bytes(options.max_bytes)}，已熔断。"
                        )
                    f.write(chunk)
                    if hasher is not None:
                        hasher.update(chunk)
                    if progress_cb is not None:
                        progress_cb(written_total, expected_total)

            sha256 = (
                hasher.hexdigest()
                if hasher is not None
                else await asyncio.to_thread(_compute_sha256_file, part_path)
            )
            return DownloadResult(
                part_path=part_path,
                meta_path=meta_path,
                final_url=probe_res.final_url,
                filename=probe_res.filename,
                file_size=written_total,
                sha256=sha256,
                etag=probe_res.etag,
                last_modified=probe_res.last_modified,
                resumed=resumed,
                multipart=False,
            )
        except (DownloadCancelledError, SsrfBlockedError, HtmlPageError, FileTooLargeError):
            raise
        except DownloadError:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt >= options.retry_count:
                break
            await asyncio.sleep(_retry_delay_seconds(attempt))
        finally:
            if resp is not None:
                resp.close()

    raise DownloadError(f"下载失败：{last_exc}")
