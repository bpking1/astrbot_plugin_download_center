"""AstrBot Download Center plugin: chat-driven file downloader with dedup cache & Baidu Netdisk."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
import re
import time
from typing import Any
import uuid

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, StarTools, register

from .baidu_pan import (
    BAIDU_WEB_API_BASE,
    BaiduConfig,
    BaiduPanClient,
    BaiduPanError,
    BaiduShareFile,
    BaiduShareMetadata,
    ShareSessionStore,
    canonical_baidu_key,
    extract_baidu_share_info,
    is_baidu_pan_url,
    parse_pick_indices,
)
from .cache_store import CacheEntry, CacheStore, canonical_key_hash, url_key
from .downloader import (
    DEFAULT_USER_AGENT,
    DownloadCancelledError,
    DownloadError,
    DownloadOptions,
    ProbeResult,
    create_http_session,
    download,
    format_bytes,
    probe,
    sanitize_filename,
    validate_url_and_ssrf,
)
from .openlist_client import OpenListClient, OpenListError, join_openlist_path
from .uploader import UploadError, normalize_upload_filename, upload_file_to_session

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_PLAIN_PWD_RE = re.compile(r"^[A-Za-z0-9]{4}$")


@dataclass
class DownloadJob:
    """Represents a deduplicated in-flight download task."""

    job_id: str
    url: str
    url_key: str
    final_url_key: str
    filename: str
    total_size: int | None
    waiters: dict[str, str] = field(default_factory=dict)
    downloaded_bytes: int = 0
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


@register(
    "astrbot_plugin_download_center",
    "bpking1",
    "聊天识别下载需求，支持直链与百度网盘（OpenList）分片续传、去重缓存与自动上传。",
    "1.1.0",
)
class DownloadCenterPlugin(Star):
    """Chat-driven download center with multi-connection resume, dedup cache, and Baidu Netdisk."""

    def __init__(
        self,
        context: Context,
        config: Any = None,
        data_dir: Path | None = None,
        baidu_api_base: str = BAIDU_WEB_API_BASE,
        openlist_client: OpenListClient | None = None,
    ) -> None:
        super().__init__(context)
        self.config: dict[str, Any] = (
            config if isinstance(config, dict) or hasattr(config, "get") else {}
        )
        self._injected_data_dir = Path(data_dir) if data_dir is not None else None
        self._baidu_api_base = baidu_api_base
        self._injected_openlist_client = openlist_client
        self.store: CacheStore | None = None
        self._share_sessions = ShareSessionStore()
        self._inflight: dict[str, DownloadJob] = {}
        self._jobs_by_id: dict[str, DownloadJob] = {}
        self._lock = asyncio.Lock()
        self._semaphore: asyncio.Semaphore | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._baidu_daily_date: str = ""
        self._baidu_daily_count: int = 0

    async def initialize(self) -> None:
        data_dir = self._injected_data_dir or StarTools.get_data_dir("astrbot_plugin_download_center")
        self.store = CacheStore(data_dir)
        max_conc = self._cfg_int("max_concurrent_downloads", 2, min_val=1, max_val=8)
        self._semaphore = asyncio.Semaphore(max_conc)

        stats = self.store.cleanup(
            max_total_bytes=self._cfg_int("cache_max_total_mb", 2048, min_val=0) * 1024 * 1024,
            active_tmp_keys=set(),
        )
        bcfg = self._build_baidu_config()
        self.logger.info(
            "下载中心已启动：上限 %d MB，缓存 %d 小时，百度网盘=%s，清理临时/过期文件 %d 个",
            self._cfg_int("max_file_size_mb", 300, min_val=1),
            self._cfg_int("cache_ttl_hours", 72, min_val=0),
            "已启用" if bcfg.enabled else "未启用",
            stats.tmp_removed + stats.expired_removed + stats.missing_removed,
        )
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(),
            name="download_center_cleanup",
        )

    async def terminate(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None

        for job in list(self._jobs_by_id.values()):
            job.cancel_event.set()
            if job.task is not None and not job.task.done():
                job.task.cancel()

        for task in list(self._background_tasks):
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._background_tasks.clear()

        if self.store is not None:
            self.store.close()
            self.store = None

    @staticmethod
    def _to_bool(val: Any, default: bool) -> bool:
        if val is None:
            return default
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on"}
        return bool(val)

    @staticmethod
    def _to_int(
        val: Any,
        default: int,
        min_val: int | None = None,
        max_val: int | None = None,
    ) -> int:
        try:
            parsed = int(val)
        except (TypeError, ValueError):
            parsed = default
        if min_val is not None:
            parsed = max(min_val, parsed)
        if max_val is not None:
            parsed = min(max_val, parsed)
        return parsed

    @staticmethod
    def _to_str(val: Any, default: str = "") -> str:
        return str(val).strip() if val is not None else default

    def _cfg_bool(self, key: str, default: bool) -> bool:
        return self._to_bool(self.config.get(key, default), default)

    def _cfg_int(
        self,
        key: str,
        default: int,
        min_val: int | None = None,
        max_val: int | None = None,
    ) -> int:
        return self._to_int(self.config.get(key, default), default, min_val=min_val, max_val=max_val)

    def _cfg_str(self, key: str, default: str = "") -> str:
        return self._to_str(self.config.get(key, default), default)

    def _build_baidu_config(self) -> BaiduConfig:
        raw = self.config.get("baidu_pan")
        section = raw if isinstance(raw, dict) or hasattr(raw, "get") else {}
        return BaiduConfig(
            enabled=self._to_bool(section.get("enabled", False), False),
            cookie=self._to_str(section.get("cookie", ""), ""),
            openlist_url=self._to_str(section.get("openlist_url", "http://192.168.10.2:5244"), "http://192.168.10.2:5244"),
            openlist_token=self._to_str(section.get("openlist_token", ""), ""),
            openlist_mount_path=self._to_str(section.get("openlist_mount_path", "/baidu"), "/baidu") or "/baidu",
            temp_dir=self._to_str(section.get("temp_dir", "/bot_tmp"), "/bot_tmp") or "/bot_tmp",
            connections=self._to_int(section.get("connections", 3), 3, min_val=1, max_val=8),
            chunk_size_bytes=self._to_int(section.get("chunk_size_mb", 8), 8, min_val=1, max_val=32) * 1024 * 1024,
            max_pick_files=self._to_int(section.get("max_pick_files", 3), 3, min_val=1, max_val=20),
            max_list_files=self._to_int(section.get("max_list_files", 50), 50, min_val=1, max_val=200),
            max_downloads_per_day=self._to_int(section.get("max_downloads_per_day", 30), 30, min_val=0),
        )

    def _get_openlist_client(self, bcfg: BaiduConfig) -> OpenListClient:
        if self._injected_openlist_client is not None:
            return self._injected_openlist_client
        return OpenListClient(bcfg.openlist_url, bcfg.openlist_token)

    def _build_options(self) -> DownloadOptions:
        allowed_raw = self._cfg_str("allowed_private_hosts", "")
        allowed = tuple(
            item.strip().lower()
            for item in re.split(r"[,;\s]+", allowed_raw)
            if item.strip()
        )
        return DownloadOptions(
            max_bytes=self._cfg_int("max_file_size_mb", 300, min_val=1) * 1024 * 1024,
            connections=self._cfg_int("connections_per_download", 6, min_val=1, max_val=16),
            chunk_size_bytes=self._cfg_int("chunk_size_mb", 4, min_val=1, max_val=32) * 1024 * 1024,
            timeout_sec=self._cfg_int("download_timeout_sec", 600, min_val=10),
            retry_count=self._cfg_int("retry_count", 4, min_val=0, max_val=10),
            block_html_pages=self._cfg_bool("block_html_pages", True),
            block_private_network=self._cfg_bool("block_private_network", True),
            allowed_private_hosts=allowed,
            proxy=self._cfg_str("proxy", ""),
            user_agent=self._cfg_str("user_agent", DEFAULT_USER_AGENT) or DEFAULT_USER_AGENT,
        )

    def _is_session_blocked(self, umo: str) -> bool:
        raw = self._cfg_str("blocked_sessions", "")
        if not raw:
            return False
        blocked = {item.strip() for item in re.split(r"[,;\n]+", raw) if item.strip()}
        return umo in blocked

    async def _cleanup_loop(self) -> None:
        while True:
            interval = self._cfg_int("cleanup_interval_minutes", 30, min_val=1)
            await asyncio.sleep(interval * 60)
            if self.store is None:
                continue
            try:
                async with self._lock:
                    active_keys = set(self._inflight.keys())
                    active_job_ids = set(self._jobs_by_id.keys())
                max_total_bytes = self._cfg_int("cache_max_total_mb", 2048, min_val=0) * 1024 * 1024
                stats = self.store.cleanup(
                    max_total_bytes=max_total_bytes,
                    active_tmp_keys=active_keys,
                )
                if stats.expired_removed or stats.lru_removed or stats.tmp_removed:
                    self.logger.info(
                        "下载缓存清理完成：过期=%d, LRU=%d, 临时=%d, 释放=%s",
                        stats.expired_removed,
                        stats.lru_removed,
                        stats.tmp_removed,
                        format_bytes(stats.freed_bytes),
                    )
                bcfg = self._build_baidu_config()
                if bcfg.enabled and bcfg.openlist_url and bcfg.openlist_token:
                    await self._cleanup_remote_baidu_temp(bcfg, active_job_ids)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("下载缓存定时清理失败：%s", exc)

    async def _cleanup_remote_baidu_temp(
        self,
        bcfg: BaiduConfig,
        active_job_ids: set[str],
    ) -> None:
        ol = self._get_openlist_client(bcfg)
        mount_temp_root = join_openlist_path(bcfg.openlist_mount_path, bcfg.temp_dir)
        async with create_http_session(self._build_options()) as session:
            removed = await ol.cleanup_stale_temp_dirs(session, mount_temp_root, active_job_ids)
            if removed:
                self.logger.info("清理百度网盘过期临时目录 %d 个", removed)

    def _track_task(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _send_text(self, umo: str, text: str) -> None:
        try:
            await self.context.send_message(umo, MessageChain([Plain(text)]))
        except Exception as exc:
            self.logger.warning("发送通知到 %s 失败：%s", umo, exc)

    def _make_notifier(
        self,
        umo: str,
    ) -> tuple[Callable[[str], Awaitable[None]], Callable[[], bool]]:
        notified = False

        async def _notify(text: str) -> None:
            nonlocal notified
            notified = True
            await self._send_text(umo, text)

        return _notify, lambda: notified

    @staticmethod
    async def _emit_and_release(
        notify_fn: Callable[[str], Awaitable[None]] | None,
        msg: str,
        gate: asyncio.Event | None = None,
    ) -> str:
        try:
            if notify_fn is not None:
                await notify_fn(msg)
        finally:
            if gate is not None:
                gate.set()
        return msg

    def _apk_rename_hint(self, filename: str) -> str:
        _, will_rename = normalize_upload_filename(
            filename,
            rename_apk=self._cfg_bool("rename_apk_on_upload", True),
        )
        return (
            "（APK 将自动改名为 .apk.1 上传以防 QQ 拦截，下载后去掉 .1 即可安装）"
            if will_rename
            else ""
        )

    async def _ensure_initialized(self) -> CacheStore:
        if self.store is None or self._semaphore is None:
            await self.initialize()
        assert self.store is not None
        return self.store

    async def submit_download(
        self,
        umo: str,
        url: str,
        custom_filename: str = "",
        force: bool = False,
        password: str = "",
        notify_fn: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        """Validate, deduplicate, and schedule a background download + upload."""
        if not self._cfg_bool("enabled", True):
            return "下载中心插件当前已在配置中关闭。"
        if not umo:
            return "无法识别当前会话，暂不能上传文件。"
        if self._is_session_blocked(umo):
            return "当前会话已被管理员禁用下载功能。"

        clean_url = url.strip()
        clean_name = custom_filename.strip()

        if is_baidu_pan_url(clean_url):
            return await self._submit_baidu_share(
                umo=umo,
                text_or_url=clean_url,
                password=password,
                custom_filename=clean_name,
                force=force,
                notify_fn=notify_fn,
            )

        store = await self._ensure_initialized()
        options = self._build_options()

        await validate_url_and_ssrf(clean_url, options)
        orig_key = url_key(clean_url)

        fast_reply: str | None = None
        fast_gate: asyncio.Event | None = None
        async with self._lock:
            existing_job = self._inflight.get(orig_key)
            if existing_job is not None:
                if umo not in existing_job.waiters:
                    existing_job.waiters[umo] = clean_name
                fast_reply = self._format_inflight_reply(existing_job)
            elif not force and not self._cfg_bool("revalidate_cache_with_head", False):
                cached = store.lookup(clean_url)
                if cached is not None:
                    fast_gate = asyncio.Event() if notify_fn is not None else None
                    fast_reply = self._schedule_cached_upload(
                        umo,
                        cached,
                        clean_name,
                        start_gate=fast_gate,
                    )

        if fast_reply is not None:
            return await self._emit_and_release(notify_fn, fast_reply, fast_gate)

        async with create_http_session(options) as session:
            probe_res = await probe(
                session,
                clean_url,
                options,
                custom_filename=clean_name or None,
            )

        final_key = url_key(probe_res.final_url)

        gate: asyncio.Event | None = None
        reply: str | None = None
        async with self._lock:
            existing_job = self._inflight.get(orig_key) or self._inflight.get(final_key)
            if existing_job is not None:
                if umo not in existing_job.waiters:
                    existing_job.waiters[umo] = clean_name
                reply = self._format_inflight_reply(existing_job)
            elif not force:
                cached = store.lookup(clean_url) or store.lookup(probe_res.final_url)
                if cached is not None and self._is_cache_still_fresh(cached, probe_res):
                    gate = asyncio.Event() if notify_fn is not None else None
                    reply = self._schedule_cached_upload(
                        umo,
                        cached,
                        clean_name,
                        start_gate=gate,
                    )

            if reply is None:
                gate = asyncio.Event() if notify_fn is not None else None
                job_id = uuid.uuid4().hex[:8]
                job = DownloadJob(
                    job_id=job_id,
                    url=clean_url,
                    url_key=orig_key,
                    final_url_key=final_key,
                    filename=probe_res.filename,
                    total_size=probe_res.total_size,
                    waiters={umo: clean_name},
                )
                self._inflight[orig_key] = job
                self._inflight[final_key] = job
                self._jobs_by_id[job_id] = job
                job.task = asyncio.create_task(
                    self._run_download_job(job, options, probe_res, start_gate=gate),
                    name=f"download_job_{job_id}",
                )
                self._track_task(job.task)
                mode_hint = (
                    f"{options.connections} 并发分片"
                    if probe_res.accept_ranges
                    and probe_res.total_size is not None
                    and probe_res.total_size >= options.chunk_size_bytes
                    and options.connections > 1
                    else "单流"
                )
                reply = (
                    f"已开始后台下载「{probe_res.filename}」（{format_bytes(probe_res.total_size)}，"
                    f"{mode_hint}，任务 {job.job_id}），完成后会自动上传{self._apk_rename_hint(probe_res.filename)}。"
                    f"查进度：/dl list，取消：/dl cancel {job.job_id}"
                )

        return await self._emit_and_release(notify_fn, reply, gate)

    def _validate_baidu_ready(self, bcfg: BaiduConfig) -> str | None:
        if not bcfg.enabled:
            return "百度网盘下载尚未开启，请在 AstrBot 插件设置页「下载中心 -> 百度网盘设置」中启用并配置。"
        missing: list[str] = []
        if not bcfg.cookie:
            missing.append("百度网盘 Cookie")
        if not bcfg.openlist_url:
            missing.append("OpenList 地址")
        if not bcfg.openlist_token:
            missing.append("OpenList 令牌")
        if missing:
            return f"百度网盘配置不完整（缺少：{'、'.join(missing)}），请在插件设置页补充后再试。"
        return None

    async def _submit_baidu_share(
        self,
        umo: str,
        text_or_url: str,
        password: str = "",
        custom_filename: str = "",
        force: bool = False,
        notify_fn: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        bcfg = self._build_baidu_config()
        err_msg = self._validate_baidu_ready(bcfg)
        if err_msg:
            return err_msg

        info = extract_baidu_share_info(text_or_url, explicit_pwd=password)
        if info is None:
            return "未识别到有效的百度网盘分享链接。"
        full_surl, short_surl, pwd = info

        await self._ensure_initialized()
        options = self._build_options()
        bd_client = BaiduPanClient(
            bcfg.cookie,
            api_base=self._baidu_api_base,
            user_agent=options.user_agent,
            proxy=options.proxy,
        )

        async with create_http_session(options) as session:
            randsk = (
                await bd_client.verify_pwd(session, full_surl, short_surl, pwd)
                if pwd
                else ""
            )
            meta = await bd_client.fetch_share_metadata(
                session,
                full_surl,
                short_surl,
                randsk=randsk,
            )
            files, truncated = await bd_client.list_share_files(
                session,
                full_surl,
                short_surl,
                randsk=randsk,
                max_files=bcfg.max_list_files,
            )

        if not files:
            return "该百度网盘分享中没有可下载的文件（可能仅包含空文件夹）。"

        if len(files) == 1 and not truncated:
            gate = asyncio.Event() if notify_fn is not None else None
            try:
                reply = await self._schedule_baidu_file(
                    umo=umo,
                    meta=meta,
                    share_file=files[0],
                    bcfg=bcfg,
                    options=options,
                    custom_filename=custom_filename,
                    force=force,
                    start_gate=gate,
                )
            except BaseException:
                if gate is not None:
                    gate.set()
                raise
            return await self._emit_and_release(notify_fn, reply, gate)

        share_session = self._share_sessions.put(umo, meta, files, truncated=truncated)
        lines = [
            f"百度网盘分享包含多个文件（清单编号 {share_session.code}，30 分钟内有效）：",
        ]
        for item in files:
            limit_tag = " ❌超限" if item.size > options.max_bytes else ""
            lines.append(f"{item.index}. {item.rel_path} ({format_bytes(item.size)}){limit_tag}")
        if truncated:
            lines.append(f"（目录文件较多，仅展示前 {len(files)} 个）")
        lines.append(
            f"可发送「下第 X 个」或 /dl pick {share_session.code} <编号>（单次最多 {bcfg.max_pick_files} 个，单文件上限 {format_bytes(options.max_bytes)}）"
        )
        return "\n".join(lines)

    async def pick_baidu_files(
        self,
        umo: str,
        indices_spec: str,
        share_code: str = "",
        force: bool = False,
        notify_fn: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        """Select and start downloading one or more files from a listed Baidu share."""
        if not self._cfg_bool("enabled", True):
            return "下载中心插件当前已在配置中关闭。"
        if not umo:
            return "无法识别当前会话。"
        if self._is_session_blocked(umo):
            return "当前会话已被管理员禁用下载功能。"

        bcfg = self._build_baidu_config()
        err_msg = self._validate_baidu_ready(bcfg)
        if err_msg:
            return err_msg

        share_session = self._share_sessions.get(umo, share_code=share_code)
        if share_session is None:
            hint = f"（编号 {share_code}）" if share_code else ""
            return f"未找到有效的百度分享清单{hint}（清单 30 分钟过期），请重新发送百度网盘链接。"

        indices = parse_pick_indices(
            indices_spec,
            max_index=len(share_session.files),
            max_pick=bcfg.max_pick_files,
        )
        meta = BaiduShareMetadata(
            full_surl=share_session.full_surl,
            short_surl=share_session.short_surl,
            shareid=share_session.shareid,
            uk=share_session.uk,
            bdstoken=share_session.bdstoken,
            randsk=share_session.randsk,
        )
        options = self._build_options()
        batch_gate = asyncio.Event() if notify_fn is not None else None
        replies: list[str] = []
        any_scheduled = False
        try:
            for idx in indices:
                share_file = share_session.get_file(idx)
                if share_file is None:
                    replies.append(f"#{idx} 不存在。")
                    continue
                try:
                    msg = await self._schedule_baidu_file(
                        umo=umo,
                        meta=meta,
                        share_file=share_file,
                        bcfg=bcfg,
                        options=options,
                        force=force,
                        start_gate=batch_gate,
                    )
                    any_scheduled = True
                except DownloadError as exc:
                    msg = f"#{idx}「{share_file.filename}」：{exc}"
                replies.append(msg)

            reply = "\n".join(replies)
            if notify_fn is not None and any_scheduled:
                await notify_fn(reply)
            return reply
        finally:
            if batch_gate is not None:
                batch_gate.set()

    def _reserve_baidu_quota_locked(self, bcfg: BaiduConfig) -> None:
        if bcfg.max_downloads_per_day <= 0:
            return
        today = time.strftime("%Y-%m-%d")
        if self._baidu_daily_date != today:
            self._baidu_daily_date = today
            self._baidu_daily_count = 0
        if self._baidu_daily_count >= bcfg.max_downloads_per_day:
            raise BaiduPanError(
                f"今日百度网盘解析下载次数已达上限（{bcfg.max_downloads_per_day} 次），请明天再试。"
            )
        self._baidu_daily_count += 1

    async def _schedule_baidu_file(
        self,
        umo: str,
        meta: BaiduShareMetadata,
        share_file: BaiduShareFile,
        bcfg: BaiduConfig,
        options: DownloadOptions,
        custom_filename: str = "",
        force: bool = False,
        start_gate: asyncio.Event | None = None,
    ) -> str:
        store = await self._ensure_initialized()
        send_name = sanitize_filename(custom_filename) if custom_filename else sanitize_filename(share_file.filename)
        if share_file.size > options.max_bytes:
            raise BaiduPanError(
                f"「{send_name}」大小为 {format_bytes(share_file.size)}，超过上限 {format_bytes(options.max_bytes)}。"
            )

        ckey = canonical_baidu_key(meta.full_surl, share_file.fs_id)
        ckey_hash = canonical_key_hash(ckey)

        async with self._lock:
            existing_job = self._inflight.get(ckey_hash)
            if existing_job is not None:
                if umo not in existing_job.waiters:
                    existing_job.waiters[umo] = custom_filename
                return self._format_inflight_reply(existing_job)

            if not force:
                cached = store.lookup("", canonical_key=ckey)
                if cached is not None:
                    return self._schedule_cached_upload(
                        umo,
                        cached,
                        custom_filename,
                        start_gate=start_gate,
                    )

            self._reserve_baidu_quota_locked(bcfg)

            job_id = uuid.uuid4().hex[:8]
            job = DownloadJob(
                job_id=job_id,
                url=ckey,
                url_key=ckey_hash,
                final_url_key=ckey_hash,
                filename=send_name,
                total_size=share_file.size,
                waiters={umo: custom_filename},
            )
            self._inflight[ckey_hash] = job
            self._jobs_by_id[job_id] = job
            job.task = asyncio.create_task(
                self._run_baidu_download_job(
                    job,
                    meta,
                    share_file,
                    bcfg,
                    options,
                    start_gate=start_gate,
                ),
                name=f"baidu_download_job_{job_id}",
            )
            self._track_task(job.task)

        return (
            f"已开始后台下载百度网盘「{send_name}」（{format_bytes(share_file.size)}，"
            f"{bcfg.connections} 并发分片，任务 {job.job_id}），完成后会自动上传{self._apk_rename_hint(send_name)}。"
            f"查进度：/dl list，取消：/dl cancel {job.job_id}"
        )

    async def _run_baidu_download_job(
        self,
        job: DownloadJob,
        meta: BaiduShareMetadata,
        share_file: BaiduShareFile,
        bcfg: BaiduConfig,
        options: DownloadOptions,
        start_gate: asyncio.Event | None = None,
    ) -> None:
        assert self.store is not None
        assert self._semaphore is not None
        ttl_hours = self._cfg_int("cache_ttl_hours", 72, min_val=0)
        entry: CacheEntry | None = None
        ol = self._get_openlist_client(bcfg)
        temp_root_baidu = "/" + bcfg.temp_dir.strip("/")
        job_baidu_dir = f"{temp_root_baidu}/{job.job_id}"
        mount_temp_root = join_openlist_path(bcfg.openlist_mount_path, bcfg.temp_dir)
        job_openlist_dir = join_openlist_path(mount_temp_root, job.job_id)
        remote_created = False

        try:
            if start_gate is not None:
                await start_gate.wait()
            async with self._semaphore:
                if job.cancel_event.is_set():
                    raise DownloadCancelledError("下载任务已取消。")

                job.status = "transferring"
                bd_client = BaiduPanClient(
                    bcfg.cookie,
                    api_base=self._baidu_api_base,
                    user_agent=options.user_agent,
                    proxy=options.proxy,
                )
                async with create_http_session(options) as api_session:
                    await ol.mkdir(api_session, job_openlist_dir)
                    remote_created = True
                    await bd_client.transfer_file(
                        api_session,
                        meta,
                        share_file.fs_id,
                        job_baidu_dir,
                    )

                    target_file_name = ""
                    for attempt in range(3):
                        items = await ol.list_dir(api_session, job_openlist_dir, refresh=True)
                        non_dirs = [it for it in items if not it.is_dir and it.name]
                        if non_dirs:
                            exact = next((it for it in non_dirs if it.name == share_file.filename), None)
                            target_file_name = (exact or non_dirs[0]).name
                            break
                        if attempt < 2:
                            await asyncio.sleep(1.0)

                    if not target_file_name:
                        raise OpenListError("转存后在 OpenList 临时目录未找到文件，请检查 OpenList 百度挂载配置。")

                    finfo = await ol.get_file(
                        api_session,
                        join_openlist_path(job_openlist_dir, target_file_name),
                    )

                job.status = "downloading"
                bd_options = DownloadOptions(
                    max_bytes=options.max_bytes,
                    connections=bcfg.connections,
                    chunk_size_bytes=bcfg.chunk_size_bytes,
                    timeout_sec=options.timeout_sec,
                    retry_count=options.retry_count,
                    block_html_pages=options.block_html_pages,
                    block_private_network=options.block_private_network,
                    allowed_private_hosts=options.allowed_private_hosts,
                    proxy=options.proxy,
                    user_agent=options.user_agent,
                    extra_headers=finfo.headers,
                )

                def _on_progress(done: int, total: int | None) -> None:
                    job.downloaded_bytes = done
                    if total is not None:
                        job.total_size = total

                result = await download(
                    url=finfo.raw_url,
                    url_key=job.url_key,
                    tmp_dir=self.store.tmp_dir,
                    options=bd_options,
                    custom_filename=share_file.filename,
                    progress_cb=_on_progress,
                    cancel_event=job.cancel_event,
                    resume_key=job.url,
                )

            job.status = "uploading"
            entry = self.store.store_downloaded(
                original_url=job.url,
                result=result,
                ttl_hours=ttl_hours,
                custom_filename=share_file.filename,
                canonical_key=job.url,
            )
            self.store.pin(entry.sha256)

            async with self._lock:
                waiters = {
                    umo: (sanitize_filename(name) if name else entry.filename)
                    for umo, name in job.waiters.items()
                }
                self._remove_job_locked(job)

            await self._upload_entry_to_waiters(entry, waiters)
            entry = None
        except asyncio.CancelledError:
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"任务 {job.job_id}（{job.filename}）已取消。")
            raise
        except DownloadError as exc:
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"下载百度网盘「{job.filename}」失败：{exc}")
        except Exception as exc:
            self.logger.exception("百度网盘任务 %s 发生未预期错误：%s", job.job_id, exc)
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"下载百度网盘「{job.filename}」出错：{exc}")
        finally:
            if remote_created:
                with suppress(Exception):
                    async with create_http_session(options) as clean_session:
                        await ol.remove(clean_session, mount_temp_root, [job.job_id])
            if entry is not None:
                self.store.unpin(entry.sha256)
            async with self._lock:
                self._remove_job_locked(job)

    @staticmethod
    def _format_inflight_reply(job: DownloadJob) -> str:
        return (
            f"该链接已在下载中（任务 {job.job_id}，「{job.filename}」），"
            f"完成后会自动上传到当前会话。查进度：/dl list，取消：/dl cancel {job.job_id}"
        )

    def _is_cache_still_fresh(self, cached: CacheEntry, probe_res: ProbeResult) -> bool:
        if not self._cfg_bool("revalidate_cache_with_head", False):
            return True
        if probe_res.etag and cached.etag and probe_res.etag != cached.etag:
            return False
        if probe_res.last_modified and cached.last_modified and probe_res.last_modified != cached.last_modified:
            return False
        if probe_res.total_size is not None and probe_res.total_size != cached.file_size:
            return False
        return True

    def _schedule_cached_upload(
        self,
        umo: str,
        entry: CacheEntry,
        custom_filename: str,
        start_gate: asyncio.Event | None = None,
    ) -> str:
        assert self.store is not None
        send_name = sanitize_filename(custom_filename) if custom_filename else entry.filename
        self.store.pin(entry.sha256)
        task = asyncio.create_task(
            self._upload_entry_to_waiters(entry, {umo: send_name}, start_gate=start_gate),
            name=f"upload_cached_{entry.sha256[:8]}",
        )
        self._track_task(task)
        return (
            f"命中本地缓存「{send_name}」（{format_bytes(entry.file_size)}），"
            f"正在直接上传，无需重复下载{self._apk_rename_hint(send_name)}。"
        )

    async def _run_download_job(
        self,
        job: DownloadJob,
        options: DownloadOptions,
        probe_res: ProbeResult,
        start_gate: asyncio.Event | None = None,
    ) -> None:
        assert self.store is not None
        assert self._semaphore is not None
        ttl_hours = self._cfg_int("cache_ttl_hours", 72, min_val=0)
        entry: CacheEntry | None = None

        try:
            if start_gate is not None:
                await start_gate.wait()
            async with self._semaphore:
                if job.cancel_event.is_set():
                    raise DownloadCancelledError("下载任务已取消。")
                job.status = "downloading"

                def _on_progress(done: int, total: int | None) -> None:
                    job.downloaded_bytes = done
                    if total is not None:
                        job.total_size = total

                result = await download(
                    url=job.url,
                    url_key=job.url_key,
                    tmp_dir=self.store.tmp_dir,
                    options=options,
                    probe_result=probe_res,
                    progress_cb=_on_progress,
                    cancel_event=job.cancel_event,
                )

            job.status = "uploading"
            entry = self.store.store_downloaded(
                original_url=job.url,
                result=result,
                ttl_hours=ttl_hours,
            )
            self.store.pin(entry.sha256)

            async with self._lock:
                waiters = {
                    umo: (sanitize_filename(name) if name else entry.filename)
                    for umo, name in job.waiters.items()
                }
                self._remove_job_locked(job)

            await self._upload_entry_to_waiters(entry, waiters)
            entry = None
        except asyncio.CancelledError:
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"任务 {job.job_id}（{job.filename}）已取消。")
            raise
        except DownloadError as exc:
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"下载「{job.filename}」失败：{exc}")
        except Exception as exc:
            self.logger.exception("下载任务 %s 发生未预期错误：%s", job.job_id, exc)
            async with self._lock:
                waiters_umo = list(job.waiters.keys())
                self._remove_job_locked(job)
            for umo in waiters_umo:
                await self._send_text(umo, f"下载「{job.filename}」出错：{exc}")
        finally:
            if entry is not None:
                self.store.unpin(entry.sha256)
            async with self._lock:
                self._remove_job_locked(job)

    def _remove_job_locked(self, job: DownloadJob) -> None:
        if self._inflight.get(job.url_key) is job:
            self._inflight.pop(job.url_key, None)
        if self._inflight.get(job.final_url_key) is job:
            self._inflight.pop(job.final_url_key, None)
        self._jobs_by_id.pop(job.job_id, None)

    async def _upload_entry_to_waiters(
        self,
        entry: CacheEntry,
        waiters: dict[str, str],
        start_gate: asyncio.Event | None = None,
    ) -> None:
        assert self.store is not None
        upload_mode = self._cfg_str("upload_mode", "shared_path")
        rename_apk = self._cfg_bool("rename_apk_on_upload", True)
        try:
            if start_gate is not None:
                await start_gate.wait()
            for umo, send_name in waiters.items():
                try:
                    await upload_file_to_session(
                        self.context,
                        umo=umo,
                        file_path=entry.file_path,
                        filename=send_name,
                        upload_mode=upload_mode,
                        rename_apk=rename_apk,
                        tmp_dir=self.store.tmp_dir,
                    )
                except (UploadError, Exception) as exc:
                    self.logger.warning("上传文件到 %s 失败：%s", umo, exc)
                    await self._send_text(
                        umo,
                        f"「{send_name}」（{format_bytes(entry.file_size)}）已下载到服务器，"
                        f"但上传失败：{exc}。稍后重发同链接可直接秒传，无需重下。",
                    )
        finally:
            self.store.unpin(entry.sha256)

    @filter.llm_tool(name="download_and_send_file")
    async def tool_download_and_send_file(
        self,
        event: AstrMessageEvent,
        url: str,
        filename: str = "",
        force: bool = False,
        password: str = "",
    ) -> str | None:
        """当用户明确请求帮忙下载文件直链或百度网盘分享链接（pan.baidu.com/s/...）并发送到当前聊天时调用。禁止用于 B站、抖音、小红书、微博、YouTube 等社交媒体的视频或图文链接。

        注意：
        - 即使用户此前尝试下载同一链接失败，只要用户再次要求下载或重试，也必须重新调用本工具，不得凭历史对话直接拒绝。
        - 调用本工具时不要提前输出“正在下载”等过渡文字；开始下载后工具会直接向用户发送包含 `/dl` 进度查询与取消指令的通知。

        Args:
            url(string): 要下载的文件直链或百度网盘分享链接（必须是 http 或 https 链接）
            filename(string): 可选自定义保存文件名，留空则自动识别
            force(boolean): 是否忽略本地缓存强制重新下载，默认 false
            password(string): 百度网盘 4 位提取码（若链接或文本中带提取码可一并传入）
        """
        umo = getattr(event, "unified_msg_origin", "")
        notify, was_notified = self._make_notifier(umo)
        try:
            reply = await self.submit_download(
                umo=umo,
                url=url,
                custom_filename=filename,
                force=force,
                password=password,
                notify_fn=notify,
            )
            return None if was_notified() else reply
        except DownloadError as exc:
            return f"无法开始下载：{exc}"
        except Exception as exc:
            self.logger.exception("download_and_send_file 工具执行异常：%s", exc)
            return f"触发下载失败：{exc}"

    @filter.llm_tool(name="pick_baidu_share_files")
    async def tool_pick_baidu_share_files(
        self,
        event: AstrMessageEvent,
        indices: str,
        share_code: str = "",
        force: bool = False,
    ) -> str | None:
        """当此前列出了百度网盘多文件清单，用户回复要下载第几个文件时调用。开始下载后工具会直接向用户发送包含 `/dl` 进度查询与取消指令的通知，调用前无需输出过渡文字。

        Args:
            indices(string): 要下载的文件编号，例如 '2'、'1 3'、'1,2' 或 '1-3'
            share_code(string): 可选 4 位清单编号，留空则自动匹配当前会话最近一次百度网盘清单
            force(boolean): 是否忽略本地缓存强制重新下载，默认 false
        """
        umo = getattr(event, "unified_msg_origin", "")
        notify, was_notified = self._make_notifier(umo)
        try:
            reply = await self.pick_baidu_files(
                umo=umo,
                indices_spec=indices,
                share_code=share_code,
                force=force,
                notify_fn=notify,
            )
            return None if was_notified() else reply
        except DownloadError as exc:
            return f"选号下载失败：{exc}"
        except Exception as exc:
            self.logger.exception("pick_baidu_share_files 工具执行异常：%s", exc)
            return f"选号下载失败：{exc}"

    @filter.command("dl", alias={"下载"})
    async def dl_command(self, event: AstrMessageEvent):
        """下载中心命令入口：/dl <url> | force <url> | pick [短码] <编号> | list | cancel <id> | clean | help"""
        raw = event.get_message_str().strip()
        rest = re.sub(r"^/?(?:dl|下载)\b\s*", "", raw, flags=re.IGNORECASE).strip()

        if not rest or rest.lower() in {"help", "-h", "--help"}:
            yield event.plain_result(self._help_text())
            return

        tokens = rest.split()
        sub = tokens[0].lower()

        if sub in {"list", "status", "ls"}:
            yield event.plain_result(await self._format_status())
            return

        if sub == "cancel":
            if len(tokens) < 2:
                yield event.plain_result("用法：/dl cancel <任务ID>")
                return
            yield event.plain_result(await self._cancel_job(tokens[1]))
            return

        if sub == "pick":
            if len(tokens) < 2:
                yield event.plain_result("用法：/dl pick [清单编号] <文件编号...>，例如 /dl pick 2")
                return
            umo = getattr(event, "unified_msg_origin", "")
            pick_args = tokens[1:]
            share_code = ""
            if len(pick_args) >= 2 and self._share_sessions.get(umo, share_code=pick_args[0]) is not None:
                share_code = pick_args[0]
                pick_args = pick_args[1:]
            notify, was_notified = self._make_notifier(umo)
            reply = ""
            try:
                reply = await self.pick_baidu_files(
                    umo=umo,
                    indices_spec=" ".join(pick_args),
                    share_code=share_code,
                    notify_fn=notify,
                )
            except DownloadError as exc:
                reply = f"选号下载失败：{exc}"
            if not was_notified() and reply:
                yield event.plain_result(reply)
            return

        if sub == "clean":
            store = await self._ensure_initialized()
            purge_all = len(tokens) >= 2 and tokens[1].lower() in {"all", "force"}
            if purge_all and not event.is_admin():
                yield event.plain_result("清空全部缓存仅管理员可执行；普通用户可发送 /dl clean 清理过期文件。")
                return
            async with self._lock:
                active_keys = set(self._inflight.keys())
            max_total_bytes = self._cfg_int("cache_max_total_mb", 2048, min_val=0) * 1024 * 1024
            stats = store.cleanup(
                max_total_bytes=max_total_bytes,
                active_tmp_keys=active_keys,
                purge_all_unpinned=purge_all,
            )
            yield event.plain_result(
                f"清理完成：移除过期/全部={stats.expired_removed}，"
                f"LRU={stats.lru_removed}，临时文件={stats.tmp_removed}，"
                f"释放空间 {format_bytes(stats.freed_bytes)}。"
            )
            return

        force = False
        if sub == "force":
            force = True
            tokens = tokens[1:]
            rest = " ".join(tokens)

        if not tokens:
            yield event.plain_result("用法：/dl [force] <url> [文件名/提取码]")
            return

        umo = getattr(event, "unified_msg_origin", "")
        if is_baidu_pan_url(rest):
            explicit_pwd = ""
            parsed_info = extract_baidu_share_info(rest)
            if parsed_info and not parsed_info[2]:
                for tok in tokens:
                    cleaned_tok = tok.strip()
                    if not is_baidu_pan_url(cleaned_tok) and _PLAIN_PWD_RE.fullmatch(cleaned_tok):
                        explicit_pwd = cleaned_tok
                        break
            notify, was_notified = self._make_notifier(umo)
            reply = ""
            try:
                reply = await self.submit_download(
                    umo=umo,
                    url=rest,
                    force=force,
                    password=explicit_pwd,
                    notify_fn=notify,
                )
            except DownloadError as exc:
                reply = f"无法开始下载：{exc}"
            if not was_notified() and reply:
                yield event.plain_result(reply)
            return

        candidate_url = tokens[0]
        if not URL_RE.match(candidate_url):
            match = URL_RE.search(rest)
            if not match:
                yield event.plain_result("未检测到有效的 http/https 下载链接。\n" + self._help_text())
                return
            candidate_url = match.group(0)
            custom_name = ""
        else:
            custom_name = " ".join(tokens[1:]).strip()

        notify, was_notified = self._make_notifier(umo)
        reply = ""
        try:
            reply = await self.submit_download(
                umo=umo,
                url=candidate_url,
                custom_filename=custom_name,
                force=force,
                notify_fn=notify,
            )
        except DownloadError as exc:
            reply = f"无法开始下载：{exc}"
        if not was_notified() and reply:
            yield event.plain_result(reply)

    async def _cancel_job(self, job_id: str) -> str:
        async with self._lock:
            job = self._jobs_by_id.get(job_id.strip())
            if job is None:
                return f"未找到进行中的任务：{job_id}"
            job.cancel_event.set()
            if job.task is not None and not job.task.done():
                job.task.cancel()
            return f"已发送取消指令给任务 {job.job_id}（{job.filename}）。"

    async def _format_status(self) -> str:
        store = await self._ensure_initialized()
        cstats = store.stats()
        max_mb = self._cfg_int("cache_max_total_mb", 2048, min_val=0)
        lines = [
            f"缓存：{cstats.total_files} 个文件，已用 {format_bytes(cstats.total_bytes)} / {max_mb} MB"
            + (f"（传输锁定 {cstats.pinned_files} 个）" if cstats.pinned_files else ""),
        ]
        async with self._lock:
            jobs = list(self._jobs_by_id.values())
        if not jobs:
            lines.append("当前无进行中的下载任务。")
        else:
            lines.append(f"进行中任务（{len(jobs)}）：")
            for job in jobs:
                if job.total_size:
                    pct = min(100.0, job.downloaded_bytes * 100.0 / job.total_size)
                    prog = f"{format_bytes(job.downloaded_bytes)}/{format_bytes(job.total_size)} ({pct:.1f}%)"
                else:
                    prog = format_bytes(job.downloaded_bytes)
                lines.append(f"- [{job.job_id}] {job.filename} | {job.status} | {prog}")
        return "\n".join(lines)

    def _help_text(self) -> str:
        max_mb = self._cfg_int("max_file_size_mb", 300, min_val=1)
        ttl = self._cfg_int("cache_ttl_hours", 72, min_val=0)
        return (
            "下载中心用法（也可直接聊天让 AI 帮忙下载直链或百度网盘分享）：\n"
            "/dl <链接> [自定义文件名/提取码] - 下载并上传到当前会话\n"
            "/dl force <链接> [文件名] - 忽略缓存强制重下\n"
            "/dl pick [清单编号] <文件编号...> - 从百度网盘多文件清单选号下载\n"
            "/dl list - 查看进行中任务与缓存空间\n"
            "/dl cancel <任务ID> - 取消指定下载任务\n"
            "/dl clean - 清理过期缓存与残留临时文件\n"
            f"当前限制：单文件 ≤ {max_mb} MB，缓存保留 {ttl} 小时。"
        )
