"""AstrBot Download Center plugin: chat-driven file downloader with dedup cache."""

from __future__ import annotations

import asyncio
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

from .cache_store import CacheEntry, CacheStore, url_key
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
from .uploader import UploadError, upload_file_to_session

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


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
    "聊天识别下载需求，支持分片断点续传、去重缓存与自动上传。",
    "1.0.0",
)
class DownloadCenterPlugin(Star):
    """Chat-driven download center with multi-connection resume, dedup cache, and upload."""

    def __init__(
        self,
        context: Context,
        config: Any = None,
        data_dir: Path | None = None,
    ) -> None:
        super().__init__(context)
        self.config: dict[str, Any] = config if isinstance(config, dict) or hasattr(config, "get") else {}
        self._injected_data_dir = Path(data_dir) if data_dir is not None else None
        self.store: CacheStore | None = None
        self._inflight: dict[str, DownloadJob] = {}
        self._jobs_by_id: dict[str, DownloadJob] = {}
        self._lock = asyncio.Lock()
        self._semaphore: asyncio.Semaphore | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def initialize(self) -> None:
        data_dir = self._injected_data_dir or StarTools.get_data_dir("astrbot_plugin_download_center")
        self.store = CacheStore(data_dir)
        max_conc = self._cfg_int("max_concurrent_downloads", 2, min_val=1, max_val=8)
        self._semaphore = asyncio.Semaphore(max_conc)

        stats = self.store.cleanup(
            max_total_bytes=self._cfg_int("cache_max_total_mb", 2048, min_val=0) * 1024 * 1024,
            active_tmp_keys=set(),
        )
        self.logger.info(
            "下载中心已启动：上限 %d MB，缓存 %d 小时，清理临时/过期文件 %d 个",
            self._cfg_int("max_file_size_mb", 300, min_val=1),
            self._cfg_int("cache_ttl_hours", 72, min_val=0),
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

    def _cfg_bool(self, key: str, default: bool) -> bool:
        val = self.config.get(key, default)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on"}
        return bool(val)

    def _cfg_int(
        self,
        key: str,
        default: int,
        min_val: int | None = None,
        max_val: int | None = None,
    ) -> int:
        try:
            val = int(self.config.get(key, default))
        except (TypeError, ValueError):
            val = default
        if min_val is not None:
            val = max(min_val, val)
        if max_val is not None:
            val = min(max_val, val)
        return val

    def _cfg_str(self, key: str, default: str = "") -> str:
        val = self.config.get(key, default)
        return str(val).strip() if val is not None else default

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
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("下载缓存定时清理失败：%s", exc)

    def _track_task(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _send_text(self, umo: str, text: str) -> None:
        try:
            await self.context.send_message(umo, MessageChain([Plain(text)]))
        except Exception as exc:
            self.logger.warning("发送通知到 %s 失败：%s", umo, exc)

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
    ) -> str:
        """Validate, deduplicate, and schedule a background download + upload."""
        if not self._cfg_bool("enabled", True):
            return "下载中心插件当前已在配置中关闭。"
        if not umo:
            return "无法识别当前会话，暂不能上传文件。"
        if self._is_session_blocked(umo):
            return "当前会话已被管理员禁用下载功能。"

        store = await self._ensure_initialized()
        options = self._build_options()
        clean_url = url.strip()
        clean_name = custom_filename.strip()

        await validate_url_and_ssrf(clean_url, options)
        orig_key = url_key(clean_url)

        async with self._lock:
            existing_job = self._inflight.get(orig_key)
            if existing_job is not None:
                if umo not in existing_job.waiters:
                    existing_job.waiters[umo] = clean_name
                return self._format_inflight_reply(existing_job)

            if not force and not self._cfg_bool("revalidate_cache_with_head", False):
                cached = store.lookup(clean_url)
                if cached is not None:
                    return self._schedule_cached_upload(umo, cached, clean_name)

        async with create_http_session(options) as session:
            probe_res = await probe(
                session,
                clean_url,
                options,
                custom_filename=clean_name or None,
            )

        final_key = url_key(probe_res.final_url)

        async with self._lock:
            existing_job = self._inflight.get(orig_key) or self._inflight.get(final_key)
            if existing_job is not None:
                if umo not in existing_job.waiters:
                    existing_job.waiters[umo] = clean_name
                return self._format_inflight_reply(existing_job)

            if not force:
                cached = store.lookup(clean_url) or store.lookup(probe_res.final_url)
                if cached is not None and self._is_cache_still_fresh(cached, probe_res):
                    return self._schedule_cached_upload(umo, cached, clean_name)

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
                self._run_download_job(job, options, probe_res),
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
        return (
            f"已开始后台下载「{probe_res.filename}」（{format_bytes(probe_res.total_size)}，"
            f"{mode_hint}，任务 {job.job_id}），完成后会自动上传。"
            f"查进度：/dl list，取消：/dl cancel {job.job_id}"
        )

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

    def _schedule_cached_upload(self, umo: str, entry: CacheEntry, custom_filename: str) -> str:
        assert self.store is not None
        send_name = sanitize_filename(custom_filename) if custom_filename else entry.filename
        self.store.pin(entry.sha256)
        task = asyncio.create_task(
            self._upload_entry_to_waiters(entry, {umo: send_name}),
            name=f"upload_cached_{entry.sha256[:8]}",
        )
        self._track_task(task)
        return f"命中本地缓存「{send_name}」（{format_bytes(entry.file_size)}），正在直接上传，无需重复下载。"

    async def _run_download_job(
        self,
        job: DownloadJob,
        options: DownloadOptions,
        probe_res: ProbeResult,
    ) -> None:
        assert self.store is not None
        assert self._semaphore is not None
        ttl_hours = self._cfg_int("cache_ttl_hours", 72, min_val=0)
        entry: CacheEntry | None = None

        try:
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
            entry = None  # unpinned inside _upload_entry_to_waiters
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
    ) -> None:
        assert self.store is not None
        upload_mode = self._cfg_str("upload_mode", "shared_path")
        try:
            for umo, send_name in waiters.items():
                try:
                    await upload_file_to_session(
                        self.context,
                        umo=umo,
                        file_path=entry.file_path,
                        filename=send_name,
                        upload_mode=upload_mode,
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
    ) -> str:
        """当用户明确请求帮忙下载某个文件直链/安装包/压缩包/文档并发送到当前聊天时调用。禁止用于 B站、抖音、小红书、微博、YouTube 等社交媒体的视频或图文链接。

        Args:
            url(string): 要下载的文件直链（必须是 http 或 https 链接）
            filename(string): 可选自定义保存文件名，留空则自动识别
            force(boolean): 是否忽略本地缓存强制重新下载，默认 false
        """
        umo = getattr(event, "unified_msg_origin", "")
        try:
            return await self.submit_download(
                umo=umo,
                url=url,
                custom_filename=filename,
                force=force,
            )
        except DownloadError as exc:
            return f"无法开始下载：{exc}"
        except Exception as exc:
            self.logger.exception("download_and_send_file 工具执行异常：%s", exc)
            return f"触发下载失败：{exc}"

    @filter.command("dl", alias={"下载"})
    async def dl_command(self, event: AstrMessageEvent):
        """下载中心命令入口：/dl <url> | force <url> | list | cancel <id> | clean | help"""
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

        if not tokens:
            yield event.plain_result("用法：/dl [force] <url> [文件名]")
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

        umo = getattr(event, "unified_msg_origin", "")
        try:
            reply = await self.submit_download(
                umo=umo,
                url=candidate_url,
                custom_filename=custom_name,
                force=force,
            )
        except DownloadError as exc:
            reply = f"无法开始下载：{exc}"
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
            "下载中心用法（也可直接聊天让 AI 帮忙下载直链）：\n"
            "/dl <链接> [自定义文件名] - 下载并上传到当前会话\n"
            "/dl force <链接> [文件名] - 忽略缓存强制重下\n"
            "/dl list - 查看进行中任务与缓存空间\n"
            "/dl cancel <任务ID> - 取消指定下载任务\n"
            "/dl clean - 清理过期缓存与残留临时文件\n"
            f"当前限制：单文件 ≤ {max_mb} MB，缓存保留 {ttl} 小时。"
        )
