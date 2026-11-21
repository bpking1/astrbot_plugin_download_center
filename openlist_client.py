"""Minimal OpenList HTTP API client for temporary Baidu Netdisk transfer links."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlsplit

import aiohttp

from .downloader import DownloadError


class OpenListError(DownloadError):
    """Raised when OpenList API returns an error or unexpected payload."""


@dataclass(slots=True)
class OpenListFileInfo:
    """Metadata and raw download URL returned by OpenList /api/fs/*."""

    name: str
    size: int
    is_dir: bool
    raw_url: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    modified: float = 0.0


def join_openlist_path(*parts: str) -> str:
    """Join OpenList POSIX paths cleanly with a single leading slash."""
    cleaned = [p.strip("/") for p in parts if p and p.strip("/")]
    return "/" + "/".join(cleaned)


def _parse_modified_ts(raw: Any) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str) and raw.strip():
        s = raw.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _normalize_headers(raw_header: Any, raw_url: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    if isinstance(raw_header, dict):
        for k, v in raw_header.items():
            if not k:
                continue
            if isinstance(v, list):
                if v:
                    headers[str(k)] = str(v[0])
            elif v is not None:
                headers[str(k)] = str(v)

    host = (urlsplit(raw_url).hostname or "").lower()
    if host.endswith("baidupcs.com") or host.endswith("baidu.com"):
        if not any(k.lower() == "user-agent" for k in headers):
            headers["User-Agent"] = "pan.baidu.com"
    return headers


class OpenListClient:
    """Async client for OpenList /api/fs/* endpoints."""

    def __init__(self, base_url: str, token: str, timeout_sec: int = 30) -> None:
        self.base_url = base_url.strip().rstrip("/")
        self.token = token.strip()
        self.timeout_sec = max(5, timeout_sec)

    async def _post_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.base_url or not self.token:
            raise OpenListError("OpenList 地址或 Token 未配置。")
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }
        try:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                proxy=None,
                timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
            ) as resp:
                text = await resp.text(errors="replace")
                try:
                    data = await resp.json(content_type=None)
                except Exception as exc:
                    raise OpenListError(
                        f"OpenList 返回非 JSON（HTTP {resp.status}）：{text[:200]}"
                    ) from exc
        except OpenListError:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            raise OpenListError(f"请求 OpenList 失败：{exc}") from exc

        if not isinstance(data, dict):
            raise OpenListError("OpenList 响应格式异常。")
        code = data.get("code")
        if code != 200:
            msg = data.get("message") or f"code={code}"
            raise OpenListError(f"OpenList 接口错误：{msg}")
        return data

    async def mkdir(self, session: aiohttp.ClientSession, remote_path: str) -> None:
        """Ensure a remote directory exists in OpenList."""
        target = join_openlist_path(remote_path)
        try:
            await self._post_json(session, "/api/fs/mkdir", {"path": target})
        except OpenListError as exc:
            if "exist" not in str(exc).lower() and "已存在" not in str(exc):
                raise

    async def list_dir(
        self,
        session: aiohttp.ClientSession,
        remote_path: str,
        refresh: bool = True,
    ) -> list[OpenListFileInfo]:
        """List files and subdirectories at remote_path."""
        target = join_openlist_path(remote_path)
        data = await self._post_json(
            session,
            "/api/fs/list",
            {
                "path": target,
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": refresh,
            },
        )
        content = (data.get("data") or {}).get("content") or []
        results: list[OpenListFileInfo] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            results.append(
                OpenListFileInfo(
                    name=str(item.get("name", "")),
                    size=int(item.get("size") or 0),
                    is_dir=bool(item.get("is_dir", False)),
                    modified=_parse_modified_ts(item.get("modified")),
                )
            )
        return results

    async def get_file(self, session: aiohttp.ClientSession, remote_path: str) -> OpenListFileInfo:
        """Fetch raw download URL and required HTTP headers for a file."""
        target = join_openlist_path(remote_path)
        data = await self._post_json(
            session,
            "/api/fs/get",
            {"path": target, "password": ""},
        )
        info = data.get("data") or {}
        if not isinstance(info, dict):
            raise OpenListError("OpenList /api/fs/get 返回数据为空。")
        raw_url = str(info.get("raw_url") or "").strip()
        if raw_url.startswith("/"):
            raw_url = urljoin(f"{self.base_url}/", raw_url)
        if not raw_url:
            raise OpenListError("OpenList 未返回文件直链（raw_url 为空）。")
        headers = _normalize_headers(info.get("header") or info.get("headers"), raw_url)
        return OpenListFileInfo(
            name=str(info.get("name") or ""),
            size=int(info.get("size") or 0),
            is_dir=bool(info.get("is_dir", False)),
            raw_url=raw_url,
            headers=headers,
            modified=_parse_modified_ts(info.get("modified")),
        )

    async def remove(
        self,
        session: aiohttp.ClientSession,
        dir_path: str,
        names: list[str],
    ) -> None:
        """Delete files or subdirectories under dir_path."""
        if not names:
            return
        await self._post_json(
            session,
            "/api/fs/remove",
            {"dir": join_openlist_path(dir_path), "names": names},
        )

    async def cleanup_stale_temp_dirs(
        self,
        session: aiohttp.ClientSession,
        temp_mount_dir: str,
        active_names: set[str],
        max_age_sec: int = 7200,
    ) -> int:
        """Remove leftover temporary job directories older than max_age_sec."""
        import time

        target = join_openlist_path(temp_mount_dir)
        try:
            items = await self.list_dir(session, target, refresh=True)
        except OpenListError:
            return 0

        now = time.time()
        stale_names = [
            item.name
            for item in items
            if item.is_dir
            and item.name
            and item.name not in active_names
            and (item.modified <= 0 or (now - item.modified) >= max_age_sec)
        ]
        if not stale_names:
            return 0
        await self.remove(session, target, stale_names)
        return len(stale_names)
