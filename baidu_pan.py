"""Baidu Netdisk share parser, file tree listing, SVIP transfer, and multi-file session state."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
import time
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit
import uuid

import aiohttp

from .downloader import DEFAULT_USER_AGENT, DownloadError

BAIDU_WEB_API_BASE = "https://pan.baidu.com"
SHARE_SESSION_TTL_SECONDS = 1800

_BAIDU_URL_RE = re.compile(
    r"https?://(?:pan|yun)\.baidu\.com/[^\s<>\"'，。！？\]）]+",
    re.IGNORECASE,
)
_PWD_IN_TEXT_RE = re.compile(
    r"(?:提取码|密码|访问码|pwd)\s*[:：=]?\s*([A-Za-z0-9]{4})\b",
    re.IGNORECASE,
)


class BaiduPanError(DownloadError):
    """Base error for Baidu Netdisk share inspection or transfer."""


class BaiduPasswordRequiredError(BaiduPanError):
    """Raised when a Baidu share requires a 4-character extraction code."""


@dataclass(slots=True)
class BaiduConfig:
    """Plugin settings for Baidu Netdisk integration."""

    enabled: bool = False
    cookie: str = ""
    openlist_url: str = "http://192.168.10.2:5244"
    openlist_token: str = ""
    openlist_mount_path: str = "/baidu"
    temp_dir: str = "/bot_tmp"
    connections: int = 3
    chunk_size_bytes: int = 8 * 1024 * 1024
    max_pick_files: int = 3
    max_list_files: int = 50
    max_downloads_per_day: int = 30


@dataclass(slots=True)
class BaiduShareFile:
    """A downloadable file inside a Baidu Netdisk share."""

    index: int
    fs_id: int
    filename: str
    rel_path: str
    size: int
    md5: str = ""


@dataclass(slots=True)
class BaiduShareMetadata:
    """Share-level identifiers required by Baidu transfer APIs."""

    full_surl: str
    short_surl: str
    shareid: str
    uk: str
    bdstoken: str
    randsk: str = ""


@dataclass(slots=True)
class BaiduShareSession:
    """Temporary session holding a listed multi-file share for user selection."""

    code: str
    umo: str
    full_surl: str
    short_surl: str
    shareid: str
    uk: str
    bdstoken: str
    randsk: str
    files: list[BaiduShareFile]
    truncated: bool = False
    created_at: float = field(default_factory=time.time)

    def get_file(self, index: int) -> BaiduShareFile | None:
        for item in self.files:
            if item.index == index:
                return item
        return None


def canonical_baidu_key(full_surl: str, fs_id: int) -> str:
    """Build a stable cache/inflight key independent of ephemeral OpenList dlinks."""
    return f"baidu://{full_surl}/{fs_id}"


def is_baidu_pan_url(text_or_url: str) -> bool:
    """Return True if the input contains a Baidu Netdisk share link."""
    return bool(_BAIDU_URL_RE.search(str(text_or_url or "")))


def _canonical_surl_pair(raw_surl: str) -> tuple[str, str]:
    token = raw_surl.strip().split("?", 1)[0].split("#", 1)[0].strip("/")
    if not token:
        raise BaiduPanError("无法从链接中识别百度网盘分享标识（surl）。")
    if token.startswith("1") and len(token) > 1:
        return token, token[1:]
    return f"1{token}", token


def extract_baidu_share_info(
    text_or_url: str,
    explicit_pwd: str = "",
) -> tuple[str, str, str] | None:
    """Extract (full_surl, short_surl, password) from a share URL or text snippet."""
    raw = str(text_or_url or "").strip()
    match = _BAIDU_URL_RE.search(raw)
    if not match:
        return None

    url = match.group(0).rstrip(".,;!?\"'）)】]")
    parsed = urlsplit(url)
    qs = parse_qs(parsed.query)
    path = parsed.path.rstrip("/")

    raw_surl = ""
    if "/s/" in path:
        raw_surl = path.split("/s/", 1)[1].split("/", 1)[0]
    elif qs.get("surl"):
        raw_surl = qs["surl"][0]

    if not raw_surl:
        raise BaiduPanError("仅支持百度网盘分享链接（如 pan.baidu.com/s/1xxx）。")

    full_surl, short_surl = _canonical_surl_pair(raw_surl)

    pwd = explicit_pwd.strip()
    if not pwd and qs.get("pwd"):
        pwd = qs["pwd"][0].strip()
    if not pwd:
        pwd_match = _PWD_IN_TEXT_RE.search(raw)
        if pwd_match:
            pwd = pwd_match.group(1).strip()

    return full_surl, short_surl, pwd


def parse_pick_indices(spec: str, max_index: int, max_pick: int) -> list[int]:
    """Parse user file selection like '2', '1 3', '1,2', or '1-3'."""
    cleaned = str(spec or "").strip()
    if not cleaned:
        raise BaiduPanError("请指定要下载的文件编号，例如：/dl pick 1 或 /dl pick 1 2")

    selected: list[int] = []
    seen: set[int] = set()
    for part in re.split(r"[,，\s]+", cleaned):
        if not part:
            continue
        range_match = re.fullmatch(r"(\d+)-(\d+)", part)
        if range_match:
            lo, hi = int(range_match.group(1)), int(range_match.group(2))
            if lo > hi or lo < 1 or hi > max_index:
                raise BaiduPanError(f"编号范围 {part} 超出清单范围（1~{max_index}）。")
            for idx in range(lo, hi + 1):
                if idx not in seen:
                    seen.add(idx)
                    selected.append(idx)
            continue

        if not part.isdigit():
            raise BaiduPanError(f"无法识别的编号「{part}」，请填写数字编号（1~{max_index}）。")
        idx = int(part)
        if idx < 1 or idx > max_index:
            raise BaiduPanError(f"编号 {idx} 超出清单范围（1~{max_index}）。")
        if idx not in seen:
            seen.add(idx)
            selected.append(idx)

    if not selected:
        raise BaiduPanError("未选择任何有效文件编号。")
    if max_pick > 0 and len(selected) > max_pick:
        raise BaiduPanError(f"单次最多同时选择 {max_pick} 个文件，请分批下载。")
    return selected


def _merge_cookie_header(cookie_str: str, randsk: str = "") -> str:
    pairs: dict[str, str] = {}
    order: list[str] = []
    for item in str(cookie_str or "").split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            continue
        if k not in pairs:
            order.append(k)
        pairs[k] = v.strip()

    if randsk:
        if "BDCLND" not in pairs:
            order.append("BDCLND")
        pairs["BDCLND"] = randsk

    return "; ".join(f"{k}={pairs[k]}" for k in order)


def _translate_baidu_errno(errno: int, context: str = "") -> str:
    if errno == -6:
        return "百度网盘 Cookie 已过期或未登录，请在插件设置中重新填写 Cookie。"
    if errno in {-7, -9}:
        return "提取码错误或分享链接已失效。" if "verify" in context else "分享链接不存在或已失效。"
    if errno in {-12, 115}:
        return "提取码无效、过期或该文件已被分享者取消分享。"
    if errno in {-62, 9019, 132}:
        return "触发百度安全验证码或风控校验，已中止操作（请稍后再试或手动在网盘转存）。"
    if errno == 12:
        return "转存失败（目标目录已存在同名文件或转存数量超限）。"
    if errno == 111:
        return "百度网盘剩余空间不足或单次转存超出账号限制。"
    if errno == -8:
        return "文件已存在。"
    return f"百度网盘接口返回错误码 {errno}。"


def _extract_int_or_str_field(html: str, keys: tuple[str, ...]) -> str:
    for key in keys:
        patterns = [
            rf'"{re.escape(key)}"\s*:\s*"([^"]+)"',
            rf'"{re.escape(key)}"\s*:\s*(\d+)',
            rf"{re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]",
            rf"{re.escape(key)}\s*=\s*(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, html)
            if m and m.group(1):
                return m.group(1).strip()
    return ""


class BaiduPanClient:
    """Async client for Baidu Netdisk share verification, listing, and SVIP transfer."""

    def __init__(
        self,
        cookie_str: str,
        api_base: str = BAIDU_WEB_API_BASE,
        user_agent: str = DEFAULT_USER_AGENT,
        proxy: str = "",
        timeout_sec: int = 30,
    ) -> None:
        self.cookie_str = cookie_str.strip()
        self.api_base = api_base.rstrip("/")
        self.user_agent = user_agent
        self.proxy = proxy.strip()
        self.timeout_sec = max(5, timeout_sec)

    def _headers(self, referer: str, randsk: str = "") -> dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Referer": referer,
        }
        cookie = _merge_cookie_header(self.cookie_str, randsk=randsk)
        if cookie:
            headers["Cookie"] = cookie
        return headers

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        referer: str,
        randsk: str = "",
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        headers = self._headers(referer=referer, randsk=randsk)
        if extra_headers:
            headers.update(extra_headers)
        try:
            async with session.request(
                method,
                url,
                data=data,
                headers=headers,
                proxy=self.proxy or None,
                timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
            ) as resp:
                text = await resp.text(errors="replace")
                try:
                    payload = json.loads(text)
                except Exception as exc:
                    raise BaiduPanError(
                        f"百度网盘接口返回非 JSON（HTTP {resp.status}）：{text[:200]}"
                    ) from exc
        except BaiduPanError:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            raise BaiduPanError(f"请求百度网盘失败：{exc}") from exc

        if not isinstance(payload, dict):
            raise BaiduPanError("百度网盘响应格式异常。")
        return payload

    async def verify_pwd(
        self,
        session: aiohttp.ClientSession,
        full_surl: str,
        short_surl: str,
        pwd: str,
    ) -> str:
        """Verify extraction code and obtain BDCLND randsk token."""
        ts = int(time.time() * 1000)
        url = (
            f"{self.api_base}/share/verify"
            f"?surl={quote(short_surl)}&t={ts}&channel=chunlei&web=1&app_id=250528&clienttype=0"
        )
        referer = f"{self.api_base}/share/init?surl={quote(short_surl)}"
        payload = await self._request_json(
            session,
            "POST",
            url,
            referer=referer,
            data={"pwd": pwd, "vcode": "", "vcode_str": ""},
            extra_headers={"X-Requested-With": "XMLHttpRequest"},
        )
        errno = int(payload.get("errno", -1))
        if errno != 0:
            raise BaiduPanError(_translate_baidu_errno(errno, context="verify"))
        randsk = str(payload.get("randsk") or "").strip()
        if not randsk:
            raise BaiduPanError("验证提取码成功但未获取到分享凭证（randsk）。")
        return randsk

    async def fetch_share_metadata(
        self,
        session: aiohttp.ClientSession,
        full_surl: str,
        short_surl: str,
        randsk: str = "",
    ) -> BaiduShareMetadata:
        """Fetch shareid, uk, and bdstoken from share page + template variable fallback."""
        share_url = f"{self.api_base}/s/{quote(full_surl)}"
        headers = self._headers(referer=f"{self.api_base}/", randsk=randsk)

        try:
            async with session.get(
                share_url,
                headers=headers,
                proxy=self.proxy or None,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
            ) as resp:
                if resp.status in {301, 302, 303, 307, 308}:
                    location = resp.headers.get("Location", "")
                    if "/share/init" in location or "/wap/init" in location:
                        if not randsk:
                            raise BaiduPasswordRequiredError(
                                "该百度网盘分享需要提取码，请在消息或链接中附带 4 位提取码后重试。"
                            )
                        raise BaiduPanError("提取码无效或分享链接已过期。")
                html = await resp.text(errors="replace")
        except (BaiduPanError, BaiduPasswordRequiredError):
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            raise BaiduPanError(f"读取百度分享页面失败：{exc}") from exc

        if "/share/init" in html and "shareid" not in html and not randsk:
            raise BaiduPasswordRequiredError(
                "该百度网盘分享需要提取码，请在消息或链接中附带 4 位提取码后重试。"
            )

        shareid = _extract_int_or_str_field(html, ("shareid", "SHARE_ID", "share_id"))
        uk = _extract_int_or_str_field(html, ("uk", "share_uk", "SHARE_UK"))
        bdstoken = _extract_int_or_str_field(html, ("bdstoken", "MYBDSTOKEN"))

        if not bdstoken:
            tv_url = (
                f"{self.api_base}/api/gettemplatevariable"
                "?fields=%5B%22bdstoken%22%2C%22username%22%5D&channel=chunlei&web=1&app_id=250528&clienttype=0"
            )
            tv = await self._request_json(session, "GET", tv_url, referer=share_url, randsk=randsk)
            errno = int(tv.get("errno", 0))
            if errno != 0:
                raise BaiduPanError(_translate_baidu_errno(errno, context="bdstoken"))
            result = tv.get("result") or {}
            if isinstance(result, dict):
                bdstoken = str(result.get("bdstoken") or "").strip()

        if not bdstoken:
            raise BaiduPanError("百度网盘 Cookie 已过期或未登录（无法获取 bdstoken），请更新设置中的 Cookie。")
        if not shareid or not uk:
            raise BaiduPanError("未能解析分享信息（分享可能已取消、失效或页面结构变更）。")

        return BaiduShareMetadata(
            full_surl=full_surl,
            short_surl=short_surl,
            shareid=shareid,
            uk=uk,
            bdstoken=bdstoken,
            randsk=randsk,
        )

    async def list_share_files(
        self,
        session: aiohttp.ClientSession,
        full_surl: str,
        short_surl: str,
        randsk: str = "",
        max_depth: int = 3,
        max_files: int = 50,
    ) -> tuple[list[BaiduShareFile], bool]:
        """Recursively list downloadable files inside a share up to max_depth/max_files."""
        referer = f"{self.api_base}/s/{quote(full_surl)}"
        collected: list[BaiduShareFile] = []
        truncated = False

        async def _walk(remote_dir: str | None, depth: int, prefix: str) -> None:
            nonlocal truncated
            if truncated:
                return
            page = 1
            while not truncated:
                if remote_dir is None:
                    url = (
                        f"{self.api_base}/share/list?shorturl={quote(short_surl)}"
                        f"&root=1&page={page}&num=100&channel=chunlei&web=1&app_id=250528&clienttype=0"
                    )
                else:
                    url = (
                        f"{self.api_base}/share/list?shorturl={quote(short_surl)}"
                        f"&root=0&dir={quote(remote_dir)}&page={page}&num=100"
                        "&channel=chunlei&web=1&app_id=250528&clienttype=0"
                    )
                payload = await self._request_json(session, "GET", url, referer=referer, randsk=randsk)
                errno = int(payload.get("errno", -1))
                if errno != 0:
                    raise BaiduPanError(_translate_baidu_errno(errno, context="list"))

                items = payload.get("list") or []
                if not items:
                    break

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    is_dir = int(item.get("isdir", 0)) == 1
                    name = str(item.get("server_filename") or "")
                    item_path = str(item.get("path") or "")
                    rel = f"{prefix}/{name}" if prefix else name
                    if is_dir:
                        if depth < max_depth and item_path:
                            await _walk(item_path, depth + 1, rel)
                        else:
                            truncated = True
                    else:
                        if len(collected) >= max_files:
                            truncated = True
                            break
                        collected.append(
                            BaiduShareFile(
                                index=len(collected) + 1,
                                fs_id=int(item["fs_id"]),
                                filename=name,
                                rel_path=rel,
                                size=int(item.get("size") or 0),
                                md5=str(item.get("md5") or ""),
                            )
                        )
                if len(items) < 100:
                    break
                page += 1

        await _walk(None, 0, "")
        return collected, truncated

    async def transfer_file(
        self,
        session: aiohttp.ClientSession,
        meta: BaiduShareMetadata,
        fs_id: int,
        dest_dir: str,
    ) -> None:
        """Transfer a single file from a share into the user's SVIP temp directory."""
        url = (
            f"{self.api_base}/share/transfer"
            f"?shareid={quote(meta.shareid)}&from={quote(meta.uk)}"
            f"&bdstoken={quote(meta.bdstoken)}&channel=chunlei&web=1&app_id=250528&clienttype=0"
        )
        referer = f"{self.api_base}/s/{quote(meta.full_surl)}"
        payload = await self._request_json(
            session,
            "POST",
            url,
            referer=referer,
            randsk=meta.randsk,
            data={"fsidlist": f"[{int(fs_id)}]", "path": dest_dir},
            extra_headers={"X-Requested-With": "XMLHttpRequest"},
        )
        errno = int(payload.get("errno", -1))
        if errno != 0:
            raise BaiduPanError(_translate_baidu_errno(errno, context="transfer"))


class ShareSessionStore:
    """Ephemeral in-memory store for multi-file Baidu share listings."""

    def __init__(self, ttl_sec: int = SHARE_SESSION_TTL_SECONDS) -> None:
        self.ttl_sec = ttl_sec
        self._by_code: dict[str, BaiduShareSession] = {}
        self._latest_by_umo: dict[str, str] = {}

    def put(
        self,
        umo: str,
        meta: BaiduShareMetadata,
        files: list[BaiduShareFile],
        truncated: bool = False,
    ) -> BaiduShareSession:
        self.prune()
        code = uuid.uuid4().hex[:4]
        while code in self._by_code:
            code = uuid.uuid4().hex[:4]
        session = BaiduShareSession(
            code=code,
            umo=umo,
            full_surl=meta.full_surl,
            short_surl=meta.short_surl,
            shareid=meta.shareid,
            uk=meta.uk,
            bdstoken=meta.bdstoken,
            randsk=meta.randsk,
            files=files,
            truncated=truncated,
        )
        self._by_code[code] = session
        self._latest_by_umo[umo] = code
        return session

    def get(self, umo: str, share_code: str = "") -> BaiduShareSession | None:
        self.prune()
        code = share_code.strip().lower()
        if not code:
            code = self._latest_by_umo.get(umo, "")
        if not code:
            return None
        return self._by_code.get(code)

    def prune(self) -> None:
        now = time.time()
        expired = [k for k, v in self._by_code.items() if now - v.created_at > self.ttl_sec]
        for k in expired:
            self._by_code.pop(k, None)
        stale_umos = [u for u, c in self._latest_by_umo.items() if c not in self._by_code]
        for u in stale_umos:
            self._latest_by_umo.pop(u, None)
