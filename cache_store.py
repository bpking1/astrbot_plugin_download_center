"""SQLite-backed deduplicated file cache with pin protection and TTL/LRU eviction."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .downloader import DownloadResult, sanitize_filename

TRACKING_QUERY_EXACT = {
    "fbclid",
    "gclid",
    "yclid",
    "mc_cid",
    "mc_eid",
    "spm",
    "vd_source",
    "share_source",
    "isappinstalled",
    "igshid",
}


def normalize_url(url: str) -> str:
    """Normalize a URL for cache/inflight deduplication while keeping functional params."""
    parts = urlsplit(str(url or "").strip())
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    port = parts.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None

    auth = ""
    if parts.username is not None:
        auth = parts.username
        if parts.password is not None:
            auth = f"{auth}:{parts.password}"
        auth = f"{auth}@"

    if ":" in hostname and not hostname.startswith("["):
        host_fmt = f"[{hostname}]"
    else:
        host_fmt = hostname
    netloc = f"{auth}{host_fmt}:{port}" if port is not None else f"{auth}{host_fmt}"
    path = parts.path or "/"

    kept_params = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not (k.lower().startswith("utm_") or k.lower() in TRACKING_QUERY_EXACT)
    ]
    kept_params.sort(key=lambda item: (item[0], item[1]))
    query = urlencode(kept_params, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def url_key(url: str) -> str:
    """Return a stable SHA256 hex digest for a normalized URL."""
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def canonical_key_hash(canonical_key: str) -> str:
    """Return a stable SHA256 hex digest for an arbitrary logical cache key."""
    return hashlib.sha256(str(canonical_key).encode("utf-8")).hexdigest()


@dataclass(slots=True)
class CacheEntry:
    """A cached file record ready for uploading."""

    sha256: str
    file_path: Path
    filename: str
    file_size: int
    etag: str | None
    last_modified: str | None
    created_at: float
    last_accessed_at: float
    expires_at: float


@dataclass(slots=True)
class CleanupStats:
    """Summary of a cache cleanup run."""

    expired_removed: int = 0
    lru_removed: int = 0
    missing_removed: int = 0
    tmp_removed: int = 0
    freed_bytes: int = 0


@dataclass(slots=True)
class CacheStats:
    """Current cache storage statistics."""

    total_files: int
    total_bytes: int
    pinned_files: int


class CacheStore:
    """Manages cached files, URL aliases, content deduplication, and retention policies."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.cache_dir = self.data_dir / "cache"
        self.tmp_dir = self.data_dir / "tmp"
        self.db_path = self.data_dir / "cache.db"

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._pins: dict[str, int] = {}
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    sha256 TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    last_accessed_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS url_aliases (
                    url_key TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    final_url TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    etag TEXT,
                    last_modified TEXT,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (sha256) REFERENCES files(sha256) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_files_expires ON files(expires_at);
                CREATE INDEX IF NOT EXISTS idx_files_accessed ON files(last_accessed_at);
                CREATE INDEX IF NOT EXISTS idx_aliases_sha ON url_aliases(sha256);
                """
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def pin(self, sha256: str) -> None:
        """Increment reference pin count so cleanup will not delete this file while uploading."""
        with self._lock:
            self._pins[sha256] = self._pins.get(sha256, 0) + 1

    def unpin(self, sha256: str) -> bool:
        """Decrement pin count; immediately purge if count reaches 0 and entry is already expired."""
        should_delete_path: Path | None = None
        with self._lock:
            count = self._pins.get(sha256, 0)
            if count <= 1:
                self._pins.pop(sha256, None)
            else:
                self._pins[sha256] = count - 1
                return False

            row = self._conn.execute(
                "SELECT file_path, expires_at FROM files WHERE sha256 = ?",
                (sha256,),
            ).fetchone()
            if row is not None and float(row["expires_at"]) <= time.time():
                should_delete_path = Path(row["file_path"])
                self._conn.execute("DELETE FROM files WHERE sha256 = ?", (sha256,))
                self._conn.commit()

        if should_delete_path is not None:
            self._safe_unlink(should_delete_path)
            return True
        return False

    def is_pinned(self, sha256: str) -> bool:
        with self._lock:
            return self._pins.get(sha256, 0) > 0

    def lookup(self, url: str, canonical_key: str | None = None) -> CacheEntry | None:
        """Look up a non-expired cached file by URL or explicit logical canonical_key."""
        key = canonical_key_hash(canonical_key) if canonical_key else url_key(url)
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT f.sha256, f.file_path, f.filename, f.file_size,
                       f.created_at, f.last_accessed_at, f.expires_at,
                       a.etag, a.last_modified
                FROM url_aliases a
                JOIN files f ON a.sha256 = f.sha256
                WHERE a.url_key = ?
                """,
                (key,),
            ).fetchone()
            if row is None:
                return None

            file_path = Path(row["file_path"])
            if not file_path.exists():
                self._conn.execute("DELETE FROM files WHERE sha256 = ?", (row["sha256"],))
                self._conn.commit()
                return None

            if float(row["expires_at"]) <= now:
                return None

            self._conn.execute(
                "UPDATE files SET last_accessed_at = ? WHERE sha256 = ?",
                (now, row["sha256"]),
            )
            self._conn.commit()

            return CacheEntry(
                sha256=row["sha256"],
                file_path=file_path,
                filename=row["filename"],
                file_size=int(row["file_size"]),
                etag=row["etag"],
                last_modified=row["last_modified"],
                created_at=float(row["created_at"]),
                last_accessed_at=now,
                expires_at=float(row["expires_at"]),
            )

    def store_downloaded(
        self,
        original_url: str,
        result: DownloadResult,
        ttl_hours: int,
        custom_filename: str | None = None,
        canonical_key: str | None = None,
    ) -> CacheEntry:
        """Persist a downloaded .part file into cache with SHA256 content deduplication."""
        now = time.time()
        expires_at = now if ttl_hours <= 0 else now + ttl_hours * 3600
        display_name = (
            sanitize_filename(custom_filename)
            if custom_filename and custom_filename.strip()
            else sanitize_filename(result.filename)
        )

        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM files WHERE sha256 = ?",
                (result.sha256,),
            ).fetchone()

            if existing is not None and Path(existing["file_path"]).exists():
                self._safe_unlink(result.part_path)
                self._safe_unlink(result.meta_path)
                self._safe_unlink(result.meta_path.with_suffix(".meta.json.tmp"))

                target_path = Path(existing["file_path"])
                stored_filename = existing["filename"]
                stored_size = int(existing["file_size"])
                created_at = float(existing["created_at"])
                new_expires = max(float(existing["expires_at"]), expires_at)
                self._conn.execute(
                    "UPDATE files SET last_accessed_at = ?, expires_at = ? WHERE sha256 = ?",
                    (now, new_expires, result.sha256),
                )
            else:
                if existing is not None:
                    self._conn.execute("DELETE FROM files WHERE sha256 = ?", (result.sha256,))

                ext = Path(display_name).suffix
                if len(ext) > 16 or not ext.replace(".", "").isalnum():
                    ext = ""
                target_path = self.cache_dir / f"{result.sha256[:16]}{ext}"
                if target_path.exists():
                    target_path = self.cache_dir / f"{result.sha256}{ext}"

                try:
                    os.replace(result.part_path, target_path)
                except OSError:
                    shutil.move(str(result.part_path), str(target_path))
                self._safe_unlink(result.meta_path)
                self._safe_unlink(result.meta_path.with_suffix(".meta.json.tmp"))

                stored_filename = display_name
                stored_size = result.file_size
                created_at = now
                new_expires = expires_at
                self._conn.execute(
                    """
                    INSERT INTO files (sha256, file_path, filename, file_size, created_at, last_accessed_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        result.sha256,
                        str(target_path),
                        stored_filename,
                        stored_size,
                        created_at,
                        now,
                        new_expires,
                    ),
                )

            if canonical_key:
                alias_targets = [(canonical_key_hash(canonical_key), canonical_key, canonical_key)]
            else:
                alias_targets = [
                    (url_key(raw_url), original_url, result.final_url)
                    for raw_url in {original_url, result.final_url}
                    if raw_url
                ]

            for k, orig_val, final_val in alias_targets:
                self._conn.execute(
                    """
                    INSERT INTO url_aliases (url_key, original_url, final_url, sha256, etag, last_modified, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url_key) DO UPDATE SET
                        original_url = excluded.original_url,
                        final_url = excluded.final_url,
                        sha256 = excluded.sha256,
                        etag = excluded.etag,
                        last_modified = excluded.last_modified,
                        updated_at = excluded.updated_at
                    """,
                    (
                        k,
                        orig_val,
                        final_val,
                        result.sha256,
                        result.etag,
                        result.last_modified,
                        now,
                    ),
                )

            self._conn.commit()

            return CacheEntry(
                sha256=result.sha256,
                file_path=target_path,
                filename=display_name or stored_filename,
                file_size=stored_size,
                etag=result.etag,
                last_modified=result.last_modified,
                created_at=created_at,
                last_accessed_at=now,
                expires_at=new_expires,
            )

    def cleanup(
        self,
        max_total_bytes: int,
        active_tmp_keys: set[str] | None = None,
        tmp_max_age_sec: int = 86400,
        purge_all_unpinned: bool = False,
    ) -> CleanupStats:
        """Clean orphan/stale tmp files, expired entries, and LRU-evict over quota."""
        stats = CleanupStats()
        now = time.time()
        active_keys = active_tmp_keys or set()

        stats.tmp_removed += self._cleanup_tmp_dir(now, active_keys, tmp_max_age_sec)

        to_unlink: list[Path] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT sha256, file_path, file_size, last_accessed_at, expires_at FROM files ORDER BY last_accessed_at ASC"
            ).fetchall()

            kept_bytes = 0
            kept_rows: list[sqlite3.Row] = []

            for row in rows:
                sha = row["sha256"]
                fpath = Path(row["file_path"])
                fsize = int(row["file_size"])
                pinned = self._pins.get(sha, 0) > 0

                if not fpath.exists():
                    if not pinned:
                        self._conn.execute("DELETE FROM files WHERE sha256 = ?", (sha,))
                        stats.missing_removed += 1
                    continue

                if purge_all_unpinned and not pinned:
                    self._conn.execute("DELETE FROM files WHERE sha256 = ?", (sha,))
                    to_unlink.append(fpath)
                    stats.expired_removed += 1
                    stats.freed_bytes += fsize
                    continue

                if float(row["expires_at"]) <= now and not pinned:
                    self._conn.execute("DELETE FROM files WHERE sha256 = ?", (sha,))
                    to_unlink.append(fpath)
                    stats.expired_removed += 1
                    stats.freed_bytes += fsize
                    continue

                kept_bytes += fsize
                kept_rows.append(row)

            if not purge_all_unpinned and kept_bytes > max_total_bytes:
                target_bytes = 0 if max_total_bytes <= 0 else int(max_total_bytes * 0.8)
                for row in kept_rows:
                    if kept_bytes <= target_bytes:
                        break
                    sha = row["sha256"]
                    if self._pins.get(sha, 0) > 0:
                        continue
                    fsize = int(row["file_size"])
                    self._conn.execute("DELETE FROM files WHERE sha256 = ?", (sha,))
                    to_unlink.append(Path(row["file_path"]))
                    kept_bytes -= fsize
                    stats.lru_removed += 1
                    stats.freed_bytes += fsize

            self._conn.commit()

        for p in to_unlink:
            self._safe_unlink(p)

        return stats

    def _cleanup_tmp_dir(
        self,
        now: float,
        active_keys: set[str],
        tmp_max_age_sec: int,
    ) -> int:
        removed = 0
        if not self.tmp_dir.exists():
            return 0

        entries = list(self.tmp_dir.iterdir())
        part_keys: set[str] = set()
        meta_keys: set[str] = set()

        for p in entries:
            name = p.name
            if name.endswith(".part"):
                part_keys.add(name[: -len(".part")])
            elif name.endswith(".meta.json"):
                meta_keys.add(name[: -len(".meta.json")])
            elif name.endswith(".meta.json.tmp"):
                try:
                    if now - p.stat().st_mtime > 300:
                        p.unlink()
                        removed += 1
                except OSError:
                    pass
            elif name.startswith("send_"):
                try:
                    if now - p.stat().st_ctime > 1800:
                        p.unlink()
                        removed += 1
                except OSError:
                    pass

        for key in part_keys ^ meta_keys:
            if key in active_keys:
                continue
            for suffix in (".part", ".meta.json"):
                candidate = self.tmp_dir / f"{key}{suffix}"
                if candidate.exists():
                    self._safe_unlink(candidate)
                    removed += 1

        for key in part_keys & meta_keys:
            if key in active_keys:
                continue
            part_p = self.tmp_dir / f"{key}.part"
            meta_p = self.tmp_dir / f"{key}.meta.json"
            try:
                latest_mtime = max(part_p.stat().st_mtime, meta_p.stat().st_mtime)
            except OSError:
                continue
            if now - latest_mtime > tmp_max_age_sec:
                self._safe_unlink(part_p)
                self._safe_unlink(meta_p)
                removed += 2

        return removed

    def stats(self) -> CacheStats:
        """Return current cache file count, total bytes, and pinned count."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(file_size), 0) AS total FROM files"
            ).fetchone()
            pinned_count = sum(1 for v in self._pins.values() if v > 0)
            return CacheStats(
                total_files=int(row["cnt"]),
                total_bytes=int(row["total"]),
                pinned_files=pinned_count,
            )

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
