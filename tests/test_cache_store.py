from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile
import time
import unittest

try:
    from . import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # noqa: F401

from astrbot_plugin_download_center.cache_store import CacheStore, normalize_url, url_key
from astrbot_plugin_download_center.downloader import DownloadResult


class CacheStoreTests(unittest.TestCase):
    def test_url_normalization(self) -> None:
        u1 = "HTTPS://Example.COM:443/files/app.zip?b=2&utm_source=qq&a=1#top"
        u2 = "https://example.com/files/app.zip?a=1&b=2&fbclid=xyz"
        self.assertEqual(normalize_url(u1), normalize_url(u2))
        self.assertEqual(url_key(u1), url_key(u2))

        # Functional token query parameters must be preserved
        u3 = "https://example.com/dl?token=abc"
        u4 = "https://example.com/dl?token=def"
        self.assertNotEqual(url_key(u3), url_key(u4))

    def test_content_dedup_pin_protection_ttl_and_lru(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CacheStore(Path(tmp))
            try:
                content_a = b"same-binary-content" * 100
                sha_a = hashlib.sha256(content_a).hexdigest()

                part1 = store.tmp_dir / "p1.part"
                meta1 = store.tmp_dir / "p1.meta.json"
                part1.write_bytes(content_a)
                meta1.write_text("{}", encoding="utf-8")

                entry1 = store.store_downloaded(
                    "https://cdn1.example.com/pkg.zip",
                    DownloadResult(
                        part_path=part1,
                        meta_path=meta1,
                        final_url="https://edge1.example.com/pkg.zip",
                        filename="pkg.zip",
                        file_size=len(content_a),
                        sha256=sha_a,
                        etag='"e1"',
                        last_modified=None,
                        resumed=False,
                        multipart=False,
                    ),
                    ttl_hours=24,
                )
                self.assertTrue(entry1.file_path.exists())
                self.assertFalse(part1.exists())
                self.assertFalse(meta1.exists())

                # Lookup via both original and final URL
                self.assertIsNotNone(store.lookup("https://cdn1.example.com/pkg.zip?utm_medium=chat"))
                self.assertIsNotNone(store.lookup("https://edge1.example.com/pkg.zip"))

                # Mirror URL with identical SHA256 deduplicates on disk
                part2 = store.tmp_dir / "p2.part"
                meta2 = store.tmp_dir / "p2.meta.json"
                part2.write_bytes(content_a)
                meta2.write_text("{}", encoding="utf-8")
                entry2 = store.store_downloaded(
                    "https://mirror.example.com/pkg-copy.zip",
                    DownloadResult(
                        part_path=part2,
                        meta_path=meta2,
                        final_url="https://mirror.example.com/pkg-copy.zip",
                        filename="pkg-copy.zip",
                        file_size=len(content_a),
                        sha256=sha_a,
                        etag=None,
                        last_modified=None,
                        resumed=False,
                        multipart=False,
                    ),
                    ttl_hours=24,
                )
                self.assertEqual(entry1.file_path, entry2.file_path)
                self.assertEqual(store.stats().total_files, 1)

                # Add a second distinct file, expire first, protect first via pin
                content_b = b"other-binary-content" * 120
                sha_b = hashlib.sha256(content_b).hexdigest()
                part3 = store.tmp_dir / "p3.part"
                meta3 = store.tmp_dir / "p3.meta.json"
                part3.write_bytes(content_b)
                meta3.write_text("{}", encoding="utf-8")
                entry_b = store.store_downloaded(
                    "https://cdn.example.com/second.bin",
                    DownloadResult(
                        part_path=part3,
                        meta_path=meta3,
                        final_url="https://cdn.example.com/second.bin",
                        filename="second.bin",
                        file_size=len(content_b),
                        sha256=sha_b,
                        etag=None,
                        last_modified=None,
                        resumed=False,
                        multipart=False,
                    ),
                    ttl_hours=24,
                )

                # Expire file A while pinned -> cleanup must keep it
                store.pin(sha_a)
                with store._lock:
                    store._conn.execute(
                        "UPDATE files SET expires_at = ? WHERE sha256 = ?",
                        (time.time() - 10, sha_a),
                    )
                    store._conn.commit()
                c1 = store.cleanup(max_total_bytes=10_000_000)
                self.assertEqual(c1.expired_removed, 0)
                self.assertTrue(entry1.file_path.exists())

                # Unpinning an already-expired entry purges it immediately
                purged = store.unpin(sha_a)
                self.assertTrue(purged)
                self.assertFalse(entry1.file_path.exists())

                # Remaining file B exceeds small max_total_bytes -> LRU evicts it
                c2 = store.cleanup(max_total_bytes=100)
                self.assertEqual(c2.lru_removed, 1)
                self.assertFalse(entry_b.file_path.exists())
            finally:
                store.close()
