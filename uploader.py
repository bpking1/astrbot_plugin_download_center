"""File upload helpers supporting shared volume paths and NapCat upload_file_stream."""

from __future__ import annotations

import asyncio
import base64
import math
import os
from pathlib import Path
from typing import Any
import uuid

from astrbot.api.event import MessageChain
from astrbot.api.message_components import File
from astrbot.api.star import Context
from astrbot.core.platform.message_session import MessageSesion
from astrbot.core.platform.message_type import MessageType

STREAM_CHUNK_SIZE = 512 * 1024


class UploadError(Exception):
    """Raised when uploading a downloaded file to a chat session fails."""


def parse_umo(umo: str) -> tuple[str, bool, str]:
    """Parse an AstrBot UMO string into (platform_id, is_group, target_id)."""
    session = MessageSesion.from_str(umo)
    is_group = session.message_type == MessageType.GROUP_MESSAGE
    raw_id = session.session_id
    target_id = raw_id.split("_")[-1] if is_group and "_" in raw_id else raw_id
    return session.platform_id, is_group, target_id


def _read_chunk(file_obj: Any, size: int) -> bytes:
    return file_obj.read(size)


async def upload_via_napcat_stream(
    bot: Any,
    is_group: bool,
    target_id: str,
    file_path: Path,
    filename: str,
) -> None:
    """Chunk-upload a file via NapCat upload_file_stream (qq-float style) and send it."""
    if not target_id.isdigit():
        raise UploadError(f"OneBot 会话 ID 不是纯数字：{target_id}")

    file_size = os.path.getsize(file_path)
    total_chunks = max(1, math.ceil(file_size / STREAM_CHUNK_SIZE))
    stream_id = str(uuid.uuid4())

    with open(file_path, "rb") as f:
        for index in range(total_chunks):
            raw_chunk = await asyncio.to_thread(_read_chunk, f, STREAM_CHUNK_SIZE)
            b64_chunk = base64.b64encode(raw_chunk).decode("ascii")
            await bot.call_action(
                "upload_file_stream",
                stream_id=stream_id,
                chunk_data=b64_chunk,
                chunk_index=index,
                total_chunks=total_chunks,
                file_size=file_size,
                filename=filename,
            )

    complete_res = await bot.call_action(
        "upload_file_stream",
        stream_id=stream_id,
        is_complete=True,
    )
    remote_path = complete_res.get("file_path") if isinstance(complete_res, dict) else None
    if not remote_path:
        raise UploadError("NapCat upload_file_stream 未返回合并后的文件路径。")

    segments = [{"type": "file", "data": {"file": remote_path, "name": filename}}]
    numeric_id = int(target_id)
    if is_group:
        await bot.call_action("send_group_msg", group_id=numeric_id, message=segments)
    else:
        await bot.call_action("send_private_msg", user_id=numeric_id, message=segments)


async def upload_file_to_session(
    context: Context,
    umo: str,
    file_path: Path,
    filename: str,
    upload_mode: str = "shared_path",
) -> None:
    """Upload a local file to an AstrBot session using shared_path or napcat_stream."""
    if not file_path.exists():
        raise UploadError(f"待上传文件不存在：{file_path}")

    if upload_mode == "napcat_stream":
        platform_id, is_group, target_id = parse_umo(umo)
        adapter = context.get_platform_inst(platform_id)
        bot = adapter.get_client() if adapter is not None and hasattr(adapter, "get_client") else None
        if bot is not None and hasattr(bot, "call_action"):
            await upload_via_napcat_stream(bot, is_group, target_id, file_path, filename)
            return

    chain = MessageChain([File(name=filename, file=str(file_path))])
    sent = await context.send_message(umo, chain)
    if sent is False:
        raise UploadError(f"未找到目标会话对应的平台适配器：{umo}")
