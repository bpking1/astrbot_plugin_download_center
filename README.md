# AstrBot 下载中心插件 (`astrbot_plugin_download_center`)

一个轻量自闭环的 AstrBot 文件下载与群/私聊上传插件。支持普通 HTTP/HTTPS 直链与**百度网盘分享**（SVIP 转存 + OpenList 取直链 + 多文件列清单选号），聊天里说一声“帮我下载这个链接”，或直接用 `/dl` 指令，Bot 就会自动校验、多线程分片下载并把文件传回原会话。

## 核心特性

- **自然语言 + 指令双入口**
  - 注册 `download_and_send_file` 与 `pick_baidu_share_files` LLM 工具（兼容 AstrBot `skills_like` 与 `full` 模式），识别聊天中的下载需求、提取码以及多文件选号。
  - 与 `astrbot_plugin_parser` 明确分工：仅下载文件直链及百度网盘分享，不接管 B 站、抖音、小红书、微博、YouTube 等社交视频/图文链接。
  - 提供 `/dl`（别名 `/下载`）兜底命令。
- **纯 `aiohttp` 多协程分片、断点续传、失败重试**
  - 无需外挂 `aria2`，不受容器更新镜像影响，不额外占常驻进程。
  - 探测支持 `Accept-Ranges: bytes` 时启用动态分片池（默认 6 并发 × 4MB/片），通过 `os.pwrite` 偏移写盘，不拖尾；不支持时自动无缝回退单流。
  - `.part` + `.meta.json` 持久化分片进度与 `ETag` / `Last-Modified` / `resume_key`，断网或重启后同校验自动续传。
  - 分片级指数退避重试，遇到 `429` 自动遵循 `Retry-After` 并减半并发。
- **百度网盘下载（SVIP 转存 + OpenList）**
  - 自动提取分享链接与 4 位提取码，支持无码与有码分享。
  - 单文件直接自动转存至隔离临时子目录 → 调 OpenList 刷新并取直链及必需 Header（如 `User-Agent: pan.baidu.com`）→ 低并发分片下载 → 上传后立即删除临时目录。
  - 多文件/文件夹自动展开清单（标记超限项），群友可通过自然语言（如“下第 2 个”）或 `/dl pick` 选号下载。
- **三层防重复下载**
  - **并发合并（Singleflight）**：同一链接（含 302 跳转终链及 `baidu://<surl>/<fs_id>`）正在下载时只下一次，完成后统一分发。
  - **逻辑键与 URL 归一化缓存**：普通链接忽略 `#fragment` 与常见追踪参数，百度网盘按 `(surl, fs_id)` 建缓存（不存短效 `dlink`），命中直接秒传且不消耗转存/每日额度。
  - **SHA256 内容去重**：不同镜像 URL 下载到相同内容时自动合并且只保留一份本地文件。
- **可调大小上限 & 自动缓存治理**
  - 默认单文件上限 **300 MB**（可在 AstrBot 后台随时修改），下载前预检 + 流式读块实时熔断双保险。
  - 默认缓存 **72 小时** + **2048 MB** 总配额（超限按 LRU 淘汰），上传期间引用计数保护绝不误删。
- **双上传模式（参考 `qq-float`）**
  - `shared_path`（默认）：AstrBot 与 NapCat 共享 `/AstrBot/data` 卷时零拷贝直接发路径上传。
  - `napcat_stream`：参考 [qq-float](https://github.com/bpking1/qq-float) 的 512KB 分片 base64 `upload_file_stream` 接口，适配跨机部署。

## 指令列表

- `/dl <链接> [自定义文件名/提取码]`：下载直链或百度网盘分享并上传到当前会话
- `/dl force <链接> [文件名]`：忽略本地缓存强制重新下载
- `/dl pick [清单编号] <文件编号...>`：从百度网盘多文件清单选号下载（如 `/dl pick 2`、`/dl pick 1 3`）
- `/dl list`：查看当前下载进度与缓存空间占用
- `/dl cancel <任务ID>`：取消进行中的任务
- `/dl clean`：清理过期缓存与残留临时文件（`/dl clean all` 仅管理员可用）
- `/dl help`：查看帮助

## 百度网盘配置步骤（可在 AstrBot 插件设置页直接配置）

1. 在 OpenList 中添加 `BaiduNetdisk` 存储（挂载路径如 `/baidu`，根文件夹 `/`），下载接口建议选 `official`。
2. 在 OpenList 后台「设置 → 其他」复制管理员令牌（Token）。
3. 在 AstrBot 后台「插件管理 → 下载中心 → 设置 → 百度网盘设置」中填写并保存：
   - `enabled`：开启百度网盘下载
   - `cookie`：百度网盘 Cookie（至少含 `BDUSS=...; STOKEN=...`）
   - `openlist_url`：OpenList 地址（如 `http://192.168.10.2:5244`）
   - `openlist_token`：OpenList 管理员令牌
   - `openlist_mount_path`：挂载路径（默认 `/baidu`）
   - `temp_dir`：临时转存目录（默认 `/bot_tmp`）
   - 以及并发数（默认 3）、分片大小（默认 8 MB）、单次最多选号数（默认 3）、最多展示数（默认 50）、每日解析次数上限（默认 30）。

## 安装

将本仓库放到 AstrBot 的 `data/plugins/astrbot_plugin_download_center` 目录后重启 AstrBot 即可，无需额外 `pip install`。
