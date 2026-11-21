# AstrBot 下载中心插件 (`astrbot_plugin_download_center`)

一个轻量自闭环的 AstrBot 文件下载与群/私聊上传插件。聊天里说一声“帮我下载这个链接”，或直接用 `/dl` 指令，Bot 就会自动校验、多线程分片下载并把文件传回原会话。

## 核心特性

- **自然语言 + 指令双入口**
  - 注册 `download_and_send_file` LLM Tool（兼容 AstrBot `skills_like` 与 `full` 模式），在聊天中识别下载意图并自动提取上下文/引用消息里的链接。
  - 与 `astrbot_plugin_parser` 明确分工：仅下载文件直链，不接管 B 站、抖音、小红书、微博、YouTube 等社交视频/图文链接。
  - 提供 `/dl`（别名 `/下载`）兜底命令。
- **纯 `aiohttp` 多协程分片、断点续传、失败重试**
  - 无需外挂 `aria2`，不受容器更新镜像影响，不额外占常驻进程。
  - 探测支持 `Accept-Ranges: bytes` 时启用动态分片池（默认 6 并发 × 4MB/片），通过 `os.pwrite` 偏移写盘，不拖尾；不支持时自动无缝回退单流。
  - `.part` + `.meta.json` 持久化分片进度与 `ETag` / `Last-Modified`，断网或重启后同校验自动续传。
  - 分片级指数退避重试，遇到 `429` 自动遵循 `Retry-After` 并减半并发。
- **三层防重复下载**
  - **并发合并（Singleflight）**：同一链接（含 302 跳转终链）正在下载时，后续请求直接挂入等待列表，仅下载一次，完成后统一分发。
  - **URL 归一化缓存**：去除 `#fragment` 与常见追踪参数（`utm_*` 等），SQLite 记录有效缓存直接秒传。
  - **SHA256 内容去重**：不同镜像 URL 下载到相同内容时自动合并且只保留一份本地文件。
- **可调大小上限 & 自动缓存治理**
  - 默认单文件上限 **300 MB**（可在 AstrBot 后台随时修改），下载前 `HEAD`/`Range 0-0` 预检 + 流式读块实时熔断双保险。
  - 默认缓存 **72 小时** + **2048 MB** 总配额（超限按 LRU 淘汰），上传期间引用计数保护绝不误删。
- **双上传模式（参考 `qq-float`）**
  - `shared_path`（默认）：AstrBot 与 NapCat 共享 `/AstrBot/data` 卷时零拷贝直接发路径上传。
  - `napcat_stream`：参考 [qq-float](https://github.com/bpking1/qq-float) 的 512KB 分片 base64 `upload_file_stream` 接口，适配跨机部署。
- **安全防护**
  - 拦截明显 HTML 登录/错误页，防止把网页传到群里。
  - 默认禁止访问内网/回环地址（含 302 重定向每一跳校验），支持配置白名单。
  - 代理字段默认留空，直接适配透明代理环境。

## 指令列表

- `/dl <链接> [自定义文件名]`：下载文件并上传到当前群/私聊
- `/dl force <链接> [自定义文件名]`：忽略本地缓存强制重新下载
- `/dl list`：查看当前下载进度与缓存空间占用
- `/dl cancel <任务ID>`：取消进行中的任务
- `/dl clean`：清理过期缓存与残留临时文件（`/dl clean all` 仅管理员可用）
- `/dl help`：查看帮助

## 安装

将本仓库放到 AstrBot 的 `data/plugins/astrbot_plugin_download_center` 目录后重启 AstrBot 即可，无需额外 `pip install`。
