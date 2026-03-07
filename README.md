# iOS-Headers 本地构建与数据流程

本项目当前使用 **v2 构建流程**（ClickHouse + Redis + MinIO），并提供首期站点（Flask + Jinja2 + Nginx）。

## 服务栈（OrbStack / Docker Compose）

- ClickHouse：元数据与符号索引
- MinIO：头文件正文对象存储
- Redis：热点缓存
- Web（Flask + Jinja2）：SSR 页面渲染（禁用前端 JavaScript）
- Nginx：站点入口与反向代理（仅 Nginx 对宿主机暴露端口）

### 快速启动

1. 复制环境变量文件：
  - `cp .env.example .env`
2. 启动服务：
   - `scripts/deploy_local_stack.zsh up`
3. 健康检查：
   - `scripts/deploy_local_stack.zsh check`
4. 打开站点：
  - `http://127.0.0.1:18080`（可通过 `.env` 的 `WEB_PORT` 调整）

### 常用命令

- 启动：`scripts/deploy_local_stack.zsh up`
- 停止：`scripts/deploy_local_stack.zsh down`
- 重启：`scripts/deploy_local_stack.zsh restart`
- 状态：`scripts/deploy_local_stack.zsh status`
- 日志：`scripts/deploy_local_stack.zsh logs`
  - 单服务：`scripts/deploy_local_stack.zsh logs clickhouse`
- 启动内网穿透客户端：`scripts/deploy_local_stack.zsh tunnel-up`
- 停止内网穿透客户端：`scripts/deploy_local_stack.zsh tunnel-down`
- 重启内网穿透客户端：`scripts/deploy_local_stack.zsh tunnel-restart`
- 查看内网穿透状态：`scripts/deploy_local_stack.zsh tunnel-status`
- 查看内网穿透日志：`scripts/deploy_local_stack.zsh tunnel-logs`
- 重建表结构：`scripts/deploy_local_stack.zsh init-db`
- 应用增量迁移：`scripts/deploy_local_stack.zsh migrate-db`
- 初始化 MinIO bucket：`scripts/deploy_local_stack.zsh init-minio`

默认导入参数（已按大规模场景优化）：
- `workers=12`
- `batch-size=30000`
- `max-retries=5`
- `retry-sleep=2`
- `pack-shards=64`
- `pack-target-bytes=134217728`（128MiB）

## v2 导入流程（无去重）

### 依赖

- 建立 Python 虚拟环境（推荐）：
  - `python3 -m venv .venv`
- 激活虚拟环境：
  - `source .venv/bin/activate`
- 安装依赖：
  - `python3 -m pip install -r requirements.txt`

### 导入命令

- 全量导入：
  - `scripts/import_headers_v2.zsh`
- 单 bundle：
  - `scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 小样本测试：
  - `scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --max-files 100`
- 断点续跑：
  - `scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --resume`
- 清空后重导：
  - `scripts/import_headers_v2.zsh --truncate-all`

### 导入行为

- 导入会将元数据/符号写入 ClickHouse。
- 导入会将头文件正文按“哈希分片 + 滚动大包”写入 MinIO，避免海量小对象。
- `contents` 中通过 `pack_object_key + pack_offset + pack_length` 定位正文片段。
- 分片与包大小可调：`--pack-shards`（默认 256）、`--pack-target-bytes`（默认 64MiB）。
- 导入会对 `versions(version_num, version_id)` 与 `paths(path_id)` 做增量唯一写入，避免分批导入造成重复项。
- `paths` 表包含派生列（`file_name/file_name_lc/dir_path/dir_name/dir_name_lc`）以优化站点查询，避免运行时路径正则处理。
- 默认禁止导入“老于当前库最新版本”的新版本（避免破坏增量语义）。
- 如需强制导入老版本，显式添加参数：`--allow-old-versions`。
- 当前流程为 **no-dedup**（不做 content 去重）。
- `--resume` 会跳过 `versions/paths` 刷新，避免重复写入。
- `--truncate-all` 与 `--resume` 不能同时使用。

### 断点状态文件

- `data/import_state_v2_no_dedup.json`

## 构建符号可用性表

说明：
- `symbol_presence` 使用 `version_bitmap UInt64` 存储版本可用性位图（按 `version_num` 映射位位置）。

- 全量重建：
  - `scripts/build_symbol_presence_v2.zsh --truncate-first`
- 指定 bundle：
  - `scripts/build_symbol_presence_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 指定 version_id：
  - `scripts/build_symbol_presence_v2.zsh --version-id '15.2|19C56'`
- 显示阶段进度：
  - `scripts/build_symbol_presence_v2.zsh --truncate-first --progress-every 1`

## 推荐执行顺序

1. `scripts/import_headers_v2.zsh ...`
2. `scripts/build_symbol_presence_v2.zsh --truncate-first`

## 端点

- ClickHouse Native：`127.0.0.1:19000`
- MinIO API：`127.0.0.1:19001`
- Redis：`127.0.0.1:16379`
- Web（Nginx）：`127.0.0.1:18080`

## 内网穿透（FRP）

本项目已在 Compose 中提供可选 `frpc` 容器（profile: `tunnel`），用于把本机 `nginx:80` 通过公网服务器暴露出去。

### 1) 在 Ubuntu 公网服务器部署 `frps`

以下示例使用 Docker 方式部署（推荐）：

1. 创建目录并进入：
  - `mkdir -p ~/frp && cd ~/frp`
2. 创建服务端配置 `frps.toml`：

```toml
bindPort = 7000

auth.method = "token"
auth.token = "replace-with-long-random-token"

# Optional: dashboard (lock it down with firewall)
webServer.addr = "0.0.0.0"
webServer.port = 7500
webServer.user = "admin"
webServer.password = "replace-with-strong-password"
```

3. 启动 `frps`：
  - `docker run -d --name frps --restart unless-stopped -v "$PWD/frps.toml:/etc/frp/frps.toml:ro" -p 7000:7000 -p 7500:7500 ghcr.io/fatedier/frps:v0.61.2 -c /etc/frp/frps.toml`
4. 放行防火墙端口：
  - `7000/tcp`（`frpc` 连接用）
  - `FRP_REMOTE_PORT/tcp`（例如 `18080/tcp`，给最终访问者）
  - `7500/tcp`（如果开启 dashboard）

说明：`FRP_REMOTE_PORT` 是最终公网访问端口。比如设为 `18080`，公网访问地址为 `http://<你的公网IP>:18080`。

### 2) 在本项目配置并启动 `frpc`

1. 复制环境变量文件（若尚未复制）：
  - `cp .env.example .env`
2. 在 `.env` 中设置：
  - `FRP_SERVER_ADDR=<你的公网服务器IP或域名>`
  - `FRP_SERVER_PORT=7000`
  - `FRP_TOKEN=<与frps.toml一致的token>`
  - `FRP_PROXY_NAME=ios-headers-web`
  - `FRP_REMOTE_PORT=18080`
3. 启动主服务栈：
  - `scripts/deploy_local_stack.zsh up`
4. 启动穿透客户端（仅 `frpc`）：
  - `docker compose --env-file .env -f docker-compose.yml --profile tunnel up -d frpc`
5. 查看日志确认连通：
  - `docker compose --env-file .env -f docker-compose.yml logs -f frpc`

### 3) 访问验证

- 本地：`http://127.0.0.1:${WEB_PORT:-18080}`
- 公网：`http://<公网服务器IP>:<FRP_REMOTE_PORT>`（默认 `18080`）

若公网无法访问，优先检查：
- Ubuntu 安全组/防火墙是否已放行 `FRP_REMOTE_PORT`
- `FRP_TOKEN` 是否与服务端一致
- `frpc` 日志是否显示 `start proxy success`

## 首期站点功能（English UI, No JavaScript）

### 搜索页（`/`）

- 支持跨版本综合搜索：
  - `Directory`：目录名前缀匹配（如 `Back` → `BackBoardServices`）
  - `Owner`：`filename/interface/protocol/category(host class)` 子串匹配
- 结果交互：
  - 选择 `Directory` 结果：跳转到目录伪静态页 `/d/{directory_name}` 并展示该目录下所有文件（基于最新版本号）
  - 选择 `Owner` 结果：直接跳转到查看页（默认打开该结果存在的最新版本）

### 查看页（`/v/{version_id}/{absolute_path}` 或 `/v/latest/{absolute_path}`）

- 展示指定版本与路径的头文件正文
- `latest` 伪静态路径会自动解析到该文件的最新可用版本
- URL 中的 `version_id` 使用 `_` 表示原始 `|`（例如 `15.2_19C56` 对应 `15.2|19C56`）
- 支持同一路径的跨版本切换
- 显示符号在各版本上的可用性（YES/NO）
- `#import/#include` 仅按同目录文件生成内部超链接

### 缓存与伪静态

- 查看页按需渲染并将最终 HTML 写入 Redis 缓存
- 搜索页按查询参数缓存 SSR HTML
- 可通过 `.env` 控制页面缓存开关：`ENABLE_REDIS_PAGE_CACHE=true|false`
- 可通过 `.env` 控制 Symbol Matrix 开关：`ENABLE_SYMBOL_MATRIX=true|false`
- 可通过 `.env` 控制 Query 耗时显示：`SHOW_QUERY_ELAPSED_MS=true|false`
- 全站为纯 SSR，不依赖任何前端 JavaScript

## 关键文件

- Compose：`docker-compose.yml`
- ClickHouse DDL：`clickhouse/init/001_schema.sql`
- MinIO Bucket：`ios-headers`
- 本地持久化目录：`data`
