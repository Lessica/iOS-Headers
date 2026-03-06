# iOS-Headers 本地构建与数据流程

本项目当前使用 **v2 构建流程**（ClickHouse + Redis + MinIO）。

## 服务栈（OrbStack / Docker Compose）

- ClickHouse：元数据与符号索引
- MinIO：头文件正文对象存储
- Redis：热点缓存

### 快速启动

1. 复制环境变量文件：
  - `cp .env.example .env`
2. 启动服务：
   - `scripts/deploy_local_stack.zsh up`
3. 健康检查：
   - `scripts/deploy_local_stack.zsh check`

### 常用命令

- 启动：`scripts/deploy_local_stack.zsh up`
- 停止：`scripts/deploy_local_stack.zsh down`
- 重启：`scripts/deploy_local_stack.zsh restart`
- 状态：`scripts/deploy_local_stack.zsh status`
- 日志：`scripts/deploy_local_stack.zsh logs`
  - 单服务：`scripts/deploy_local_stack.zsh logs clickhouse`
- 重建表结构：`scripts/deploy_local_stack.zsh init-db`
- 初始化 MinIO bucket：`scripts/deploy_local_stack.zsh init-minio`

默认进度刷新频率可在 `.env` 中统一配置：
- `PROGRESS_EVERY=1000`
- 作用于 `import_headers_v2.zsh`、`build_symbol_presence_v2.zsh`、`verify_import_integrity_v2.zsh`
- 如命令行显式传入 `--progress-every`，会覆盖该默认值

默认导入参数（已按大规模场景优化）：
- `workers=12`
- `batch-size=30000`
- `max-retries=5`
- `retry-sleep=2`
- `pack-shards=64`
- `pack-target-bytes=134217728`（128MiB）

## v2 导入流程（无去重）

### 依赖

- 需要 Python 包：
  - `python3 -m pip install -r requirements.txt`

### 导入命令

- 单 bundle 全量导入：
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

## 导入完整性核对

- 全量核对：
  - `scripts/verify_import_integrity_v2.zsh`
- 单 bundle 核对：
  - `scripts/verify_import_integrity_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 抽样对象存在性核对：
  - `scripts/verify_import_integrity_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --sample-check 50`
- 巡检模式（遍历所有 bundle）：
  - `scripts/verify_import_integrity_v2.zsh --inspect-all-bundles`
- 巡检 + 每 bundle 抽样：
  - `scripts/verify_import_integrity_v2.zsh --inspect-all-bundles --sample-check 20`

核对退出码：

- `0`：通过
- `2`：发现不一致（数量不匹配或抽样缺失）

## 推荐执行顺序

1. `scripts/import_headers_v2.zsh ...`
2. `scripts/build_symbol_presence_v2.zsh --truncate-first`
3. `scripts/verify_import_integrity_v2.zsh --inspect-all-bundles`

## 端点

- ClickHouse Native：`127.0.0.1:19000`
- MinIO API：`127.0.0.1:19001`
- Redis：`127.0.0.1:16379`

## 关键文件

- Compose：`docker-compose.yml`
- ClickHouse DDL：`clickhouse/init/001_schema.sql`
- MinIO Bucket：`ios-headers`
- 本地持久化目录：`data`
