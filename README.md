# iOS-Headers 本地构建与数据流程

本项目当前使用 **v2 构建流程**（ClickHouse + Redis + MinIO）。

## 弃用说明

- `scripts/build_headers_site_data.py` 已弃用。
- 该脚本基于 SQLite 的轻量索引思路，不再适用于当前数据规模与站点功能需求。
- 请使用下文的 **v2 导入与聚合流程**。

## 服务栈（OrbStack / Docker Compose）

- ClickHouse：元数据与符号索引
- Redis：热点缓存
- RedisInsight：Redis 可视化管理
- MinIO：头文件正文对象存储

### 快速启动

1. 复制环境变量文件：
   - `cp deploy/.env.example deploy/.env`
2. 启动服务：
   - `zsh scripts/deploy_local_stack.zsh up`
3. 健康检查：
   - `zsh scripts/deploy_local_stack.zsh check`

启动完成后，`deploy_local_stack.zsh up` 会自动在 RedisInsight 中创建到 `redis:6379` 的连接（名称 `local-redis`）。

### 常用命令

- 启动：`zsh scripts/deploy_local_stack.zsh up`
- 停止：`zsh scripts/deploy_local_stack.zsh down`
- 重启：`zsh scripts/deploy_local_stack.zsh restart`
- 状态：`zsh scripts/deploy_local_stack.zsh status`
- 日志：`zsh scripts/deploy_local_stack.zsh logs`
- 单服务日志：`zsh scripts/deploy_local_stack.zsh logs clickhouse`
- 重建表结构：`zsh scripts/deploy_local_stack.zsh init-db`

默认进度刷新频率可在 `deploy/.env` 中统一配置：
- `PROGRESS_EVERY=1000`
- 作用于 `import_headers_v2.zsh`、`build_symbol_presence_v2.zsh`、`verify_import_integrity_v2.zsh`
- 如命令行显式传入 `--progress-every`，会覆盖该默认值

## v2 导入流程（无去重）

### 依赖

- 需要 Python 包：
  - `python3 -m pip install minio`

### 导入命令

- 单 bundle 全量导入示例：
  - `zsh scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --workers 8 --batch-size 1000`
- 小样本测试：
  - `zsh scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --max-files 100 --workers 4 --batch-size 200`
- 断点续跑：
  - `zsh scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --resume`
- 清空后重导：
  - `zsh scripts/import_headers_v2.zsh --truncate-all --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 显示更密集进度（每 200 文件）：
  - `zsh scripts/import_headers_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --progress-every 200`

### 导入行为

- 导入会将元数据/符号写入 ClickHouse。
- 导入会将头文件正文上传到 MinIO。
- 导入会对 `versions(version_num, version_id)` 与 `paths(path_id)` 做增量唯一写入，避免分批导入造成重复项。
- 当前流程为 **no-dedup**（不做 content 去重）。
- `--resume` 会跳过 `versions/paths` 刷新，避免重复写入。
- `--truncate-all` 与 `--resume` 不能同时使用。

### 断点状态文件

- `deploy/data/import_state_v2_no_dedup.json`

## 构建符号可用性表

说明：
- `symbol_presence` 使用 `version_bitmap UInt64` 存储版本可用性位图（按 `version_num` 映射位位置）。

- 全量重建：
  - `zsh scripts/build_symbol_presence_v2.zsh --truncate-first`
- 指定 bundle：
  - `zsh scripts/build_symbol_presence_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 指定 version_id：
  - `zsh scripts/build_symbol_presence_v2.zsh --version-id '15.2|19C56'`
- 显示阶段进度：
  - `zsh scripts/build_symbol_presence_v2.zsh --truncate-first --progress-every 1`

## 导入完整性核对

- 全量核对：
  - `zsh scripts/verify_import_integrity_v2.zsh`
- 单 bundle 核对：
  - `zsh scripts/verify_import_integrity_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5`
- 抽样对象存在性核对：
  - `zsh scripts/verify_import_integrity_v2.zsh --bundle 19C56__iPhone11,2_4_6_iPhone12,3_5 --sample-check 50`
- 巡检模式（遍历所有 bundle）：
  - `zsh scripts/verify_import_integrity_v2.zsh --inspect-all-bundles`
- 巡检 + 每 bundle 抽样：
  - `zsh scripts/verify_import_integrity_v2.zsh --inspect-all-bundles --sample-check 20`
- 控制进度刷新频率（对象计数/抽样）：
  - `zsh scripts/verify_import_integrity_v2.zsh --inspect-all-bundles --sample-check 20 --progress-every 5000`

核对退出码：

- `0`：通过
- `2`：发现不一致（数量不匹配或抽样缺失）

## 推荐执行顺序

1. `zsh scripts/import_headers_v2.zsh ...`
2. `zsh scripts/build_symbol_presence_v2.zsh --truncate-first`
3. `zsh scripts/verify_import_integrity_v2.zsh --inspect-all-bundles`

## 端点

- ClickHouse HTTP：`http://127.0.0.1:18123`
- ClickHouse Native：`127.0.0.1:19000`
- Redis：`127.0.0.1:16379`
- RedisInsight：`http://127.0.0.1:15540`
- MinIO API：`http://127.0.0.1:19001`
- MinIO Console：`http://127.0.0.1:19002`

## 关键文件

- Compose：`deploy/docker-compose.yml`
- ClickHouse DDL：`deploy/clickhouse/init/001_schema.sql`
- 本地持久化目录：`deploy/data`
- MinIO Bucket：`ios-headers`
