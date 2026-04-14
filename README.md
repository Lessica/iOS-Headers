# headers.82flex.com

This repository contains the source code for [headers.82flex.com](https://headers.82flex.com).

It includes the web application, import/indexing scripts, infrastructure configuration, and static assets used to run the site. It does not include the production database contents, imported header corpora, object storage payloads, or any other runtime data snapshots.

## What This Project Does

- Serves an SSR website for browsing iOS header files across versions.
- Supports directory and owner search.
- Renders header source views and version-to-version diffs.
- Uses ClickHouse for metadata and symbol indexes, MinIO for header content storage, Redis for caching, and Nginx as the public entrypoint.

## Stack

- Flask + Jinja2
- ClickHouse
- MinIO
- Redis
- Nginx
- Docker Compose

## Repository Layout

- `web/`: Flask application, templates, search logic, and data access.
- `scripts/`: import, indexing, deployment, and utility scripts.
- `clickhouse/`: schema initialization and manual migration SQL.
- `nginx/`: Nginx site configuration.
- `webroot/`: static assets served directly by Nginx.
- `data/`: local runtime state for self-hosted instances.

## Local Development

1. Copy the environment file:

   ```sh
   cp .env.example .env
   ```

2. Start the local stack:

   ```sh
   scripts/deploy_local_stack.zsh up
   ```

3. Open the site:

   ```text
   http://127.0.0.1:18080
   ```

If you want the site to return real search and view results, you must import your own header dataset and build the indexes locally.

## Documentation

- Detailed setup, import flow, service operations, and deployment notes: [docs/setup.md](docs/setup.md)

## Data Notice

This repository is intentionally source-only. Production data is not part of version control.
