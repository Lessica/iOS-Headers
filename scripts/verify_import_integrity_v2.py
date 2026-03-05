#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any
from urllib import error, parse, request

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None
    S3Error = Exception


class ClickHouseClient:
    def __init__(self, base_url: str, database: str, user: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.database = database
        self.user = user
        self.password = password

    def execute(self, sql: str, retries: int = 3, retry_sleep: float = 1.0) -> str:
        params = {
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "query": sql,
        }
        url = f"{self.base_url}/?{parse.urlencode(params)}"

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                req = request.Request(url=url, method="POST")
                with request.urlopen(req, timeout=300) as resp:
                    return resp.read().decode("utf-8", errors="replace")
            except (error.HTTPError, error.URLError, TimeoutError) as exc:
                last_exc = exc
                if attempt == retries:
                    break
                time.sleep(retry_sleep * attempt)

        raise RuntimeError(f"ClickHouse SQL failed: {sql[:160]}...") from last_exc


class MinioClient:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool) -> None:
        if Minio is None:
            raise RuntimeError("Missing dependency: minio. Install with: python3 -m pip install minio")
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def count_objects(
        self,
        prefix: str = "",
        *,
        progress_every: int = 0,
        progress_label: str = "",
    ) -> int:
        count = 0
        start_ts = time.time()
        for _ in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
            count += 1
            if progress_every > 0 and count % progress_every == 0:
                elapsed = max(0.001, time.time() - start_ts)
                rate = count / elapsed
                print(
                    f"[progress] minio-count {progress_label} objects={count} rate={rate:.2f} obj/s",
                    file=sys.stderr,
                )

        if progress_every > 0:
            elapsed = max(0.001, time.time() - start_ts)
            rate = count / elapsed
            print(
                f"[progress] minio-count done {progress_label} objects={count} rate={rate:.2f} obj/s",
                file=sys.stderr,
            )
        return count

    def object_exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(self.bucket, object_name)
            return True
        except S3Error as exc:
            if getattr(exc, "code", "") in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                return False
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify import integrity between ClickHouse contents rows and MinIO objects."
    )
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:18123")
    parser.add_argument("--clickhouse-db", default="ios_headers")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")

    parser.add_argument("--minio-endpoint", default="127.0.0.1:19001")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--minio-bucket", default="ios-headers")
    parser.add_argument("--minio-secure", action="store_true")

    parser.add_argument("--bundle", action="append", default=[])
    parser.add_argument("--prefix", default="")
    parser.add_argument("--inspect-all-bundles", action="store_true")

    parser.add_argument("--sample-check", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-sleep", type=float, default=1.0)
    return parser.parse_args()


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _build_contents_filter(bundles: list[str]) -> str:
    if not bundles:
        return ""
    predicates = [f"startsWith(blob_key, {_quote(item + '/')})" for item in bundles]
    return "WHERE " + " OR ".join(predicates)


def _get_all_bundles(ch: ClickHouseClient, retries: int, retry_sleep: float) -> list[str]:
    rows = ch.execute(
        "SELECT DISTINCT bundle_name FROM versions ORDER BY bundle_name",
        retries=retries,
        retry_sleep=retry_sleep,
    ).splitlines()
    return [row.strip() for row in rows if row.strip()]


def _verify_scope(
    ch: ClickHouseClient,
    mc: MinioClient,
    *,
    bundles: list[str],
    minio_prefix: str,
    sample_check: int,
    progress_every: int,
    max_retries: int,
    retry_sleep: float,
) -> dict[str, Any]:
    contents_filter_sql = _build_contents_filter(bundles)

    count_sql = f"SELECT count() FROM contents {contents_filter_sql}"
    ch_count_text = ch.execute(
        count_sql,
        retries=max_retries,
        retry_sleep=retry_sleep,
    ).strip()
    ch_count = int(ch_count_text or "0")

    minio_count = mc.count_objects(
        prefix=minio_prefix,
        progress_every=progress_every,
        progress_label=minio_prefix or "(all)",
    )

    result: dict[str, Any] = {
        "ok": ch_count == minio_count,
        "clickhouse_contents": ch_count,
        "minio_objects": minio_count,
        "bundle_filter": bundles,
        "minio_prefix": minio_prefix,
        "sample_check": sample_check,
    }

    if sample_check > 0:
        sample_sql = f"SELECT blob_key FROM contents {contents_filter_sql} ORDER BY blob_key LIMIT {sample_check}"
        rows = ch.execute(
            sample_sql,
            retries=max_retries,
            retry_sleep=retry_sleep,
        ).splitlines()
        checked = 0
        missing: list[str] = []
        sample_start = time.time()

        for row in rows:
            blob_key = row.strip()
            if not blob_key:
                continue
            object_name = blob_key.lstrip("/")
            exists = mc.object_exists(object_name)
            checked += 1
            if not exists:
                missing.append(object_name)

            if progress_every > 0 and checked % progress_every == 0:
                elapsed = max(0.001, time.time() - sample_start)
                rate = checked / elapsed
                print(
                    f"[progress] sample-check {minio_prefix or '(all)'} {checked}/{sample_check} rate={rate:.2f} item/s",
                    file=sys.stderr,
                )

        if progress_every > 0:
            elapsed = max(0.001, time.time() - sample_start)
            rate = checked / elapsed if checked > 0 else 0.0
            print(
                f"[progress] sample-check done {minio_prefix or '(all)'} {checked}/{sample_check} rate={rate:.2f} item/s",
                file=sys.stderr,
            )

        result["sample_checked"] = checked
        result["sample_missing_count"] = len(missing)
        result["sample_missing_examples"] = missing[:20]
        if missing:
            result["ok"] = False

    return result


def main() -> None:
    args = parse_args()

    ch = ClickHouseClient(
        base_url=args.clickhouse_url,
        database=args.clickhouse_db,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    mc = MinioClient(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        bucket=args.minio_bucket,
        secure=args.minio_secure,
    )

    if args.inspect_all_bundles:
        if args.bundle or args.prefix:
            raise SystemExit("--inspect-all-bundles cannot be combined with --bundle/--prefix")

        bundles = _get_all_bundles(ch, args.max_retries, args.retry_sleep)
        reports: list[dict[str, Any]] = []
        all_ok = True
        total_bundles = len(bundles)
        for index, bundle_name in enumerate(bundles, start=1):
            print(f"[progress] inspect bundle {index}/{total_bundles}: {bundle_name}", file=sys.stderr)
            report = _verify_scope(
                ch,
                mc,
                bundles=[bundle_name],
                minio_prefix=bundle_name,
                sample_check=args.sample_check,
                progress_every=args.progress_every,
                max_retries=args.max_retries,
                retry_sleep=args.retry_sleep,
            )
            reports.append(report)
            if not report["ok"]:
                all_ok = False

        result: dict[str, Any] = {
            "ok": all_ok,
            "mode": "inspect-all-bundles",
            "bundles_total": len(reports),
            "bundles_failed": sum(1 for item in reports if not item["ok"]),
            "reports": reports,
        }
    else:
        minio_prefix = args.prefix.strip("/")
        if not minio_prefix and args.bundle and len(args.bundle) == 1:
            minio_prefix = args.bundle[0].strip("/")

        result = _verify_scope(
            ch,
            mc,
            bundles=args.bundle,
            minio_prefix=minio_prefix,
            sample_check=args.sample_check,
            progress_every=args.progress_every,
            max_retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if not result["ok"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
