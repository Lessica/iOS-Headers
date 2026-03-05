#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import shlex
import subprocess
import sys

DEFAULT_CACHE_RELPATH = Path("System/Library/Caches/com.apple.dyld/dyld_shared_cache_arm64e")
DEFAULT_DRIVERKIT_CACHE_RELPATH = Path(
    "System/DriverKit/System/Library/dyld/dyld_shared_cache_arm64e"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Batch split dyld shared caches for firmware folders using `ipsw dyld split`."
        )
    )
    parser.add_argument(
        "--firmwares-root",
        type=Path,
        default=Path("files"),
        help="Root directory that contains firmware folders (default: files)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("caches"),
        help="Root directory for split outputs (default: caches)",
    )
    parser.add_argument(
        "--cache-relpath",
        type=Path,
        default=None,
        help=(
            "Relative path of dyld shared cache inside each firmware. "
            "When omitted, uses standard path by default or DriverKit path with --driverkit."
        ),
    )
    parser.add_argument(
        "--driverkit",
        action="store_true",
        help=(
            "Split DriverKit dyld shared cache from each firmware "
            "(default relpath: System/DriverKit/System/Library/dyld/dyld_shared_cache_arm64e)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing remaining firmwares if a command fails",
    )
    return parser.parse_args()


def quote_command(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def main() -> None:
    args = parse_args()

    firmwares_root: Path = args.firmwares_root
    output_root: Path = args.output_root
    if args.cache_relpath is not None:
        cache_relpath = args.cache_relpath
    elif args.driverkit:
        cache_relpath = DEFAULT_DRIVERKIT_CACHE_RELPATH
    else:
        cache_relpath = DEFAULT_CACHE_RELPATH

    if not firmwares_root.exists() or not firmwares_root.is_dir():
        raise SystemExit(f"Invalid firmwares root: {firmwares_root}")

    firmware_dirs = sorted(path for path in firmwares_root.iterdir() if path.is_dir())
    if not firmware_dirs:
        raise SystemExit(f"No firmware directories found under: {firmwares_root}")

    total = 0
    succeeded = 0
    skipped = 0
    failed = 0

    for firmware_dir in firmware_dirs:
        total += 1

        cache_path = firmware_dir / cache_relpath
        if not cache_path.is_file():
            skipped += 1
            print(f"[SKIP] Missing cache: {cache_path}", file=sys.stderr)
            continue

        out_dir = output_root / firmware_dir.name
        out_dir.mkdir(parents=True, exist_ok=True)

        command = [
            "ipsw",
            "dyld",
            "split",
            "--output",
            str(out_dir),
            str(cache_path),
        ]

        print(f"[RUN ] {quote_command(command)}")
        if args.dry_run:
            succeeded += 1
            continue

        result = subprocess.run(command, check=False)
        if result.returncode == 0:
            succeeded += 1
        else:
            failed += 1
            print(
                f"[FAIL] Firmware {firmware_dir.name} exited with code {result.returncode}",
                file=sys.stderr,
            )
            if not args.continue_on_error:
                raise SystemExit(result.returncode)

    print(
        f"Done. total={total} succeeded={succeeded} skipped={skipped} failed={failed}",
        file=sys.stderr if failed else sys.stdout,
    )

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
