#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import uuid

DEFAULT_CACHE_RELPATH = Path("System/Library/Caches/com.apple.dyld/dyld_shared_cache_arm64e")


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    log_path: Path | None


class AuditWriter:
    def __init__(self, root: Path | None) -> None:
        self.root = root
        self.run_id = uuid.uuid4().hex
        self._lock = threading.Lock()

    def log_path(self, firmware_name: str, input_path: str) -> Path:
        if self.root is None:
            raise RuntimeError("Audit output is disabled")
        digest = hashlib.sha256(input_path.encode("utf-8")).hexdigest()[:20]
        return self.root / "logs" / self.run_id / firmware_name / f"{digest}.log"

    def record(self, payload: dict[str, object]) -> None:
        if self.root is None:
            return
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            **payload,
        }
        manifest_path = self.root / "manifest.jsonl"
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with manifest_path.open("a", encoding="utf-8") as file_obj:
                file_obj.write(line + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run `ipsw class-dump` for one or all firmware folders using dyld shared cache."
        )
    )
    parser.add_argument(
        "firmware_name",
        nargs="?",
        help=(
            "Firmware folder name under --firmwares-root "
            "(for example: 23C55__iPhone12,3_5)"
        ),
    )
    parser.add_argument(
        "--ipsw-path",
        type=Path,
        default=Path("ipsw"),
        help="Path to the ipsw executable (default: ipsw from PATH)",
    )
    parser.add_argument(
        "--firmwares-root",
        type=Path,
        default=Path(".files"),
        help="Root directory that contains firmware folders (default: .files)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(".headers"),
        help="Root directory for class-dump headers output (default: .headers)",
    )
    parser.add_argument(
        "--cache-relpath",
        type=Path,
        default=DEFAULT_CACHE_RELPATH,
        help=(
            "Relative path of dyld shared cache inside each firmware "
            "(default: System/Library/Caches/com.apple.dyld/dyld_shared_cache_arm64e)"
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run class dump for all firmware folders under --firmwares-root",
    )
    parser.add_argument(
        "--regenerate-umbrella-headers",
        action="store_true",
        help=(
            "Use --regenerate-umbrella-headers instead of --all when running "
            "ipsw class-dump in cache mode"
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
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel workers for --all cache mode (default: 1)",
    )
    parser.add_argument(
        "--audit-root",
        type=Path,
        default=Path("data/class_dump_audit"),
        help=(
            "Directory for manifest.jsonl and retained error/warning logs "
            "(default: data/class_dump_audit)"
        ),
    )
    parser.add_argument(
        "--no-audit",
        action="store_true",
        help="Disable per-target class-dump audit records",
    )
    args = parser.parse_args()

    if args.all and args.firmware_name is not None:
        parser.error("Do not provide firmware_name when using --all")

    if args.workers < 1:
        parser.error("--workers must be >= 1")

    if not args.all and args.workers != 1:
        parser.error("--workers is only supported with --all")

    return args


def quote_command(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def run_command(command: list[str], log_path: Path | None) -> CommandResult:
    if log_path is None:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return CommandResult(returncode=result.returncode, log_path=None)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=log_file,
        )
    return CommandResult(returncode=result.returncode, log_path=log_path)


def count_headers(path: Path) -> int:
    if not path.is_dir():
        return 0
    return sum(1 for item in path.rglob("*.h") if item.is_file())


def retain_nonempty_log(result: CommandResult) -> Path | None:
    if result.log_path is None:
        return None
    try:
        if result.log_path.stat().st_size > 0:
            return result.log_path
    except FileNotFoundError:
        return None

    try:
        result.log_path.unlink()
    except FileNotFoundError:
        return None

    parent = result.log_path.parent
    for _ in range(2):
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent
    return None


def audit_result(
    audit: AuditWriter,
    *,
    mode: str,
    firmware_name: str,
    input_path: str,
    command: list[str] | None,
    status: str,
    returncode: int | None,
    header_count: int,
    log_path: Path | None = None,
    reason: str = "",
) -> None:
    audit.record(
        {
            "mode": mode,
            "firmware_name": firmware_name,
            "input_path": input_path,
            "command": command or [],
            "status": status,
            "returncode": returncode,
            "header_count": header_count,
            "log_path": str(log_path) if log_path is not None else "",
            "reason": reason,
        }
    )


def resolve_targets(args: argparse.Namespace) -> list[Path]:
    firmwares_root: Path = args.firmwares_root

    if not firmwares_root.exists() or not firmwares_root.is_dir():
        raise SystemExit(f"Invalid firmwares root: {firmwares_root}")

    if args.all:
        firmware_dirs = sorted(path for path in firmwares_root.iterdir() if path.is_dir())
        if not firmware_dirs:
            raise SystemExit(f"No firmware directories found under: {firmwares_root}")
        return firmware_dirs

    firmware_dir = firmwares_root / args.firmware_name
    if not firmware_dir.exists() or not firmware_dir.is_dir():
        raise SystemExit(f"Firmware directory not found: {firmware_dir}")
    return [firmware_dir]


def run_cache_mode(args: argparse.Namespace, audit: AuditWriter) -> tuple[int, int, int, int, int]:
    ipsw_path = str(args.ipsw_path)
    output_root: Path = args.output_root
    cache_relpath: Path = args.cache_relpath
    class_dump_scope_flag = (
        "--regenerate-umbrella-headers" if args.regenerate_umbrella_headers else "--all"
    )

    firmware_dirs = resolve_targets(args)

    total = 0
    succeeded = 0
    empty = 0
    skipped = 0
    failed = 0

    def process_firmware(firmware_dir: Path) -> tuple[str, int | None, str]:
        cache_path = firmware_dir / cache_relpath
        if not cache_path.is_file():
            print(f"[SKIP] Missing cache: {cache_path}", file=sys.stderr)
            audit_result(
                audit,
                mode="dyld_cache",
                firmware_name=firmware_dir.name,
                input_path=str(cache_path),
                command=None,
                status="skipped",
                returncode=None,
                header_count=0,
                reason="missing input",
            )
            return "skipped", None, firmware_dir.name

        # out_dir = output_root / firmware_dir.name / cache_relpath
        out_dir = output_root / firmware_dir.name
        out_dir.mkdir(parents=True, exist_ok=True)

        command = [
            ipsw_path,
            "class-dump",
            class_dump_scope_flag,
            "--demangle",
            "--deps",
            "--headers",
            "--refs",
            "-o",
            str(out_dir),
            str(cache_path),
        ]

        print(f"[RUN ] {quote_command(command)}", flush=True)
        if args.dry_run:
            audit_result(
                audit,
                mode="dyld_cache",
                firmware_name=firmware_dir.name,
                input_path=str(cache_path),
                command=command,
                status="dry_run",
                returncode=None,
                header_count=0,
            )
            return "succeeded", None, firmware_dir.name

        log_path = (
            audit.log_path(firmware_dir.name, str(cache_path))
            if audit.root is not None
            else None
        )
        result = run_command(command, log_path)
        header_count = count_headers(out_dir)
        if result.returncode == 0 and header_count > 0:
            retained_log = retain_nonempty_log(result)
            audit_result(
                audit,
                mode="dyld_cache",
                firmware_name=firmware_dir.name,
                input_path=str(cache_path),
                command=command,
                status="succeeded",
                returncode=result.returncode,
                header_count=header_count,
                log_path=retained_log,
            )
            return "succeeded", None, firmware_dir.name
        if result.returncode == 0:
            audit_result(
                audit,
                mode="dyld_cache",
                firmware_name=firmware_dir.name,
                input_path=str(cache_path),
                command=command,
                status="empty",
                returncode=result.returncode,
                header_count=0,
                log_path=result.log_path,
                reason="class-dump produced no headers",
            )
            return "empty", None, firmware_dir.name
        audit_result(
            audit,
            mode="dyld_cache",
            firmware_name=firmware_dir.name,
            input_path=str(cache_path),
            command=command,
            status="failed",
            returncode=result.returncode,
            header_count=header_count,
            log_path=result.log_path,
            reason="class-dump exited with a nonzero status",
        )
        return "failed", result.returncode, firmware_dir.name

    if args.all and args.workers > 1:
        total = len(firmware_dirs)
        pending_dirs = iter(firmware_dirs)
        in_flight: dict[Future[tuple[str, int | None, str]], Path] = {}

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for _ in range(min(args.workers, total)):
                firmware_dir = next(pending_dirs, None)
                if firmware_dir is None:
                    break
                future = executor.submit(process_firmware, firmware_dir)
                in_flight[future] = firmware_dir

            stop_on_failure = False
            failure_code = 1

            while in_flight:
                done, _ = wait(set(in_flight.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.pop(future, None)
                    status, returncode, firmware_name = future.result()

                    if status == "succeeded":
                        succeeded += 1
                    elif status == "empty":
                        empty += 1
                    elif status == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                        print(
                            f"[FAIL] Firmware {firmware_name} exited with code {returncode}",
                            file=sys.stderr,
                        )
                        if not args.continue_on_error:
                            stop_on_failure = True
                            failure_code = returncode if returncode is not None else 1

                    if stop_on_failure:
                        break

                    firmware_dir = next(pending_dirs, None)
                    if firmware_dir is not None:
                        new_future = executor.submit(process_firmware, firmware_dir)
                        in_flight[new_future] = firmware_dir

                if stop_on_failure:
                    for future in in_flight:
                        future.cancel()
                    raise SystemExit(failure_code)
    else:
        for firmware_dir in firmware_dirs:
            total += 1
            status, returncode, firmware_name = process_firmware(firmware_dir)
            if status == "succeeded":
                succeeded += 1
            elif status == "empty":
                empty += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1
                print(
                    f"[FAIL] Firmware {firmware_name} exited with code {returncode}",
                    file=sys.stderr,
                )
                if not args.continue_on_error:
                    raise SystemExit(returncode if returncode is not None else 1)

    return total, succeeded, empty, skipped, failed


def run_stdin_mode(args: argparse.Namespace, audit: AuditWriter) -> tuple[int, int, int, int, int]:
    ipsw_path = str(args.ipsw_path)
    output_root: Path = args.output_root
    firmwares_root_abs = args.firmwares_root.resolve()

    if not args.firmwares_root.exists() or not args.firmwares_root.is_dir():
        raise SystemExit(f"Invalid firmwares root: {args.firmwares_root}")

    lines = [line.strip() for line in sys.stdin if line.strip()]
    lines = [line for line in lines if "/tmp/" not in line]
    if not lines:
        raise SystemExit("No Mach-O paths received from stdin")

    total = 0
    succeeded = 0
    empty = 0
    skipped = 0
    failed = 0

    def remove_empty_binary_output_dir(path: Path) -> None:
        if not path.exists() or not path.is_dir():
            return
        try:
            next(path.iterdir())
            return
        except StopIteration:
            pass
        except OSError:
            return

        try:
            path.rmdir()
            print(f"[CLEAN] Removed empty output dir: {path}")
        except OSError:
            return

    for raw_path in lines:
        total += 1

        executable_path_input = Path(raw_path)
        command_input_path = str(executable_path_input)
        if executable_path_input.is_absolute():
            executable_path = executable_path_input.resolve()
            command_input_path = str(executable_path)
        else:
            executable_path = (Path.cwd() / executable_path_input).resolve()

        if not executable_path.is_file():
            skipped += 1
            print(f"[SKIP] Missing Mach-O: {raw_path}", file=sys.stderr)
            audit_result(
                audit,
                mode="standalone",
                firmware_name="",
                input_path=raw_path,
                command=None,
                status="skipped",
                returncode=None,
                header_count=0,
                reason="missing input",
            )
            continue

        try:
            rel_to_root = executable_path.relative_to(firmwares_root_abs)
        except ValueError:
            skipped += 1
            print(
                (
                    "[SKIP] Executable is outside --firmwares-root: "
                    f"{raw_path} (firmwares_root={args.firmwares_root})"
                ),
                file=sys.stderr,
            )
            audit_result(
                audit,
                mode="standalone",
                firmware_name="",
                input_path=raw_path,
                command=None,
                status="skipped",
                returncode=None,
                header_count=0,
                reason="input is outside firmwares root",
            )
            continue

        parts = rel_to_root.parts
        if len(parts) < 2:
            skipped += 1
            print(f"[SKIP] Invalid Mach-O path under root: {raw_path}", file=sys.stderr)
            audit_result(
                audit,
                mode="standalone",
                firmware_name="",
                input_path=raw_path,
                command=None,
                status="skipped",
                returncode=None,
                header_count=0,
                reason="invalid input path under firmwares root",
            )
            continue

        firmware_name = parts[0]
        binary_relpath = Path(*parts[1:])
        out_dir = output_root / firmware_name / binary_relpath.parent
        out_dir.mkdir(parents=True, exist_ok=True)

        command = [
            ipsw_path,
            "class-dump",
            "--demangle",
            "--headers",
            "--refs",
            "-o",
            str(out_dir),
            command_input_path,
        ]

        print(f"[RUN ] {quote_command(command)}", flush=True)
        if args.dry_run:
            audit_result(
                audit,
                mode="standalone",
                firmware_name=firmware_name,
                input_path=str(executable_path),
                command=command,
                status="dry_run",
                returncode=None,
                header_count=0,
            )
            succeeded += 1
            continue

        expected_output_dir = out_dir / binary_relpath.name
        log_path = (
            audit.log_path(firmware_name, str(executable_path))
            if audit.root is not None
            else None
        )
        result = run_command(command, log_path)
        header_count = count_headers(expected_output_dir)
        if result.returncode == 0 and header_count > 0:
            succeeded += 1
            retained_log = retain_nonempty_log(result)
            audit_result(
                audit,
                mode="standalone",
                firmware_name=firmware_name,
                input_path=str(executable_path),
                command=command,
                status="succeeded",
                returncode=result.returncode,
                header_count=header_count,
                log_path=retained_log,
            )
        elif result.returncode == 0:
            empty += 1
            remove_empty_binary_output_dir(expected_output_dir)
            audit_result(
                audit,
                mode="standalone",
                firmware_name=firmware_name,
                input_path=str(executable_path),
                command=command,
                status="empty",
                returncode=result.returncode,
                header_count=0,
                log_path=result.log_path,
                reason="class-dump produced no headers",
            )
        else:
            failed += 1
            audit_result(
                audit,
                mode="standalone",
                firmware_name=firmware_name,
                input_path=str(executable_path),
                command=command,
                status="failed",
                returncode=result.returncode,
                header_count=header_count,
                log_path=result.log_path,
                reason="class-dump exited with a nonzero status",
            )
            print(
                f"[FAIL] Mach-O {raw_path} exited with code {result.returncode}",
                file=sys.stderr,
            )
            if not args.continue_on_error:
                raise SystemExit(result.returncode)

    return total, succeeded, empty, skipped, failed


def main() -> None:
    args = parse_args()
    audit = AuditWriter(None if args.no_audit else args.audit_root)

    use_cache_mode = args.all or args.firmware_name is not None
    has_stdin_input = not sys.stdin.isatty()

    if use_cache_mode:
        total, succeeded, empty, skipped, failed = run_cache_mode(args, audit)
    else:
        if not has_stdin_input:
            raise SystemExit("Provide firmware_name / --all, or pipe Mach-O paths from stdin")
        total, succeeded, empty, skipped, failed = run_stdin_mode(args, audit)

    print(
        f"Done. total={total} succeeded={succeeded} empty={empty} "
        f"skipped={skipped} failed={failed}",
        file=sys.stderr if failed else sys.stdout,
    )

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
