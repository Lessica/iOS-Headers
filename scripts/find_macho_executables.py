#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import struct
from typing import BinaryIO

MH_EXECUTE = 0x2

MACHO32_BE = b"\xfe\xed\xfa\xce"
MACHO32_LE = b"\xce\xfa\xed\xfe"
MACHO64_BE = b"\xfe\xed\xfa\xcf"
MACHO64_LE = b"\xcf\xfa\xed\xfe"

FAT32_BE = b"\xca\xfe\xba\xbe"
FAT32_LE = b"\xbe\xba\xfe\xca"
FAT64_BE = b"\xca\xfe\xba\xbf"
FAT64_LE = b"\xbf\xba\xfe\xca"


def _is_thin_macho_executable_from_header(header: bytes) -> bool:
    if len(header) < 16:
        return False

    magic = header[:4]
    if magic in (MACHO32_LE, MACHO64_LE):
        filetype = struct.unpack("<I", header[12:16])[0]
    elif magic in (MACHO32_BE, MACHO64_BE):
        filetype = struct.unpack(">I", header[12:16])[0]
    else:
        return False

    return filetype == MH_EXECUTE


def _read_exact(file_obj: BinaryIO, offset: int, size: int) -> bytes | None:
    file_obj.seek(offset)
    data = file_obj.read(size)
    if len(data) != size:
        return None
    return data


def _is_fat_macho_executable(file_obj: BinaryIO, first4: bytes) -> bool:
    if first4 in (FAT32_BE, FAT64_BE):
        endian = ">"
    elif first4 in (FAT32_LE, FAT64_LE):
        endian = "<"
    else:
        return False

    is_fat64 = first4 in (FAT64_BE, FAT64_LE)

    nfat_raw = _read_exact(file_obj, 4, 4)
    if nfat_raw is None:
        return False
    nfat_arch = struct.unpack(f"{endian}I", nfat_raw)[0]

    if nfat_arch == 0 or nfat_arch > 4096:
        return False

    if is_fat64:
        arch_size = 24
    else:
        arch_size = 20

    arch_table = _read_exact(file_obj, 8, nfat_arch * arch_size)
    if arch_table is None:
        return False

    for index in range(nfat_arch):
        base = index * arch_size

        if is_fat64:
            offset = struct.unpack(
                f"{endian}Q", arch_table[base + 8: base + 16])[0]
        else:
            offset = struct.unpack(
                f"{endian}I", arch_table[base + 8: base + 12])[0]

        header = _read_exact(file_obj, offset, 16)
        if header is None:
            continue

        if _is_thin_macho_executable_from_header(header):
            return True

    return False


def is_macho_executable(file_path: Path) -> bool:
    try:
        with file_path.open("rb") as file_obj:
            first4 = file_obj.read(4)
            if len(first4) != 4:
                return False

            if first4 in (MACHO32_BE, MACHO32_LE, MACHO64_BE, MACHO64_LE):
                rest = file_obj.read(12)
                if len(rest) != 12:
                    return False
                return _is_thin_macho_executable_from_header(first4 + rest)

            if first4 in (FAT32_BE, FAT32_LE, FAT64_BE, FAT64_LE):
                return _is_fat_macho_executable(file_obj, first4)

            return False
    except (OSError, ValueError, struct.error):
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recursively scan a directory and print paths of Mach-O executables."
    )
    parser.add_argument("path", type=Path, help="Root directory to scan")
    args = parser.parse_args()

    root = args.path
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Invalid directory: {root}")

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if is_macho_executable(path):
            print(path)


if __name__ == "__main__":
    main()
