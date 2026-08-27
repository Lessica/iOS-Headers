#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import struct
from typing import BinaryIO


MH_EXECUTE = 0x2
MH_DYLIB = 0x6
MH_BUNDLE = 0x8

SUPPORTED_FILE_TYPES = {
    MH_EXECUTE: "MH_EXECUTE",
    MH_DYLIB: "MH_DYLIB",
    MH_BUNDLE: "MH_BUNDLE",
}

LC_SEGMENT = 0x1
LC_SEGMENT_64 = 0x19

OBJC_DEFINITION_SECTIONS = {
    b"__objc_classlist",
    b"__objc_catlist",
    b"__objc_protolist",
    b"__objc_nlclslist",
    b"__objc_nlcatlist",
}

MACHO32_BE = b"\xfe\xed\xfa\xce"
MACHO32_LE = b"\xce\xfa\xed\xfe"
MACHO64_BE = b"\xfe\xed\xfa\xcf"
MACHO64_LE = b"\xcf\xfa\xed\xfe"

FAT32_BE = b"\xca\xfe\xba\xbe"
FAT32_LE = b"\xbe\xba\xfe\xca"
FAT64_BE = b"\xca\xfe\xba\xbf"
FAT64_LE = b"\xbf\xba\xfe\xca"

MAX_FAT_ARCHES = 4096
MAX_LOAD_COMMAND_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True)
class MachOSlice:
    filetype: int
    has_objc_definitions: bool

    @property
    def is_supported(self) -> bool:
        return self.filetype in SUPPORTED_FILE_TYPES


def _read_exact(file_obj: BinaryIO, offset: int, size: int) -> bytes | None:
    file_obj.seek(offset)
    data = file_obj.read(size)
    if len(data) != size:
        return None
    return data


def _decode_thin_header(header: bytes) -> tuple[str, bool] | None:
    magic = header[:4]
    if magic == MACHO32_LE:
        return ("<", False)
    if magic == MACHO32_BE:
        return (">", False)
    if magic == MACHO64_LE:
        return ("<", True)
    if magic == MACHO64_BE:
        return (">", True)
    return None


def _section_names(
    command: bytes,
    *,
    endian: str,
    is_64_bit: bool,
) -> list[bytes]:
    if is_64_bit:
        if len(command) < 72:
            return []
        section_count = struct.unpack(f"{endian}I", command[64:68])[0]
        section_offset = 72
        section_size = 80
    else:
        if len(command) < 56:
            return []
        section_count = struct.unpack(f"{endian}I", command[48:52])[0]
        section_offset = 56
        section_size = 68

    names: list[bytes] = []
    for index in range(section_count):
        offset = section_offset + index * section_size
        if offset + section_size > len(command):
            break
        names.append(command[offset: offset + 16].split(b"\0", 1)[0])
    return names


def _inspect_thin_slice(file_obj: BinaryIO, offset: int) -> MachOSlice | None:
    base_header = _read_exact(file_obj, offset, 32)
    if base_header is None:
        return None

    decoded = _decode_thin_header(base_header)
    if decoded is None:
        return None
    endian, is_64_bit = decoded
    header_size = 32 if is_64_bit else 28

    try:
        filetype = struct.unpack(f"{endian}I", base_header[12:16])[0]
        command_count = struct.unpack(f"{endian}I", base_header[16:20])[0]
        command_bytes = struct.unpack(f"{endian}I", base_header[20:24])[0]
    except struct.error:
        return None

    if command_bytes > MAX_LOAD_COMMAND_BYTES:
        return None
    commands = _read_exact(file_obj, offset + header_size, command_bytes)
    if commands is None:
        return None

    has_objc_definitions = False
    command_offset = 0
    try:
        for _ in range(command_count):
            if command_offset + 8 > len(commands):
                break
            command_type, command_size = struct.unpack(
                f"{endian}II",
                commands[command_offset: command_offset + 8],
            )
            if command_size < 8 or command_offset + command_size > len(commands):
                break

            is_segment = (
                (is_64_bit and command_type == LC_SEGMENT_64)
                or (not is_64_bit and command_type == LC_SEGMENT)
            )
            if is_segment:
                command = commands[command_offset: command_offset + command_size]
                if any(
                    name in OBJC_DEFINITION_SECTIONS
                    for name in _section_names(command, endian=endian, is_64_bit=is_64_bit)
                ):
                    has_objc_definitions = True
                    break

            command_offset += command_size
    except struct.error:
        return None

    return MachOSlice(
        filetype=filetype,
        has_objc_definitions=has_objc_definitions,
    )


def inspect_macho(file_path: Path) -> list[MachOSlice]:
    try:
        with file_path.open("rb") as file_obj:
            first4 = file_obj.read(4)
            if len(first4) != 4:
                return []

            if first4 in (MACHO32_BE, MACHO32_LE, MACHO64_BE, MACHO64_LE):
                item = _inspect_thin_slice(file_obj, 0)
                return [item] if item is not None else []

            if first4 in (FAT32_BE, FAT64_BE):
                endian = ">"
            elif first4 in (FAT32_LE, FAT64_LE):
                endian = "<"
            else:
                return []

            is_fat64 = first4 in (FAT64_BE, FAT64_LE)
            count_bytes = _read_exact(file_obj, 4, 4)
            if count_bytes is None:
                return []
            arch_count = struct.unpack(f"{endian}I", count_bytes)[0]
            if arch_count == 0 or arch_count > MAX_FAT_ARCHES:
                return []

            arch_size = 24 if is_fat64 else 20
            arch_table = _read_exact(file_obj, 8, arch_count * arch_size)
            if arch_table is None:
                return []

            slices: list[MachOSlice] = []
            for index in range(arch_count):
                base = index * arch_size
                if is_fat64:
                    slice_offset = struct.unpack(
                        f"{endian}Q",
                        arch_table[base + 8: base + 16],
                    )[0]
                else:
                    slice_offset = struct.unpack(
                        f"{endian}I",
                        arch_table[base + 8: base + 12],
                    )[0]
                item = _inspect_thin_slice(file_obj, slice_offset)
                if item is not None:
                    slices.append(item)
            return slices
    except (OSError, ValueError, struct.error):
        return []


def is_class_dump_candidate(file_path: Path) -> bool:
    return any(
        item.is_supported and item.has_objc_definitions
        for item in inspect_macho(file_path)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively print MH_EXECUTE, MH_DYLIB, and MH_BUNDLE files "
            "that contain Objective-C definition sections."
        )
    )
    parser.add_argument("path", type=Path, help="Root directory to scan")
    args = parser.parse_args()

    root = args.path
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Invalid directory: {root}")

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if is_class_dump_candidate(path):
            print(path)


if __name__ == "__main__":
    main()
