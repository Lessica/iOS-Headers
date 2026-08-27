from __future__ import annotations

import importlib.util
from pathlib import Path
import struct
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


finder = load_module("test_find_macho", "scripts/find_macho_executables.py")
importer = load_module("test_import_headers", "scripts/_internal/import_headers_v2.py")


def make_macho64(filetype: int, section_name: bytes) -> bytes:
    section_name = section_name[:16].ljust(16, b"\0")
    segment_name = b"__DATA_CONST".ljust(16, b"\0")
    section = struct.pack(
        "<16s16sQQIIIIIIII",
        section_name,
        segment_name,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )
    command_size = 72 + len(section)
    segment = struct.pack(
        "<II16sQQQQiiII",
        finder.LC_SEGMENT_64,
        command_size,
        segment_name,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
    ) + section
    header = struct.pack(
        "<IiiIIIII",
        0xFEEDFACF,
        0,
        0,
        filetype,
        1,
        len(segment),
        0,
        0,
    )
    return header + segment


class MachOCandidateTests(unittest.TestCase):
    def assert_candidate(self, filetype: int, section_name: bytes, expected: bool) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample"
            path.write_bytes(make_macho64(filetype, section_name))
            self.assertEqual(finder.is_class_dump_candidate(path), expected)

    def test_supported_types_with_objc_definitions_are_candidates(self) -> None:
        for filetype in (finder.MH_EXECUTE, finder.MH_DYLIB, finder.MH_BUNDLE):
            with self.subTest(filetype=filetype):
                self.assert_candidate(filetype, b"__objc_classlist", True)

    def test_reference_only_section_is_not_a_candidate(self) -> None:
        self.assert_candidate(finder.MH_EXECUTE, b"__objc_classrefs", False)

    def test_unsupported_type_is_not_a_candidate(self) -> None:
        self.assert_candidate(0xB, b"__objc_classlist", False)

    def test_fat_macho_checks_contained_slice(self) -> None:
        thin = make_macho64(finder.MH_BUNDLE, b"__objc_protolist")
        slice_offset = 0x1000
        fat_header = struct.pack(">II", 0xCAFEBABE, 1)
        fat_arch = struct.pack(">iiIII", 0, 0, slice_offset, len(thin), 12)
        payload = fat_header + fat_arch
        payload += b"\0" * (slice_offset - len(payload)) + thin

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "fat-sample"
            path.write_bytes(payload)
            self.assertTrue(finder.is_class_dump_candidate(path))


class ImportFallbackTests(unittest.TestCase):
    def test_unknown_owner_keeps_content_and_skips_only_symbols(self) -> None:
        source = b"""@interface (extension in Foundation):__C.NSObject\n\n@property (readonly) id value;\n\n@end\n"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "InvalidSwiftExtension.h"
            path.write_bytes(source)
            text_md5, byte_size, symbols, raw_bytes = importer.parse_file_task(path)

        self.assertEqual(byte_size, len(source))
        self.assertEqual(raw_bytes, source)
        self.assertEqual(symbols, [])
        self.assertEqual(len(text_md5), 32)


class DirectoryKeyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from web import app as web_app

        cls.extract_directory_name = staticmethod(web_app._extract_directory_name)

    def test_versioned_framework_uses_framework_and_binary_names(self) -> None:
        path = "/System/Library/Frameworks/IOKit.framework/Versions/A/IOKit/HIDDevice.h"
        self.assertEqual(self.extract_directory_name(path), "IOKit.framework/IOKit")

    def test_regular_directory_keeps_last_two_segments(self) -> None:
        path = "/usr/libexec/backboardd/BKEvent.h"
        self.assertEqual(self.extract_directory_name(path), "libexec/backboardd")


if __name__ == "__main__":
    unittest.main()
