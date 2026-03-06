from __future__ import annotations

import os
import re
from dataclasses import dataclass
from html import escape
from urllib.parse import quote

IMPORT_PATTERN = re.compile(r'^(\s*#\s*(?:import|include)\s*[<\"])([^>\"]+)([>\"].*)$')


@dataclass(frozen=True)
class RenderedHeader:
    html: str
    line_count: int


def _build_view_link(version_id: str, absolute_path: str) -> str:
    encoded_version = quote(version_id, safe="")
    normalized = absolute_path.lstrip("/")
    encoded_path = "/".join(quote(segment, safe="") for segment in normalized.split("/") if segment)
    return f"/v/{encoded_version}/{encoded_path}"


def render_header_with_import_links(
    source_text: str,
    version_id: str,
    current_absolute_path: str,
    directory_files: set[str],
) -> RenderedHeader:
    directory = os.path.dirname(current_absolute_path)
    lines = source_text.splitlines()
    rendered_lines: list[str] = []

    for raw_line in lines:
        match = IMPORT_PATTERN.match(raw_line)
        if not match:
            rendered_lines.append(escape(raw_line))
            continue

        prefix, token, suffix = match.groups()
        token_basename = os.path.basename(token.strip())
        target_path = f"{directory}/{token_basename}" if directory else f"/{token_basename}"

        if target_path in directory_files:
            href = _build_view_link(version_id=version_id, absolute_path=target_path)
            linked_token = f'<a href="{escape(href)}">{escape(token)}</a>'
            rendered_lines.append(f"{escape(prefix)}{linked_token}{escape(suffix)}")
            continue

        rendered_lines.append(escape(raw_line))

    html = "\n".join(rendered_lines)
    return RenderedHeader(html=html, line_count=len(lines))
