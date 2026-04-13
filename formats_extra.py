from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_EXTRA_PATH = "data/formats_extra.json"

_ID_RE = re.compile(r"^[a-zA-Z0-9_]{1,48}$")
_SLUG_RE = re.compile(r"^[a-zA-Z0-9_]*$")


def formats_extra_path(project_dir: Path) -> Path:
    return project_dir / _EXTRA_PATH


def _ensure_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(
            json.dumps({"formats": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def load_extra_format_rows(project_dir: Path) -> list[dict[str, str]]:
    path = formats_extra_path(project_dir)
    _ensure_file(path)
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(raw, dict):
        return []
    rows = raw.get("formats", [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, str]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        try:
            out.append(
                {
                    "id": str(item["id"]).strip(),
                    "label": str(item["label"]).strip(),
                    "slug": str(item["slug"]).strip(),
                }
            )
        except KeyError:
            continue
    return out


def save_extra_format_rows(project_dir: Path, rows: list[dict[str, str]]) -> None:
    path = formats_extra_path(project_dir)
    _ensure_file(path)
    path.write_text(
        json.dumps({"formats": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def validate_format_row(
    format_id: str,
    label: str,
    slug: str,
    *,
    reserved_ids: set[str],
) -> str | None:
    if format_id in reserved_ids:
        return f"Код «{format_id}» уже задан в settings.json."
    if not _ID_RE.match(format_id):
        return "Код формата: латиница, цифры, подчёркивание, длина 1–48."
    if not label or len(label) > 64:
        return "Подпись: 1–64 символа."
    if not _SLUG_RE.match(slug):
        return "Суффикс в шорткоде: только латиница, цифры, подчёркивание (можно пусто)."
    if len(slug) > 32:
        return "Суффикс не длиннее 32 символов."
    return None


def parse_format_line(line: str) -> tuple[str, str, str] | None:
    parts = [p.strip() for p in line.strip().split("|", maxsplit=2)]
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def add_extra_format(
    project_dir: Path,
    format_id: str,
    label: str,
    slug: str,
    *,
    reserved_ids: set[str],
) -> str | None:
    err = validate_format_row(format_id, label, slug, reserved_ids=reserved_ids)
    if err:
        return err

    rows = load_extra_format_rows(project_dir)
    if any(r["id"] == format_id for r in rows):
        return f"Формат с кодом «{format_id}» уже есть в дополнительных."

    rows.append({"id": format_id, "label": label, "slug": slug})
    save_extra_format_rows(project_dir, rows)
    return None


def remove_extra_format(project_dir: Path, format_id: str) -> bool:
    rows = load_extra_format_rows(project_dir)
    new_rows = [r for r in rows if r["id"] != format_id]
    if len(new_rows) == len(rows):
        return False
    save_extra_format_rows(project_dir, new_rows)
    return True


def extra_format_ids(project_dir: Path) -> set[str]:
    return {r["id"] for r in load_extra_format_rows(project_dir)}
