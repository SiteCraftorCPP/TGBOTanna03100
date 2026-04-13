from __future__ import annotations

import json
from pathlib import Path


def extra_admins_path(project_dir: Path) -> Path:
    return project_dir / "data" / "extra_admins.json"


def load_extra_admin_ids(project_dir: Path) -> set[int]:
    path = extra_admins_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(
            json.dumps({"admin_ids": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return set()

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()
    if not isinstance(raw, dict):
        return set()
    arr = raw.get("admin_ids", [])
    if not isinstance(arr, list):
        return set()

    out: set[int] = set()
    for x in arr:
        try:
            out.add(int(x))
        except (TypeError, ValueError):
            continue
    return out


def _save_extra_admin_ids(project_dir: Path, ids: set[int]) -> None:
    path = extra_admins_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"admin_ids": sorted(ids)},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def add_extra_admin(project_dir: Path, user_id: int, *, env_admin_ids: set[int]) -> str | None:
    if user_id <= 0:
        return "Некорректный Telegram ID."

    if user_id in env_admin_ids:
        return "Этот ID уже указан в TELEGRAM_ADMIN_IDS (.env)."

    extra = load_extra_admin_ids(project_dir)
    if user_id in extra:
        return "Этот пользователь уже в списке администраторов бота."

    extra.add(user_id)
    _save_extra_admin_ids(project_dir, extra)
    return None


def remove_extra_admin(project_dir: Path, user_id: int, *, env_admin_ids: set[int]) -> str | None:
    """Удаляет только из extra_admins.json. Возвращает текст ошибки или None при успехе."""
    extra = load_extra_admin_ids(project_dir)
    if user_id not in extra:
        return (
            "В списке бота такого id нет. Администраторов из TELEGRAM_ADMIN_IDS "
            "удаляют только вручную в .env."
        )

    new_extra = set(extra)
    new_extra.discard(user_id)
    if not env_admin_ids and not new_extra:
        return "Нельзя удалить последнего администратора."

    _save_extra_admin_ids(project_dir, new_extra)
    return None
