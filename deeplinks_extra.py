from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import DeeplinkConfig, deeplink_from_raw

_DEEPLINKS_EXTRA = "data/deeplinks_extra.json"


def deeplinks_extra_path(project_dir: Path) -> Path:
    return project_dir / _DEEPLINKS_EXTRA


def _ensure_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(
            json.dumps({"deeplinks": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def load_raw_deeplinks(project_dir: Path) -> list[dict[str, Any]]:
    path = deeplinks_extra_path(project_dir)
    _ensure_file(path)
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(raw, dict):
        return []
    arr = raw.get("deeplinks", [])
    if not isinstance(arr, list):
        return []
    return [x for x in arr if isinstance(x, dict)]


def save_raw_deeplinks(project_dir: Path, rows: list[dict[str, Any]]) -> None:
    path = deeplinks_extra_path(project_dir)
    _ensure_file(path)
    path.write_text(
        json.dumps({"deeplinks": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def deeplink_to_dict(dl: DeeplinkConfig) -> dict[str, Any]:
    return {
        "id": dl.id,
        "label": dl.label,
        "api_key_env": dl.api_key_env,
        "default_domain": dl.default_domain,
        "marketplaces": [
            {
                "id": m.id,
                "label": m.label,
                "suffix": m.suffix,
                "notification_label": m.notification_label,
                "folders": list(m.folders),
            }
            for m in dl.marketplaces
        ],
    }


def load_extra_deeplinks(project_dir: Path) -> list[DeeplinkConfig]:
    out: list[DeeplinkConfig] = []
    for item in load_raw_deeplinks(project_dir):
        try:
            out.append(deeplink_from_raw(item))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def add_extra_deeplink(project_dir: Path, raw: dict[str, Any], base_ids: set[str]) -> str | None:
    try:
        dl = deeplink_from_raw(raw)
    except (KeyError, TypeError, ValueError) as exc:
        return f"Ошибка в JSON: {exc}"

    if dl.id in base_ids:
        return f"Код «{dl.id}» уже занят диплинком из settings.json."

    rows = load_raw_deeplinks(project_dir)
    if any(str(r.get("id", "")).strip() == dl.id for r in rows):
        return "Диплинк с таким id уже есть в дополнительных. Удалите старый или выберите другой id."

    rows.append(deeplink_to_dict(dl))
    save_raw_deeplinks(project_dir, rows)
    return None


def remove_extra_deeplink(project_dir: Path, deeplink_id: str) -> bool:
    rows = load_raw_deeplinks(project_dir)
    new_rows = [r for r in rows if str(r.get("id", "")).strip() != deeplink_id]
    if len(new_rows) == len(rows):
        return False
    save_raw_deeplinks(project_dir, new_rows)
    return True
