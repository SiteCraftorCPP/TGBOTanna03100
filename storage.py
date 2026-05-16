from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonStorage:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self.file_path.write_text(
                json.dumps({"links": [], "allowed_user_ids": []}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def _read(self) -> dict[str, Any]:
        raw: Any = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raw = {"links": [], "allowed_user_ids": []}
            self._write(raw)
            return raw
        changed = False
        if "links" not in raw or not isinstance(raw["links"], list):
            raw["links"] = []
            changed = True
        aides = raw.get("allowed_user_ids")
        parsed: list[int] = []
        if isinstance(aides, list):
            for item in aides:
                try:
                    parsed.append(int(item))
                except (TypeError, ValueError):
                    continue
        else:
            changed = True
        normalized = sorted(set(parsed))
        if raw.get("allowed_user_ids") != normalized:
            raw["allowed_user_ids"] = normalized
            changed = True
        if changed:
            self._write(raw)
        return raw

    def _write(self, payload: dict[str, Any]) -> None:
        self.file_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def create_link(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self._read()
        record = deepcopy(payload)
        record["id"] = str(uuid4())
        record["created_at"] = utcnow_iso()
        record["updated_at"] = record["created_at"]
        data["links"].append(record)
        self._write(data)
        return record

    def list_links(self, limit: int = 20) -> list[dict[str, Any]]:
        data = self._read()
        links = sorted(
            data["links"],
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )
        return links[:limit]

    def list_links_for_owner(self, owner_id: int, limit: int = 20) -> list[dict[str, Any]]:
        data = self._read()
        links = [
            item
            for item in data["links"]
            if item.get("owner_id") is not None and int(item.get("owner_id")) == int(owner_id)
        ]
        links = sorted(
            links,
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )
        return links[:limit]

    def list_all_links(self) -> list[dict[str, Any]]:
        """все карточки — для stat по периоду"""
        data = self._read()
        return sorted(
            data["links"],
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )

    def get_link(self, link_id: str) -> dict[str, Any] | None:
        data = self._read()
        for item in data["links"]:
            if item["id"] == link_id:
                return item
        return None

    def list_allowed_user_ids(self) -> list[int]:
        data = self._read()
        return list(data.get("allowed_user_ids") or [])

    def is_allowed_user(self, user_id: int) -> bool:
        return int(user_id) in self.list_allowed_user_ids()

    def add_allowed_user_id(self, user_id: int) -> bool:
        """True если добавлен, False если уже был в списке."""
        data = self._read()
        cur = sorted(set(int(x) for x in (data.get("allowed_user_ids") or []) if isinstance(x, (int, str))))
        uid = int(user_id)
        if uid in cur:
            return False
        cur.append(uid)
        data["allowed_user_ids"] = sorted(set(cur))
        self._write(data)
        return True

    def remove_allowed_user_id(self, user_id: int) -> bool:
        """True если удалили, False если id не было в списке."""
        data = self._read()
        cur = sorted(set(int(x) for x in (data.get("allowed_user_ids") or [])))
        uid = int(user_id)
        if uid not in cur:
            return False
        data["allowed_user_ids"] = [x for x in cur if x != uid]
        self._write(data)
        return True

    def update_link(self, link_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        data = self._read()
        for index, item in enumerate(data["links"]):
            if item["id"] != link_id:
                continue

            updated = deepcopy(item)
            updated.update(updates)
            updated["updated_at"] = utcnow_iso()
            data["links"][index] = updated
            self._write(data)
            return updated

        return None
