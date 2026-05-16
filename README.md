# TGBOTanna03100

Телеграм-бот вокруг диплинков Mobz: создание, ЕРИД, ссылки в `data/storage.json`, статистика.

## .env

- `TELEGRAM_BOT_TOKEN` — обязателен.
- `TELEGRAM_PROXY` — по желанию.
- `TELEGRAM_ADMIN_IDS` — **обязательно**, хотя бы один числовой Telegram ID **администратора** через запятую: полный доступ, команда `/admin`, управление доступом пользователей, в «Мои ссылки» весь `storage`. Обычным пользователям доступ выдаётся только через `/admin` → Добавить пользователя (список хранится в `data/storage.json` → `allowed_user_ids`).
- `MOBZ_PROVIDER` — `mock` или `http`.
- Ключи Mobz: `MOBZ_API_KEY_MAIN` (или что в `api_key_env` у диплинка), fallback `MOBZ_API_KEY`.

`mock` — без HTTP в Mobz, ссылка собирается локально: `https://<домен>/<шорткод>`.

`http` — публичное API, ключ в `Authorization` (см. `settings.json` → `auth_header`).

## Что жмёт бот (кратко)

Площадка → (папка) → URL товара → ник блогера → дата ДД.ММ → формат → ссылка. Можно «Ещё ссылка» для другого маркетплейса с тем же ником/датой/форматом.

## mobz_api в settings.json

По умолчанию можно не трогать. Полезно знать:

| Поле | Смысл |
|------|--------|
| `origin` | База API, чаще `https://mobz.io` |
| `auth_header` | Заголовок с ключом, обычно `Authorization` |
| `editlink_token_field` | поле для ЕРИД в `editlink` — сейчас `detail_erid` |
| `default_deeplink_id` | для вызовов не из карточки (стата за период и т.д.) |
| `stats_unique_only` | `clean=1` в onelink/stats |
| `marketplace_link_types` | у каждой площадки: `type` + поле с URL (wb/ozon/…) под API Mobz |

`POST /addlink`, `POST /editlink`, `GET /mylinks`, `GET /onelink`, `GET /stats`, `GET /folders` — смотри актуальную доку `mobz.io`. Для mylinks в боте без `stats=1` (крупные аккаунты + nginx = часто 504), таймаут ответа поднят.

## Где ещё править

- `settings.json` — форматы, диплинки, `mobz_api`.
- `data/formats_extra.json`, `data/deeplinks_extra.json` — доп. строки на сервере, если не хочется лезть в `settings.json`.

## Запуск локально (Windows)

```powershell
cd "C:\Users\MOD PC COMPANY\Desktop\TGBOTanna03100"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

## VPS + systemd

1. Python 3.10+, git.
2. Клон в `/opt/tgbotanna03100`, `.env` из шаблона.
3. Подправь `deploy/systemd/tgbotanna03100.service` (user, пути).
4. `./scripts/vps_deploy.sh` или вручную: `systemctl enable --now tgbotanna03100.service`.

Логи: `journalctl -u tgbotanna03100.service -f`
