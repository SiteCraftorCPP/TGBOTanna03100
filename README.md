# TGBOTanna03100

Telegram-бот для менеджера диплинков Mobz.

## Режимы (`MOBZ_PROVIDER` в `.env`)

- **`mock`** — без запросов в Mobz, короткая ссылка собирается локально как `https://{домен}/{шорткод}`.
- **`http`** — официальный публичный API Mobz:
  - ключ в заголовке **`Authorization`** (значение = сам API-ключ, без префикса `Bearer`, если иное не задано в `settings.json`);
  - **`POST /api/public/addlink`** — создание;
  - **`POST /api/public/editlink`** — правка (ЕРИД по умолчанию в параметр `erid`; в публичной доке отдельно не описан — при необходимости задайте имя в `editlink_token_field` по подсказке поддержки Mobz);
  - **`GET /api/public/mylinks`** — список ссылок (в боте вызывается **без** `stats=1`: с `stats=1` у крупных аккаунтов часто **504** от nginx, без параметра ответ может быть большим и долго скачиваться — таймаут запроса увеличен до 300 с);
  - **`GET /api/public/onelink`** — одна ссылка и агрегированная статистика (`stats.all`);
  - **`GET /api/public/stats`** — клики за период (для отчёта «за период» бот обходит ссылки из `mylinks` и суммирует постранично);
  - **`GET /api/public/folders`** — поиск `folder_id` по имени папки (имена в боте должны совпадать с Mobz).

## Настройка `mobz_api` в `settings.json`

Секция **`mobz_api`** (можно опустить — подставятся разумные значения по умолчанию):

| Поле | Назначение |
|------|------------|
| `origin` | Базовый URL, по умолчанию `https://mobz.io` |
| `auth_header` | Имя заголовка с ключом, по умолчанию `Authorization` |
| `editlink_token_field` | Имя POST-параметра для ЕРИД при `editlink`, по умолчанию `erid` |
| `default_deeplink_id` | Какой диплинк из `deeplinks` использовать для запросов без привязки к карточке (например статистика за период) |
| `stats_unique_only` | Передавать `clean=1` в `onelink` и `stats` |
| `marketplace_link_types` | Для каждого `marketplace id`: `type` и `url_field` для `addlink` (как в API Mobz: WB — `wildberries`/`wildberries`, Ozon — `ozon`/`ozon`, Золотое яблоко — `goldapple`/`goldapple`, Лэтуаль — `letual`/`letual`) |

Тип `custom` с полем `url` в API не подставляет целевую ссылку (ответ «нужно добавить хотя бы 1 ссылку»); для маркетплейсов используйте их `type` и одноимённое поле URL.

## Ключ API

В `.env` задайте переменную из `api_key_env` у нужного диплинка (например `MOBZ_API_KEY_MAIN`) или запасной `MOBZ_API_KEY`.

## Сценарий бота

- площадка → папка → URL → ник → дата → формат;
- шорткод: ник + дата + формат + суффикс маркетплейса;
- карточки в `data/storage.json`;
- «Мои ссылки», вшивание токена, статистика.

## Установка и запуск

```powershell
cd "C:\Users\MOD PC COMPANY\Desktop\TGBOTanna03100"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

В `.env`: `TELEGRAM_BOT_TOKEN`, при необходимости `TELEGRAM_PROXY`. Бот доступен любому пользователю Telegram в личном чате. Дополнительные форматы и диплинки — правка `data/formats_extra.json` и `data/deeplinks_extra.json` на сервере (или базовые в `settings.json`).

## Деплой на VPS (systemd)

1. На VPS установите Python 3.10+ и git.
2. Клонируйте репозиторий в `/opt/tgbotanna03100`.
3. Создайте файл `/opt/tgbotanna03100/.env` (можно по шаблону `.env.example`).
4. Отредактируйте `deploy/systemd/tgbotanna03100.service`:
   - `User` (например `ubuntu`),
   - `WorkingDirectory` и пути (если у вас не `/opt/tgbotanna03100`).
5. Запустите деплой:

```bash
cd /opt/tgbotanna03100
chmod +x scripts/vps_deploy.sh
./scripts/vps_deploy.sh
```

Полезные команды:

```bash
sudo journalctl -u tgbotanna03100.service -f
sudo systemctl restart tgbotanna03100.service
sudo systemctl status tgbotanna03100.service --no-pager
```
