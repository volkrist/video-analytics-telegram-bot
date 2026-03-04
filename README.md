# video-analytics-telegram-bot

Telegram-бот для аналитики по видео: вопрос на естественном языке → одно число.

---

## 1. Как запустить

```bash
git clone https://github.com/volkrist/video-analytics-telegram-bot
cd video-analytics-telegram-bot
docker compose -f docker-compose.prod.yml up -d --build
```

---

## 2. Как задать токен

Создайте файл `.env` в корне проекта:

```env
BOT_TOKEN=your_token_here
OPENAI_API_KEY=your_key
OPENAI_MODEL=gpt-4o-mini
LLM_PROVIDER=openai
```

Для локальной разработки добавьте `DATABASE_URL` (например `postgresql://postgres:postgres@localhost:5433/video_analytics`, если PostgreSQL запущен через `docker compose up -d`).

---

## 3. Как загрузить JSON

Из корня проекта (после запуска БД и применения миграций):

```bash
PYTHONPATH=. python scripts/load_json.py --path data/videos.json
```

В Docker: сначала загрузите данные в уже запущенный `postgres`, затем перезапустите бота при необходимости (бот читает из БД при каждом запросе).

---

## 4. Архитектура

```
user text
    ↓
rules parser (deterministic, no LLM)
    ↓
LLM fallback (if rules did not match)
    ↓
intent JSON (e.g. {"intent": "count_videos_total"})
    ↓
SQL layer (queries.py)
    ↓
PostgreSQL
    ↓
bot returns one number
```

**LLM never generates SQL.** SQL is generated only in code (`app/db/queries.py`). The LLM (or rules) only produce a structured intent; the application maps intents to parameterized queries.
