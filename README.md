# video-analytics-telegram-bot

Telegram-бот, который отвечает на аналитические вопросы о видео одним числом.

**Пример:**

*Сколько всего видео есть в системе?*  
→ 358

Бот принимает вопрос на естественном языке, определяет нужную метрику и выполняет SQL-запрос к базе данных.

---

## Запуск

Клонировать репозиторий:

```bash
git clone https://github.com/volkrist/video-analytics-telegram-bot
cd video-analytics-telegram-bot
```

Запустить сервисы:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Будут подняты:

- PostgreSQL
- Telegram-бот

---

## Настройка токенов

Создайте файл `.env` в корне проекта:

```env
BOT_TOKEN=your_telegram_bot_token
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4o-mini
LLM_PROVIDER=openai
```

Для локального запуска можно также указать:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/video_analytics
```

Файл `.env` не попадает в репозиторий (добавлен в `.gitignore`).

---

## Загрузка данных

После запуска PostgreSQL необходимо загрузить JSON-датасет.

Из корня проекта:

```bash
PYTHONPATH=. python scripts/load_json.py --path data/videos.json
```

Скрипт:

- читает JSON
- вставляет данные в таблицы `videos` и `video_snapshots`
- поддерживает повторный запуск без ошибок.

---

## Архитектура

Общий поток обработки запроса:

```
User question
      ↓
Rule-based parser
      ↓
LLM fallback (если правила не подошли)
      ↓
Intent JSON
      ↓
SQL layer
      ↓
PostgreSQL
      ↓
Telegram bot returns a number
```

Пример intent:

```json
{"intent": "count_videos_views_gt", "threshold": 100000}
```

### Важное ограничение

**LLM никогда не генерирует SQL.**

LLM (или rule parser) возвращает только структурированный intent.  
SQL-запросы определены строго в коде (`app/db/queries.py`) и выполняются через параметризованные запросы к PostgreSQL.

✔ Бот всегда возвращает одно число.
