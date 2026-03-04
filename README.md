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

Цепочка обработки запроса:

```
Вопрос пользователя
      ↓
Rules (правила/регулярки) — детерминированный разбор типовых формулировок
      ↓
LLM (если правила не сработали) — возвращает только JSON intent, не SQL
      ↓
При ошибке LLM (401, таймаут и т.д.) — emergency rules (time-range + delta по ключевым словам)
      ↓
Intent JSON
      ↓
intent_parser — выбор функции и параметров по intent
      ↓
SQL-слой (queries.py) — параметризованные запросы к PostgreSQL
      ↓
Бот отвечает одним числом
```

**LLM не генерирует SQL.** Модель возвращает только структурированный JSON (intent + поля). Все SQL-запросы заданы в коде (`app/db/queries.py`) и выполняются через параметризованные вызовы.

✔ Бот всегда отвечает одним числом (в т.ч. при неизвестном интенте или ошибке — «0»).

### Фоллбек при 401/таймауте

Если вызов LLM завершается с ошибкой (401, таймаут, сеть), используется **emergency rules**: по ключевым словам (прирост, выросли, между замерами, время «с … до …», дата словами, опционально креатор с id) строится intent для `sum_delta_in_time_range` или `sum_delta_in_time_range_by_creator`, чтобы ответить числом без LLM.

### Поддерживаемые интенты (возвращаемые после правил/LLM)

- `count_videos_total` — всего видео в системе
- `count_videos_views_gt` — число видео с просмотрами больше порога (threshold)
- `count_videos_metric_cmp_final` — число видео по итоговой статистике: метрика op value (metric, op: lt|lte|gt|gte|eq, value)
- `count_videos_by_creator_views_gt_final` — число видео креатора с просмотрами > порога (creator_id, threshold)
- `count_videos_by_creator_metric_gt_final` — то же с метрикой (creator_id, threshold, metric)
- `count_videos_by_creator_metric_cmp_final` — число видео креатора: метрика op value (creator_id, metric, op, value)
- `count_videos_by_creator_date_range` — число видео креатора за период (creator_id, date_from, date_to; date_to включительно)
- `sum_final_metric_in_period` — сумма финальной метрики по опубликованным видео за период (metric, date_from, date_to_exclusive)
- `sum_delta_on_date` — сумма прироста метрики за дату (date, metric), video_snapshots
- `count_distinct_videos_with_positive_delta_on_date` — число разных видео с положительным приростом за дату (date, metric)
- `count_snapshots_delta_cmp` — число замеров, где прирост за час удовлетворяет условию (metric, op, value)
- `sum_delta_in_time_range` — сумма прироста метрики в промежутке времени дня, все видео (date, time_from, time_to)
- `sum_delta_in_time_range_by_creator` — то же для видео одного креатора (creator_id, date, time_from, time_to)
