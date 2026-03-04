"""
LLM слой: текст пользователя → JSON intent.
Не генерирует SQL, только структурированный intent для intent_parser.
"""
from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import settings
from app.nlp.rules import try_parse_rules


SCHEMA_DESCRIPTION = """
Таблицы БД:
- videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
- video_snapshots: video_id, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at

Метрика: views (просмотры), likes (лайки), comments (комментарии), reports (жалобы). По умолчанию views.

Доступные интенты (возвращай только один JSON):
- count_videos_total — всего видео в системе
- count_videos_by_creator_date_range — число видео креатора за период (creator_id, date_from, date_to)
- count_videos_views_gt — число видео с просмотрами больше порога (threshold)
- sum_delta_on_date — сумма прироста метрики за дату (date, metric: views|likes|comments|reports)
- count_distinct_videos_with_positive_delta_on_date — число разных видео с положительным приростом метрики за дату (date, metric)

Формат ответа — только валидный JSON. Примеры:
{"intent": "count_videos_total"}
{"intent": "count_videos_views_gt", "threshold": 100000}
{"intent": "count_videos_by_creator_date_range", "creator_id": 42, "date_from": "2025-11-01", "date_to": "2025-11-05"}
{"intent": "sum_delta_on_date", "date": "2025-11-28", "metric": "views"}
{"intent": "count_distinct_videos_with_positive_delta_on_date", "date": "2025-11-27", "metric": "views"}
Даты в формате YYYY-MM-DD.
"""


async def get_intent(user_text: str) -> dict[str, Any]:
    """Сначала rules (без LLM), при неудаче — LLM. Возвращает JSON intent."""
    rules_result = try_parse_rules(user_text)
    if rules_result is not None:
        return rules_result

    if not settings.OPENAI_API_KEY or settings.LLM_PROVIDER.lower() != "openai":
        raise ValueError("LLM_PROVIDER=openai and OPENAI_API_KEY required")

    prompt = f"""{SCHEMA_DESCRIPTION}

Вопрос пользователя на русском: {user_text}

Ответ (только JSON):"""

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""

    content = content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return json.loads(content)
