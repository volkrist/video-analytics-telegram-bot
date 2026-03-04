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
- videos: итоговая статистика по ролику (views_count, likes_count, comments_count, reports_count — финальные значения).
- video_snapshots: почасовые снапшоты (delta_* — прирост за час, created_at — время замера).

Важно:
- «по итоговой статистике» / «итоговой» → считать по таблице videos (финальные значения).
- «прирост» / «выросли» / «новые за дату» → считать по video_snapshots (дельты, дата по created_at).
- Если в тексте есть время (HH:MM или «с 10 до 15», «промежутке с … до …») → обязательно вернуть intent sum_delta_in_time_range или sum_delta_in_time_range_by_creator с полями date, time_from, time_to. sum_delta_on_date использовать только когда времени в вопросе нет.

Метрика: views (просмотры), likes (лайки), comments (комментарии), reports (жалобы). По умолчанию views.

Доступные интенты (возвращай только один JSON):
- count_videos_total — всего видео в системе
- count_videos_by_creator_date_range — число видео креатора за период (creator_id, date_from, date_to)
- count_videos_views_gt — число видео с просмотрами больше порога (threshold), таблица videos
- count_videos_metric_cmp_final — число видео по итоговой статистике с метрикой op value (metric, op: lt|lte|gt|gte|eq, value). «Меньше 100000», «не меньше», «не больше» → op lt/gte/lte.
- count_videos_by_creator_views_gt_final — число видео креатора с итоговыми просмотрами > порога (creator_id, threshold), таблица videos
- count_videos_by_creator_metric_gt_final — то же с метрикой (creator_id, threshold, metric)
- count_videos_by_creator_metric_cmp_final — число видео креатора по итоговой статистике: метрика op value (creator_id, metric, op: lt|lte|gt|gte|eq, value)
- sum_delta_on_date — сумма прироста метрики за дату (date, metric), таблица video_snapshots
- count_distinct_videos_with_positive_delta_on_date — число разных видео с положительным приростом за дату (date, metric), video_snapshots
- count_snapshots_delta_cmp — число замеров (video_snapshots), где прирост за час удовлетворяет условию (metric, op: lt|gt|eq, value). «Отрицательный прирост просмотров» / «замеры, где просмотры за час меньше 0» → op=lt, value=0.
- sum_final_metric_in_period — сумма финальной метрики по опубликованным видео за период (metric, date_from, date_to_exclusive). Таблица videos, поле video_created_at. date_to_exclusive = первый день после периода (например для июня 2025: date_from=2025-06-01, date_to_exclusive=2025-07-01).
- sum_delta_in_time_range — сумма прироста метрики по video_snapshots в промежутке времени дня (date, time_from, time_to), все видео. Без creator_id.
- sum_delta_in_time_range_by_creator — то же для видео одного креатора (creator_id, date, time_from, time_to). Пример: «прирост просмотров креатора X за 28 ноября с 10:00 до 15:00».

Формат ответа — только валидный JSON. Примеры:
{"intent": "count_videos_total"}
{"intent": "count_videos_views_gt", "threshold": 100000}
{"intent": "count_videos_metric_cmp_final", "metric": "views", "op": "lt", "value": 100000}
{"intent": "count_videos_by_creator_views_gt_final", "creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63", "threshold": 10000}
{"intent": "count_videos_by_creator_metric_cmp_final", "creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63", "metric": "views", "op": "lte", "value": 5000}
{"intent": "count_videos_by_creator_metric_gt_final", "creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63", "threshold": 10000, "metric": "views"}
{"intent": "count_videos_by_creator_date_range", "creator_id": "42", "date_from": "2025-11-01", "date_to": "2025-11-05"}
{"intent": "sum_delta_on_date", "date": "2025-11-28", "metric": "views"}
{"intent": "count_distinct_videos_with_positive_delta_on_date", "date": "2025-11-27", "metric": "views"}
{"intent": "count_snapshots_delta_cmp", "metric": "views", "op": "lt", "value": 0}
{"intent": "sum_final_metric_in_period", "metric": "views", "date_from": "2025-06-01", "date_to_exclusive": "2025-07-01"}
{"intent": "sum_delta_in_time_range", "metric": "views", "date": "2025-11-28", "time_from": "10:00", "time_to": "15:00"}
{"intent": "sum_delta_in_time_range_by_creator", "metric": "views", "creator_id": "8b76e572635b400c9052286a56176e03", "date": "2025-11-28", "time_from": "10:00", "time_to": "15:00"}
Даты в формате YYYY-MM-DD. Время в формате HH:MM или HH:MM:SS.
"""


async def get_intent(user_text: str) -> dict[str, Any]:
    """
    Порядок: try_parse_rules → LLM → при ошибке LLM (401/любой) try_parse_rules(emergency=True).
    Не возвращаем пусто/0 при 401 — идём в emergency rules.
    """
    rules_result = try_parse_rules(user_text)
    if rules_result is not None:
        return rules_result

    if not settings.OPENAI_API_KEY or settings.LLM_PROVIDER.lower() != "openai":
        emergency_result = try_parse_rules(user_text, emergency=True)
        if emergency_result is not None:
            return emergency_result
        raise ValueError("LLM_PROVIDER=openai and OPENAI_API_KEY required")

    try:
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
            lines = content.splitlines()
            if len(lines) >= 3 and lines[-1].strip().startswith("```"):
                content = "\n".join(lines[1:-1]).strip()
            else:
                content = "\n".join(lines[1:]).strip()
        return json.loads(content)
    except Exception:
        emergency_result = try_parse_rules(user_text, emergency=True)
        if emergency_result is not None:
            return emergency_result
        raise
