"""
Преобразование JSON intent от LLM в вызов соответствующей функции queries.
SQL не генерируется, только выбор функции и параметров.
"""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

from app.db import queries


def _parse_date(s: str) -> datetime:
    s = (s or "").strip()[:10]
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


async def execute_intent(pool: asyncpg.Pool, intent: dict) -> int:
    """
    По intent из LLM вызвать нужную функцию из queries и вернуть число.
    intent — словарь вида {"intent": "count_videos_total"} или с параметрами.
    """
    name = (intent.get("intent") or "").strip()
    if not name:
        raise ValueError("Missing 'intent' in response")

    if name == "count_videos_total":
        return await queries.count_videos_total(pool)

    if name == "count_videos_views_gt":
        threshold = intent.get("threshold")
        if threshold is None:
            raise ValueError("count_videos_views_gt requires 'threshold'")
        return await queries.count_videos_views_gt(pool, int(threshold))

    if name == "count_videos_by_creator_views_gt":
        creator_id = intent.get("creator_id")
        threshold = intent.get("threshold")
        if creator_id is None or threshold is None:
            raise ValueError(
                "count_videos_by_creator_views_gt requires creator_id and threshold"
            )
        cid = creator_id if isinstance(creator_id, str) else str(creator_id)
        return await queries.count_videos_by_creator_views_gt(
            pool, cid, int(threshold)
        )

    if name == "count_videos_by_creator_date_range":
        creator_id = intent.get("creator_id")
        date_from = intent.get("date_from")
        date_to = intent.get("date_to")
        if creator_id is None or not date_from or not date_to:
            raise ValueError(
                "count_videos_by_creator_date_range requires creator_id, date_from, date_to"
            )
        cid = creator_id if isinstance(creator_id, str) else str(creator_id)
        return await queries.count_videos_by_creator_date_range(
            pool,
            cid,
            _parse_date(str(date_from)),
            _parse_date(str(date_to)),
        )

    metric = intent.get("metric", "views")

    if name == "sum_delta_views_on_date":
        d = intent.get("date")
        if not d:
            raise ValueError("sum_delta_views_on_date requires 'date'")
        return await queries.sum_delta_on_date(
            pool, "views", _parse_date(str(d))
        )

    if name == "sum_delta_on_date":
        d = intent.get("date")
        if not d:
            raise ValueError("sum_delta_on_date requires 'date'")
        return await queries.sum_delta_on_date(
            pool, metric, _parse_date(str(d))
        )

    if name == "count_distinct_videos_with_new_views_on_date":
        d = intent.get("date")
        if not d:
            raise ValueError(
                "count_distinct_videos_with_new_views_on_date requires 'date'"
            )
        return await queries.count_distinct_videos_with_positive_delta_on_date(
            pool, "views", _parse_date(str(d))
        )

    if name == "count_distinct_videos_with_positive_delta_on_date":
        d = intent.get("date")
        if not d:
            raise ValueError(
                "count_distinct_videos_with_positive_delta_on_date requires 'date'"
            )
        return await queries.count_distinct_videos_with_positive_delta_on_date(
            pool, metric, _parse_date(str(d))
        )

    raise ValueError(f"Unknown intent: {name}")
