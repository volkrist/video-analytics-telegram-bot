"""
Преобразование JSON intent от LLM в вызов соответствующей функции queries.
SQL не генерируется, только выбор функции и параметров.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import asyncpg

from app.db import queries

logger = logging.getLogger(__name__)


def _parse_time_hhmm(s: str) -> tuple[int, int]:
    s = (s or "").strip()
    hh, mm = s.split(":")
    return int(hh), int(mm)


def _dt_utc(date_yyyy_mm_dd: str, time_hhmm: str) -> datetime:
    d = (date_yyyy_mm_dd or "").strip()[:10]
    hh, mm = _parse_time_hhmm(time_hhmm)
    return datetime.strptime(d, "%Y-%m-%d").replace(
        hour=hh, minute=mm, second=0, microsecond=0, tzinfo=timezone.utc
    )


def _dt_naive(date_yyyy_mm_dd: str, time_hhmm: str) -> datetime:
    """Naive datetime для сравнения с timestamp without time zone (video_snapshots.created_at)."""
    d = (date_yyyy_mm_dd or "").strip()[:10]
    hh, mm = _parse_time_hhmm(time_hhmm)
    return datetime.strptime(d, "%Y-%m-%d").replace(
        hour=hh, minute=mm, second=0, microsecond=0
    )


def _parse_date(s: str) -> datetime:
    s = (s or "").strip()[:10]
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


async def execute_intent(pool: asyncpg.Pool, intent: dict) -> int:
    """
    По intent из LLM вызвать нужную функцию из queries и вернуть число.
    intent — словарь вида {"intent": "count_videos_total"} или с параметрами.
    """
    name = (intent.get("intent") or "").strip()
    # FORCE: если есть time_from/time_to — всегда time_range интент
    if intent.get("time_from") and intent.get("time_to"):
        name = "sum_delta_in_time_range_by_creator" if intent.get("creator_id") else "sum_delta_in_time_range"
    if not name:
        raise ValueError("Missing 'intent' in response")

    if name == "count_videos_total":
        return await queries.count_videos_total(pool)

    if name == "count_videos_views_gt":
        threshold = intent.get("threshold")
        if threshold is None:
            raise ValueError("count_videos_views_gt requires 'threshold'")
        return await queries.count_videos_views_gt(pool, int(threshold))

    if name == "count_videos_metric_cmp_final":
        metric = intent.get("metric", "views")
        op = intent.get("op", "gt")
        value = intent.get("value")
        if value is None:
            raise ValueError("count_videos_metric_cmp_final requires 'value'")
        return await queries.count_videos_metric_cmp_final(pool, metric, op, int(value))

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

    if name == "count_videos_by_creator_views_gt_final":
        creator_id = intent.get("creator_id")
        threshold = intent.get("threshold")
        if creator_id is None or threshold is None:
            raise ValueError(
                "count_videos_by_creator_views_gt_final requires creator_id and threshold"
            )
        cid = creator_id if isinstance(creator_id, str) else str(creator_id)
        return await queries.count_videos_by_creator_metric_gt_final(
            pool, cid, "views", int(threshold)
        )

    if name == "count_videos_by_creator_metric_gt_final":
        creator_id = intent.get("creator_id")
        threshold = intent.get("threshold")
        if creator_id is None or threshold is None:
            raise ValueError(
                "count_videos_by_creator_metric_gt_final requires creator_id and threshold"
            )
        cid = creator_id if isinstance(creator_id, str) else str(creator_id)
        m = intent.get("metric", "views")
        return await queries.count_videos_by_creator_metric_gt_final(
            pool, cid, m, int(threshold)
        )

    if name == "count_videos_by_creator_metric_cmp_final":
        creator_id = intent.get("creator_id")
        metric = intent.get("metric", "views")
        op = intent.get("op", "gt")
        value = intent.get("value")
        if creator_id is None or value is None:
            raise ValueError(
                "count_videos_by_creator_metric_cmp_final requires creator_id and value"
            )
        cid = str(creator_id)
        return await queries.count_videos_by_creator_metric_cmp_final(
            pool, cid, metric, op, int(value)
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

    if name == "sum_final_metric_in_period":
        metric = intent.get("metric", "views")
        date_from = intent.get("date_from")
        date_to_excl = intent.get("date_to_exclusive")
        if not date_from or not date_to_excl:
            raise ValueError(
                "sum_final_metric_in_period requires date_from and date_to_exclusive"
            )
        return await queries.sum_final_metric_in_date_range(
            pool, metric, str(date_from), str(date_to_excl)
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

    if name == "count_snapshots_delta_cmp":
        metric = intent.get("metric", "views")
        op = intent.get("op", "lt")
        value = int(intent.get("value", 0))
        return await queries.count_snapshots_delta_cmp(pool, metric, op, value)

    if name == "sum_delta_in_time_range":
        metric = intent.get("metric", "views")
        date_str = intent.get("date")
        time_from = intent.get("time_from")
        time_to = intent.get("time_to")
        if not date_str or not time_from or not time_to:
            raise ValueError("sum_delta_in_time_range requires date, time_from, time_to")
        dt_start = _dt_naive(str(date_str), str(time_from))
        dt_end = _dt_naive(str(date_str), str(time_to))
        return await queries.sum_delta_in_time_range(pool, metric, dt_start, dt_end)

    if name == "sum_delta_in_time_range_by_creator":
        metric = intent.get("metric", "views")
        creator_id = intent.get("creator_id")
        date_str = intent.get("date")
        time_from = intent.get("time_from")
        time_to = intent.get("time_to")
        if not creator_id or not date_str or not time_from or not time_to:
            raise ValueError(
                "sum_delta_in_time_range_by_creator requires creator_id, date, time_from, time_to"
            )
        cid = str(creator_id)
        dt_start = _dt_naive(str(date_str), str(time_from))
        dt_end = _dt_naive(str(date_str), str(time_to))
        return await queries.sum_delta_in_time_range_by_creator(
            pool, metric, cid, dt_start, dt_end
        )

    logger.warning("Unknown intent: name=%s intent=%s", name, intent)
    return 0
