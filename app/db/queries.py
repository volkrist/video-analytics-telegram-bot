from datetime import date, datetime, timedelta, timezone

import asyncpg


def _day_bounds(d: date | datetime):
    if isinstance(d, datetime):
        dt = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        return dt
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return start


async def count_videos_total(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT COUNT(*) FROM videos")
    return int(row)


async def count_videos_by_creator_date_range(
    pool: asyncpg.Pool,
    creator_id: int | str,
    date_from: date | datetime,
    date_to: date | datetime,
) -> int:
    start = _day_bounds(date_from)
    end = _day_bounds(date_to) + timedelta(days=1)
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM videos
            WHERE creator_id = $1
              AND video_created_at >= $2
              AND video_created_at < $3
            """,
            creator_id,
            start,
            end,
        )
    return int(row)


async def count_videos_views_gt(pool: asyncpg.Pool, threshold: int) -> int:
    """Всего видео в системе с просмотрами больше порога (итоговая статистика)."""
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT COUNT(*) FROM videos WHERE views_count > $1",
            threshold,
        )
    return int(row)


async def count_videos_by_creator_views_gt(
    pool: asyncpg.Pool, creator_id: int | str, threshold: int
) -> int:
    """Число видео креатора с итоговыми просмотрами больше порога (videos.views_count)."""
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM videos
            WHERE creator_id = $1
              AND views_count > $2
            """,
            str(creator_id),
            threshold,
        )
    return int(row)


DELTA_COLUMN = {
    "views": "delta_views_count",
    "likes": "delta_likes_count",
    "comments": "delta_comments_count",
    "reports": "delta_reports_count",
}


async def sum_delta_on_date(
    pool: asyncpg.Pool, metric: str, d: date | datetime
) -> int:
    col = DELTA_COLUMN.get(metric, "delta_views_count")
    start = _day_bounds(d)
    end = start + timedelta(days=1)
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM({col}), 0)
            FROM video_snapshots
            WHERE created_at >= $1
              AND created_at < $2
            """,
            start,
            end,
        )
    return int(row)


async def count_distinct_videos_with_positive_delta_on_date(
    pool: asyncpg.Pool, metric: str, d: date | datetime
) -> int:
    col = DELTA_COLUMN.get(metric, "delta_views_count")
    start = _day_bounds(d)
    end = start + timedelta(days=1)
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"""
            SELECT COUNT(DISTINCT video_id)
            FROM video_snapshots
            WHERE created_at >= $1
              AND created_at < $2
              AND {col} > 0
            """,
            start,
            end,
        )
    return int(row)


async def sum_delta_views_on_date(pool: asyncpg.Pool, d: date | datetime) -> int:
    return await sum_delta_on_date(pool, "views", d)


async def count_distinct_videos_with_new_views_on_date(
    pool: asyncpg.Pool, d: date | datetime
) -> int:
    return await count_distinct_videos_with_positive_delta_on_date(
        pool, "views", d
    )
