import logging
from datetime import date, datetime, timedelta, timezone

import asyncpg

logger = logging.getLogger(__name__)

# Ориентация delta по метрике: "NEXT" = delta в строке T = прирост T-1→T (границы (start, end])
# "CURR" = delta в строке T = прирост T→T+1 (границы [start, end)). Калибруется один раз на метрику.
_delta_orientation_by_metric: dict[str, str] = {}

# Колонка накопительного счётчика в video_snapshots для калибровки (diff = count_curr - count_prev)
SNAPSHOT_COUNT_COLUMN = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
}


def _floor_to_hour(dt: datetime) -> datetime:
    """Округление вниз до часа (минуты/секунды/микросекунды = 0). Сохраняет tzinfo."""
    return dt.replace(minute=0, second=0, microsecond=0)


async def _calibrate_delta_orientation(pool: asyncpg.Pool, metric: str) -> str:
    """
    По двум соседним снапшотам: diff = count(t1)-count(t0) для данной метрики.
    Если diff == delta(t1) → NEXT (> start, <= end). Если diff == delta(t0) → CURR (>= start, < end).
    По умолчанию CURR.
    """
    global _delta_orientation_by_metric
    count_col = SNAPSHOT_COUNT_COLUMN.get(metric, "views_count")
    delta_col = DELTA_COLUMN.get(metric, "delta_views_count")
    sql = f"""
        WITH ordered AS (
            SELECT video_id, created_at, {count_col} AS cnt, {delta_col} AS delta,
                   LAG({count_col}) OVER (PARTITION BY video_id ORDER BY created_at) AS prev_cnt,
                   LAG({delta_col}) OVER (PARTITION BY video_id ORDER BY created_at) AS prev_delta
            FROM video_snapshots
            WHERE {delta_col} IS NOT NULL
        )
        SELECT (cnt - prev_cnt) AS diff, delta AS delta_curr, prev_delta AS delta_prev
        FROM ordered
        WHERE prev_cnt IS NOT NULL
        LIMIT 1
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql)
        if row is None:
            _delta_orientation_by_metric[metric] = "CURR"
            logger.info("delta_orientation metric=%s: no calibration data, default CURR [start, end)", metric)
            return "CURR"
        diff = row["diff"]
        delta_curr = row["delta_curr"]
        delta_prev = row["delta_prev"]
        if diff is not None and delta_curr is not None and diff == delta_curr:
            _delta_orientation_by_metric[metric] = "NEXT"
            logger.info("delta_orientation metric=%s: calibrated NEXT (delta at next hour), use (start, end]", metric)
            return "NEXT"
        if diff is not None and delta_prev is not None and diff == delta_prev:
            _delta_orientation_by_metric[metric] = "CURR"
            logger.info("delta_orientation metric=%s: calibrated CURR (delta at current hour), use [start, end)", metric)
            return "CURR"
        _delta_orientation_by_metric[metric] = "CURR"
        logger.info("delta_orientation metric=%s: ambiguous (diff=%s curr=%s prev=%s), default CURR", metric, diff, delta_curr, delta_prev)
        return "CURR"
    except Exception as e:
        logger.warning("delta_orientation metric=%s calibration failed: %s, default CURR", metric, e)
        _delta_orientation_by_metric[metric] = "CURR"
        return "CURR"


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
    return await count_videos_by_creator_metric_gt_final(
        pool, str(creator_id), "views", threshold
    )


FINAL_COLUMN = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
}


def _date_naive(s: str) -> datetime:
    """YYYY-MM-DD → naive datetime 00:00:00 (для колонок timestamp without time zone)."""
    s = (s or "").strip()[:10]
    return datetime.strptime(s, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)


def _date_utc(s: str) -> datetime:
    """YYYY-MM-DD → datetime 00:00:00 UTC (для колонок timestamptz). videos.video_created_at — TIMESTAMPTZ."""
    s = (s or "").strip()[:10]
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


async def sum_final_metric_in_date_range(
    pool: asyncpg.Pool,
    metric: str,
    date_from: str,
    date_to_exclusive: str,
) -> int:
    """
    Сумма финальной метрики по видео, опубликованным в период [date_from, date_to_exclusive).
    date_from / date_to_exclusive: YYYY-MM-DD (date_to_exclusive — первый день после периода, не входит).
    """
    col = FINAL_COLUMN.get(metric, "views_count")
    from_ts = _date_utc(date_from)
    to_ts = _date_utc(date_to_exclusive)
    sql = f"""
            SELECT COALESCE(SUM({col}), 0)
            FROM videos
            WHERE video_created_at >= $1
              AND video_created_at < $2
            """
    params = (from_ts, to_ts)
    logger.info(
        "sum_final_metric_in_date_range table=videos metric=%s date_from=%s date_to_exclusive=%s sql=%s params=%s",
        metric,
        date_from,
        date_to_exclusive,
        sql.strip(),
        params,
    )
    async with pool.acquire() as conn:
        row = await conn.fetchval(sql, *params)
    return int(row)


async def count_videos_by_creator_metric_gt_final(
    pool: asyncpg.Pool,
    creator_id: str,
    metric: str,
    threshold: int,
) -> int:
    """Число видео креатора по итоговой статистике (таблица videos): метрика > порог."""
    return await count_videos_by_creator_metric_cmp_final(
        pool, str(creator_id), metric, "gt", threshold
    )


FINAL_OP = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "eq": "="}


async def count_videos_metric_cmp_final(
    pool: asyncpg.Pool, metric: str, op: str, value: int
) -> int:
    """Число видео по итоговой статистике (videos): метрика op value (op: lt, lte, gt, gte, eq)."""
    col = FINAL_COLUMN.get(metric, "views_count")
    sql_op = FINAL_OP.get(op, ">")
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"SELECT COUNT(*) FROM videos WHERE {col} {sql_op} $1",
            value,
        )
    return int(row)


async def count_videos_by_creator_metric_cmp_final(
    pool: asyncpg.Pool,
    creator_id: str,
    metric: str,
    op: str,
    value: int,
) -> int:
    """Число видео креатора по итоговой статистике (videos): метрика op value."""
    col = FINAL_COLUMN.get(metric, "views_count")
    sql_op = FINAL_OP.get(op, ">")
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM videos
            WHERE creator_id = $1
              AND {col} {sql_op} $2
            """,
            str(creator_id),
            value,
        )
    return int(row)


DELTA_COLUMN = {
    "views": "delta_views_count",
    "likes": "delta_likes_count",
    "comments": "delta_comments_count",
    "reports": "delta_reports_count",
}

OP = {"lt": "<", "gt": ">", "eq": "="}


async def count_snapshots_delta_cmp(
    pool: asyncpg.Pool, metric: str, op: str, value: int
) -> int:
    """Число замеров (video_snapshots), где прирост метрики за час удовлетворяет условию (op value)."""
    col = DELTA_COLUMN.get(metric, "delta_views_count")
    sql_op = OP.get(op, "<")
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"SELECT COUNT(*) FROM video_snapshots WHERE {col} {sql_op} $1",
            value,
        )
    return int(row)


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


def _time_range_bounds(metric: str) -> tuple[str, str]:
    """Границы: (op_start, op_end) для created_at. NEXT: (>, <=). CURR: (>=, <)."""
    orient = _delta_orientation_by_metric.get(metric)
    if orient == "NEXT":
        return (">", "<=")
    return (">=", "<")


async def sum_delta_in_time_range(
    pool: asyncpg.Pool,
    metric: str,
    datetime_start: datetime,
    datetime_end: datetime,
) -> int:
    """Сумма прироста метрики по снапшотам в интервале (границы по калибровке для метрики), created_at напрямую."""
    if metric not in _delta_orientation_by_metric:
        await _calibrate_delta_orientation(pool, metric)
    op_start, op_end = _time_range_bounds(metric)
    start_floor = _floor_to_hour(datetime_start)
    end_floor = _floor_to_hour(datetime_end)
    col = DELTA_COLUMN.get(metric, "delta_views_count")
    sql = f"""
        SELECT COALESCE(SUM({col}), 0)
        FROM video_snapshots
        WHERE created_at {op_start} $1
          AND created_at {op_end} $2
    """
    params = (start_floor, end_floor)
    logger.info(
        "sum_delta_in_time_range table=video_snapshots metric=%s datetime_start=%s datetime_end=%s sql=%s params=%s",
        metric, datetime_start, datetime_end, sql.strip(), params,
    )
    async with pool.acquire() as conn:
        row = await conn.fetchval(sql, *params)
    return int(row)


async def sum_delta_in_time_range_by_creator(
    pool: asyncpg.Pool,
    metric: str,
    creator_id: str,
    datetime_start: datetime,
    datetime_end: datetime,
) -> int:
    """
    Сумма прироста метрики по снапшотам в интервале (границы из калибровки по метрике), created_at напрямую,
    только для видео данного креатора.
    """
    if metric not in _delta_orientation_by_metric:
        await _calibrate_delta_orientation(pool, metric)
    op_start, op_end = _time_range_bounds(metric)
    start_floor = _floor_to_hour(datetime_start)
    end_floor = _floor_to_hour(datetime_end)
    col = DELTA_COLUMN.get(metric, "delta_views_count")
    cid = str(creator_id)
    sql = f"""
        SELECT COALESCE(SUM(s.{col}), 0)
        FROM video_snapshots s
        JOIN videos v ON v.id = s.video_id
        WHERE v.creator_id::text = $1
          AND s.created_at {op_start} $2
          AND s.created_at {op_end} $3
    """
    params = (cid, start_floor, end_floor)
    async with pool.acquire() as conn:
        row = await conn.fetchval(sql, *params)
    result = int(row)
    logger.info(
        "sum_delta_in_time_range_by_creator table=video_snapshots metric=%s creator_id=%s datetime_start=%s datetime_end=%s sql=%s params=%s result=%s",
        metric, cid, datetime_start, datetime_end, sql.strip(), params, result,
    )
    return result


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
