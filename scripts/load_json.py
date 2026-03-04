"""
Загрузка JSON с видео в PostgreSQL.
Запуск: PYTHONPATH=. python scripts/load_json.py --path data/videos.json
"""
import argparse
from datetime import datetime, timezone
from pathlib import Path

import orjson
import asyncpg

from app.db.pool import create_pool, close_pool


BATCH_SIZE = 1000


def _parse_ts(value):
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _row_video(obj: dict) -> tuple:
    vid = obj["id"]
    if isinstance(vid, (int, float)):
        vid = int(vid)
    else:
        vid = str(vid).strip()
    creator_id = obj.get("creator_id")
    if creator_id is None:
        creator_id = ""
    creator_id = str(creator_id).strip()
    return (
        vid,
        creator_id,
        _parse_ts(obj.get("video_created_at")),
        obj.get("views_count", 0) or 0,
        obj.get("likes_count", 0) or 0,
        obj.get("comments_count", 0) or 0,
        obj.get("reports_count", 0) or 0,
        _parse_ts(obj.get("created_at")),
        _parse_ts(obj.get("updated_at")),
    )


def _row_snapshot(snapshot_id: int, video_id, obj: dict) -> tuple:
    return (
        snapshot_id,
        video_id,
        obj.get("views_count", 0) or 0,
        obj.get("likes_count", 0) or 0,
        obj.get("comments_count", 0) or 0,
        obj.get("reports_count", 0) or 0,
        obj.get("delta_views_count", 0) or 0,
        obj.get("delta_likes_count", 0) or 0,
        obj.get("delta_comments_count", 0) or 0,
        obj.get("delta_reports_count", 0) or 0,
        _parse_ts(obj.get("created_at")),
        _parse_ts(obj.get("updated_at")),
    )


async def load(path: Path, pool: asyncpg.Pool) -> tuple[int, int]:
    data = orjson.loads(path.read_bytes())
    videos = data.get("videos") or data
    if not isinstance(videos, list):
        raise ValueError("JSON must contain 'videos' array or be an array of videos")

    video_rows = [_row_video(v) for v in videos]
    inserted_videos = 0

    async with pool.acquire() as conn:
        for i in range(0, len(video_rows), BATCH_SIZE):
            batch = video_rows[i : i + BATCH_SIZE]
            await conn.executemany(
                """
                INSERT INTO videos (
                    id, creator_id, video_created_at,
                    views_count, likes_count, comments_count, reports_count,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO NOTHING
                """,
                batch,
            )
            inserted_videos += len(batch)

    snapshot_rows = []
    snapshot_id = 1
    for v in videos:
        video_id = v["id"]
        if isinstance(video_id, (int, float)):
            video_id = int(video_id)
        else:
            video_id = str(video_id).strip()
        for s in v.get("snapshots") or []:
            snapshot_rows.append(_row_snapshot(snapshot_id, video_id, s))
            snapshot_id += 1

    inserted_snapshots = 0
    async with pool.acquire() as conn:
        for i in range(0, len(snapshot_rows), BATCH_SIZE):
            batch = snapshot_rows[i : i + BATCH_SIZE]
            await conn.executemany(
                """
                INSERT INTO video_snapshots (
                    id, video_id, views_count, likes_count, comments_count, reports_count,
                    delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                batch,
            )
            inserted_snapshots += len(batch)

    return inserted_videos, inserted_snapshots


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, required=True, help="Path to videos.json")
    args = parser.parse_args()

    if not args.path.exists():
        raise SystemExit(f"File not found: {args.path}")

    pool = await create_pool()
    try:
        videos_count, snapshots_count = await load(args.path, pool)
        print(f"Inserted videos: {videos_count}")
        print(f"Inserted snapshots: {snapshots_count}")
    finally:
        await close_pool(pool)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
