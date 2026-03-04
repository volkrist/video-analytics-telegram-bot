import asyncio
from datetime import datetime

from app.db.pool import create_pool, close_pool
from app.db import queries


async def main():
    pool = await create_pool()

    print("total videos:", await queries.count_videos_total(pool))

    print("views > 1000:", await queries.count_videos_views_gt(pool, 1000))

    print(
        "sum delta views:",
        await queries.sum_delta_views_on_date(pool, datetime(2025, 11, 28)),
    )

    await close_pool(pool)


if __name__ == "__main__":
    asyncio.run(main())
