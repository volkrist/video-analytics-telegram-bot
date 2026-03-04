import asyncpg

from app.config import settings


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.DATABASE_URL,
        min_size=1,
        max_size=10,
    )


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()


async def check_connection(pool: asyncpg.Pool) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT 1")
    return row == 1
