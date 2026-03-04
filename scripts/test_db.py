import asyncio

from app.db.pool import check_connection, close_pool, create_pool


async def main():
    pool = await create_pool()
    try:
        ok = await check_connection(pool)
        print("DB connection:", ok)
    finally:
        await close_pool(pool)


if __name__ == "__main__":
    asyncio.run(main())
