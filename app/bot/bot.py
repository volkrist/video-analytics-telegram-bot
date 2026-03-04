"""
Telegram-бот: одно текстовое сообщение → одно число.
Полностью async, пул БД создаётся при старте и закрывается при остановке.
"""
import asyncio
import logging

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

from app.config import settings
from app.db.pool import create_pool, close_pool
from app.nlp.llm import get_intent
from app.nlp.intent_parser import execute_intent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _register_handlers(dp: Dispatcher, pool: asyncpg.Pool) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        await message.answer("ok")

    @dp.message(F.text)
    async def on_text(message: Message) -> None:
        text = (message.text or "").strip()
        if not text:
            await message.answer("0")
            return
        logger.info("text=%r", text)
        intent = None
        try:
            intent = await get_intent(text)
            logger.info("intent=%s", intent)
            result = await execute_intent(pool, intent)
            logger.info("result=%s", result)
            await message.answer(str(result))
        except Exception as e:
            logger.exception("Intent/execute error: %s", e)
            logger.warning("Failed intent (see above for SQL/params if DB error): %s", intent)
            await message.answer("0")


async def run() -> None:
    pool = await create_pool()
    bot = Bot(token=settings.BOT_TOKEN)
    dp = Dispatcher()
    _register_handlers(dp, pool)
    try:
        await dp.start_polling(bot)
    finally:
        await close_pool(pool)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
