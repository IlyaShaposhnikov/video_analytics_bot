import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging

# Импортируем dp и уже созданный объект bot из app.bot
from app.bot import dp, bot
from app.database import db

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def on_startup():
    """Действия при запуске бота."""
    logger.info("Запуск бота...")

    # Подключаемся к базе данных
    await db.connect()
    logger.info("Подключение к БД установлено")

    # Устанавливаем команды бота (используем импортированный bot)
    await bot.set_my_commands([
        {"command": "start", "description": "Запустить бота"},
        {"command": "help", "description": "Помощь по использованию"}
    ])

    logger.info("Бот успешно запущен!")


async def on_shutdown():
    """Действия при остановке бота."""
    logger.info("Остановка бота...")

    # Закрываем соединение с БД
    await db.disconnect()
    logger.info("Соединение с БД закрыто")


async def main():
    """Основная функция запуска бота."""
    try:
        # Выполняем действия при запуске
        await on_startup()

        # Запускаем бота (используем импортированные dp и bot)
        await dp.start_polling(bot)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

    finally:
        # Выполняем действия при остановке
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
