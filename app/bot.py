from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode
import logging

from app.query_processor import query_processor
from app.database import db
from app.config import TELEGRAM_BOT_TOKEN

logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start."""
    welcome_text = (
        "Привет! Я бот для аналитики видео.\n\n"
        "Я могу ответить на вопросы о статистике видео, например:\n"
        "• Сколько всего видео в системе?\n"
        "• Сколько видео у креатора с id ...?\n"
        "• Сколько видео набрало больше N просмотров?\n"
        "• На сколько просмотров выросли видео за определённую дату?\n\n"
        "Просто задайте вопрос на русском языке!"
    )
    await message.answer(welcome_text)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help."""
    help_text = (
        "<b>Как пользоваться ботом:</b>\n\n"
        "Просто напишите вопрос на русском языке, например:\n"
        "• <code>Сколько всего видео есть в системе?</code>\n"
        "• <code>Сколько видео у креатора с id abc123?</code>\n"
        "• <code>Сколько видео набрало больше 100000 просмотров?</code>\n"
        "• <code>На сколько просмотров выросли все видео 28 ноября 2025?</code>\n\n"
        "Бот работает с данными из PostgreSQL и использует LLM для понимания запросов.\n\n"
        "<b>Важно:</b> Бот возвращает только числовой ответ."
    )
    await message.answer(help_text, parse_mode=ParseMode.HTML)


@dp.message()
async def handle_text_query(message: Message):
    """Основной обработчик текстовых запросов."""
    user_query = message.text.strip()

    # Игнорируем пустые сообщения и команды
    if not user_query or user_query.startswith('/'):
        return

    logger.info(f"Получен запрос от {message.from_user.id}: {user_query}")

    # Отправляем индикатор "печатает"
    await message.bot.send_chat_action(
        chat_id=message.chat.id, action="typing"
    )

    try:
        # 1. Преобразуем текст в SQL
        sql_query = await query_processor.text_to_sql(user_query)

        # 2. Выполняем SQL запрос
        result = await db.execute_query(sql_query)

        # 3. Отправляем результат (только число)
        await message.answer(result)

        logger.info(f"Успешный ответ: {result}")

    except Exception as e:
        logger.error(f"Ошибка обработки запроса: {e}")
        error_message = (
            "⚠️ Произошла ошибка при обработке запроса.\n\n"
            "Возможные причины:\n"
            "• Некорректный формат запроса\n"
            "• Ошибка в работе с базой данных\n"
            "• Проблема с API анализатора\n\n"
            "Попробуйте переформулировать вопрос или используйте /help для примеров."
        )
        await message.answer(error_message)
