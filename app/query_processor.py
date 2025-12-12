import openai
from app.config import OPENAI_API_KEY, LLM_MODEL, LLM_TEMPERATURE
import logging

logger = logging.getLogger(__name__)

# Инициализация клиента OpenAI
client = openai.OpenAI(api_key=OPENAI_API_KEY)


class QueryProcessor:
    def __init__(self):
        self.system_prompt = self._create_system_prompt()

    def _create_system_prompt(self):
        """Создает системный промпт с описанием схемы БД."""
        return """
Ты — опытный SQL-аналитик. Твоя задача — преобразовывать вопросы на русском языке о статистике видео в корректные SQL-запросы к PostgreSQL.

СТРУКТУРА БАЗЫ ДАННЫХ:

1. ТАБЛИЦА videos (итоговая статистика по каждому видео):
   - id (UUID) — уникальный идентификатор видео
   - creator_id (VARCHAR) — ID креатора (строка)
   - video_created_at (TIMESTAMPTZ) — дата и время публикации видео
   - views_count (INTEGER) — финальное количество просмотров
   - likes_count (INTEGER) — финальное количество лайков
   - comments_count (INTEGER) — финальное количество комментариев
   - reports_count (INTEGER) — финальное количество жалоб
   - created_at (TIMESTAMPTZ) — время создания записи
   - updated_at (TIMESTAMPTZ) — время обновления

2. ТАБЛИЦА video_snapshots (почасовые снапшоты статистики):
   - id (VARCHAR) — ID снапшота
   - video_id (UUID) — ссылка на видео (FOREIGN KEY к videos.id)
   - views_count (INTEGER) — просмотры на момент снапшота
   - likes_count (INTEGER) — лайки на момент снапшота
   - comments_count (INTEGER) — комментарии на момент снапшота
   - reports_count (INTEGER) — жалобы на момент снапшота
   - delta_views_count (INTEGER) — прирост просмотров за час
   - delta_likes_count (INTEGER) — прирост лайков за час
   - delta_comments_count (INTEGER) — прирост комментариев за час
   - delta_reports_count (INTEGER) — прирост жалоб за час
   - created_at (TIMESTAMPTZ) — время создания снапшота (раз в час)
   - updated_at (TIMESTAMPTZ) — время обновления

ВАЖНЫЕ ПРАВИЛА:
1. ВСЕГДА возвращай ТОЛЬКО SQL-запрос, без каких-либо пояснений, примечаний или форматирования.
2. Запрос должен возвращать ОДНО ЧИСЛО (используй COUNT(), SUM(), COUNT(DISTINCT ...)).
3. Даты в SQL указывай в формате 'YYYY-MM-DD'.
4. Для извлечения даты из TIMESTAMPTZ используй функцию DATE().
5. ID креатора — это строка, обязательно оборачивай в одинарные кавычки.
6. Не используй LIMIT, OFFSET — нам нужно точное число.
7. Для подсчета уникальных видео используй COUNT(DISTINCT video_id).
8. Если нужно считать за период, используй BETWEEN 'дата1' AND 'дата2'.

ПРИМЕРЫ ПРЕОБРАЗОВАНИЯ:

Вопрос: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: "Сколько видео набрало больше 100000 просмотров за всё время?"
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

Вопрос: "Сколько видео получило новые лайки 26 ноября 2025?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-26' AND delta_likes_count > 0;

Вопрос: "Какая сумма просмотров у всех видео креатора с id test123?"
SQL: SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE creator_id = 'test123';
"""

    async def text_to_sql(self, user_query: str) -> str:
        """Преобразует текстовый запрос пользователя в SQL."""
        try:
            logger.info(f"Преобразую запрос в SQL: {user_query}")

            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=500
            )

            sql_query = response.choices[0].message.content.strip()
            logger.info(f"Получен SQL: {sql_query}")

            # Очистка ответа: убираем возможные backticks и лишние пробелы
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:-3].strip()
            elif sql_query.startswith("```"):
                sql_query = sql_query[3:-3].strip()

            return sql_query

        except Exception as e:
            logger.error(f"Ошибка при работе с LLM: {e}")
            raise Exception(f"Не удалось обработать запрос. Ошибка: {str(e)}")


# Глобальный экземпляр процессора
query_processor = QueryProcessor()
