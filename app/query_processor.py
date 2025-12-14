import ollama
import logging
import re
from app.config import LLM_MODEL

logger = logging.getLogger(__name__)


class QueryProcessor:
    def __init__(self):
        self.system_prompt = self._create_system_prompt()

    def _create_system_prompt(self):
        """Создает системный промпт с описанием схемы БД."""
        return """
Ты — опытный SQL-аналитик. Твоя задача — преобразовывать вопросы на русском языке о статистике видео в корректные SQL-запросы к PostgreSQL.

ВАЖНЕЙШЕЕ ПРАВИЛО:
1. Твой ответ должен содержать ТОЛЬКО SQL-запрос, и ничего больше.
2. НИКОГДА не пиши перед запросом слова "SQL:", "Запрос:", "Ответ:" или любые другие пояснения.
3. НИКОГДА не используй форматирование с обратными кавычками (```sql ... ```).
4. Запрос должен возвращать ОДНО ЧИСЛО (используй COUNT(), SUM(), COUNT(DISTINCT ...)).
5. Даты в SQL указывай в формате 'YYYY-MM-DD'.
6. Для извлечения даты из TIMESTAMPTZ используй функцию DATE().
7. ID креатора — это строка, обязательно оборачивай в одинарные кавычки.

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
"""

    async def text_to_sql(self, user_query: str) -> str:
        """Преобразует текстовый запрос в SQL, используя локальную модель через Ollama."""
        try:
            logger.info(f"Преобразую запрос в SQL: {user_query}")

            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_query}
            ]

            response = ollama.chat(
                model=LLM_MODEL,
                messages=messages,
                options={'temperature': 0.1}
            )

            raw_sql = response['message']['content'].strip()
            logger.info(f"Получен сырой ответ: {raw_sql}")

            # Улучшенная очистка ответа
            sql_query = self._clean_sql_response(raw_sql)

            logger.info(f"Очищенный SQL: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"Ошибка при работе с Ollama: {e}")
            return "SELECT COUNT(*) FROM videos"

    def _clean_sql_response(self, raw_response: str) -> str:
        """
        Агрессивно очищает ответ модели, оставляя только SQL.
        Удаляет префиксы 'SQL:', обратные кавычки и случайный текст.
        """
        if not raw_response:
            return "SELECT COUNT(*) FROM videos"

        # 1. Удаляем блоки кода с обратными кавычками (```sql ... ```)
        code_block_pattern = r'```(?:\w+)?\s*(.*?)\s*```'
        match = re.search(
            code_block_pattern, raw_response, re.DOTALL | re.IGNORECASE
        )
        if match:
            cleaned = match.group(1).strip()
        else:
            cleaned = raw_response

        # 2. Удаляем префиксы типа 'SQL:', 'Query:', 'Запрос:
        #  с любым регистром
        cleaned = re.sub(
            r'^(SQL|Query|Запрос|Ответ|Answer):\s*', '',
            cleaned, flags=re.IGNORECASE
        )

        # 3. Находим начало первого SQL-запроса (ищем ключевые слов
        # SELECT, INSERT, UPDATE, DELETE, WITH)
        sql_start_pattern = r'\b(SELECT|INSERT|UPDATE|DELETE|WITH)\b'
        match = re.search(sql_start_pattern, cleaned, re.IGNORECASE)
        if match:
            cleaned = cleaned[match.start():]

        # 4. Удаляем все, что идет после закрывающей
        # точки с запятой (если она есть)
        semicolon_index = cleaned.find(';')
        if semicolon_index != -1:
            cleaned = cleaned[:semicolon_index + 1]

        # 5. Удаляем лишние пробелы и переносы строк
        cleaned = cleaned.strip()

        # 6. Если после всех чисток строка пуста,
        # возвращаем запрос по умолчанию
        if not cleaned:
            cleaned = "SELECT COUNT(*) FROM videos"

        return cleaned


# Глобальный экземпляр процессора
query_processor = QueryProcessor()
