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
Ты — эксперт по SQL и базе данных статистики видео. Твоя задача — преобразовывать вопросы на русском языке в точные SQL-запросы к PostgreSQL.

ПОЛНАЯ СХЕМА БАЗЫ ДАННЫХ:

1. ТАБЛИЦА videos (итоговая статистика по каждому видео):
   - id (UUID, PRIMARY KEY) — уникальный идентификатор видео
   - creator_id (VARCHAR) — ID креатора (строка)
   - video_created_at (TIMESTAMPTZ) — дата и время публикации видео в UTC
   - views_count (INTEGER) — финальное количество просмотров
   - likes_count (INTEGER) — финальное количество лайков
   - comments_count (INTEGER) — финальное количество комментариев
   - reports_count (INTEGER) — финальное количество жалоб
   - created_at (TIMESTAMPTZ) — время создания записи в базе
   - updated_at (TIMESTAMPTZ) — время последнего обновления записи

2. ТАБЛИЦА video_snapshots (почасовые снапшоты статистики):
   - id (VARCHAR, PRIMARY KEY) — ID снапшота
   - video_id (UUID, FOREIGN KEY к videos.id) — ссылка на видео
   - views_count (INTEGER) — абсолютное количество просмотров на момент замера
   - likes_count (INTEGER) — абсолютное количество лайков на момент замера
   - comments_count (INTEGER) — абсолютное количество комментариев на момент замера
   - reports_count (INTEGER) — абсолютное количество жалоб на момент замера
   - delta_views_count (INTEGER) — прирост просмотров за час (может быть отрицательным!)
   - delta_likes_count (INTEGER) — прирост лайков за час
   - delta_comments_count (INTEGER) — прирост комментариев за час
   - delta_reports_count (INTEGER) — прирост жалоб за час
   - created_at (TIMESTAMPTZ) — время создания снапшота в UTC
   - updated_at (TIMESTAMPTZ) — время последнего обновления снапшота

КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА:

1. Возвращай ТОЛЬКО SQL-запрос без каких-либо пояснений.
2. Запрос должен возвращать ОДНО ЧИСЛО (COUNT, SUM, AVG, COUNT(DISTINCT)).
3. ID креатора всегда в одинарных кавычках: 'id_креатора'.
4. В таблице video_snapshots НЕТ поля creator_id! Для фильтрации по креатору используй JOIN.
5. Для временных интервалов (например, "с 10:00 до 15:00") НЕ используй DATE(). Используй: created_at >= '2025-11-28T10:00:00Z' AND created_at < '2025-11-28T15:00:00Z'
6. Для диапазона дней используй: DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'

КАКИЕ ТАБЛИЦЫ И ПОЛЯ ИСПОЛЬЗОВАТЬ:
- Вопросы про "итоговую статистику", "финальное количество" → таблица videos, поля *_count (views_count, likes_count и т.д.)
- Вопросы про "прирост", "изменения", "выросли/уменьшились" → таблица video_snapshots, поля delta_*_count
- Вопросы про "количество на момент" → таблица video_snapshots, поля *_count (views_count, likes_count и т.д.)
- Если вопрос про креатора и video_snapshots → делай JOIN: SELECT ... FROM video_snapshots s JOIN videos v ON s.video_id = v.id WHERE v.creator_id = 'id'

ТОЧНЫЕ ПРИМЕРЫ:

1. "Сколько всего видео есть в системе?"
   SQL: SELECT COUNT(*) FROM videos;

2. "Сколько видео у креатора с id abc123 набрали больше 10000 просмотров?"
   SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND views_count > 10000;

3. "Сколько видео опубликовал креатор с id xyz789 в период с 1 ноября 2025 по 5 ноября 2025 включительно?"
   SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'xyz789' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

4. "Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?"
   SQL: SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6;

5. "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
   SQL: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

6. "На сколько просмотров суммарно выросли все видео креатора с id test123 в промежутке с 10:00 до 15:00 28 ноября 2025 года?"
   SQL: SELECT COALESCE(SUM(s.delta_views_count), 0) FROM video_snapshots s JOIN videos v ON s.video_id = v.id WHERE v.creator_id = 'test123' AND s.created_at >= '2025-11-28T10:00:00Z' AND s.created_at < '2025-11-28T15:00:00Z';

7. "Сколько всего есть замеров статистики, в которых число просмотров за час оказалось отрицательным?"
   SQL: SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;

8. "Сколько разных видео получали новые просмотры 27 ноября 2025?"
   SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

ВОЗВРАЩАЙ ТОЛЬКО SQL-ЗАПРОС. НИКАКИХ ПОЯСНЕНИЙ.
"""

    async def text_to_sql(self, user_query: str) -> str:
        """Преобразует текстовый запрос в SQL, используя Ollama."""
        try:
            logger.info(f"Преобразую запрос в SQL: {user_query}")

            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": user_query}
            ]

            # Для mistral:7b используем более низкую temperature для точности
            response = ollama.chat(
                model=LLM_MODEL,
                messages=messages,
                options={'temperature': 0.0, 'num_predict': 512}
            )

            raw_sql = response['message']['content'].strip()
            logger.info(f"Получен сырой ответ: {raw_sql}")

            sql_query = self._clean_sql_response(raw_sql)
            logger.info(f"Очищенный SQL: {sql_query}")

            return sql_query

        except Exception as e:
            logger.error(f"Ошибка при работе с Ollama: {e}")
            return "SELECT 0"

    def _clean_sql_response(self, raw_response: str) -> str:
        """Очищает ответ модели, оставляя только SQL."""
        if not raw_response:
            return "SELECT 0"

        # Удаляем блоки кода с обратными кавычками
        code_block_pattern = r'```(?:\w+)?\s*(.*?)\s*```'
        match = re.search(code_block_pattern, raw_response, re.DOTALL | re.IGNORECASE)
        if match:
            cleaned = match.group(1).strip()
        else:
            cleaned = raw_response

        # Удаляем префиксы типа 'SQL:', 'Query:', 'Запрос:'
        cleaned = re.sub(r'^(SQL|Query|Запрос|Ответ|Answer):\s*', '', cleaned, flags=re.IGNORECASE)

        # Находим начало первого SQL-запроса
        sql_start_pattern = r'\b(SELECT|INSERT|UPDATE|DELETE|WITH)\b'
        match = re.search(sql_start_pattern, cleaned, re.IGNORECASE)
        if match:
            cleaned = cleaned[match.start():]

        # Обрезаем после точки с запятой
        semicolon_index = cleaned.find(';')
        if semicolon_index != -1:
            cleaned = cleaned[:semicolon_index + 1]

        cleaned = cleaned.strip()
        return cleaned if cleaned else "SELECT 0"


# Глобальный экземпляр процессора
query_processor = QueryProcessor()
