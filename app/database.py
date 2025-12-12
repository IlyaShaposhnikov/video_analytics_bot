import asyncpg
from app.config import DATABASE_URL


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Создает пул подключений к БД."""
        self.pool = await asyncpg.create_pool(DATABASE_URL)

    async def disconnect(self):
        """Закрывает пул подключений."""
        if self.pool:
            await self.pool.close()

    async def execute_query(self, sql_query: str) -> str:
        """
        Выполняет SQL-запрос и возвращает результат как строку.
        Ожидается, что запрос возвращает одно значение.
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as connection:
            try:
                # Выполняем запрос
                result = await connection.fetchval(sql_query)

                # Преобразуем результат в строку
                if result is None:
                    return "0"
                return str(result)

            except Exception as e:
                raise Exception(f"Ошибка выполнения SQL: {e}")


# Глобальный экземпляр БД
db = Database()
