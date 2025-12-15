import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь Python
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import asyncpg
import asyncio

from app.config import DATABASE_URL


async def create_tables():
    """Создаёт таблицы в базе данных PostgreSQL."""

    create_videos_table = """
    CREATE TABLE IF NOT EXISTS videos (
        id UUID PRIMARY KEY,
        creator_id VARCHAR(255) NOT NULL,
        video_created_at TIMESTAMPTZ NOT NULL,
        views_count INTEGER DEFAULT 0,
        likes_count INTEGER DEFAULT 0,
        comments_count INTEGER DEFAULT 0,
        reports_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_snapshots_table = """
    CREATE TABLE IF NOT EXISTS video_snapshots (
        id VARCHAR(255) PRIMARY KEY,
        video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
        views_count INTEGER DEFAULT 0,
        likes_count INTEGER DEFAULT 0,
        comments_count INTEGER DEFAULT 0,
        reports_count INTEGER DEFAULT 0,
        delta_views_count INTEGER DEFAULT 0,
        delta_likes_count INTEGER DEFAULT 0,
        delta_comments_count INTEGER DEFAULT 0,
        delta_reports_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_indexes = """
    CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
    CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
    CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
    CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
    """

    print("Подключаемся к базе данных...")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("Подключение успешно!")

        print("Создаём таблицу videos...")
        await conn.execute(create_videos_table)

        print("Создаём таблицу video_snapshots...")
        await conn.execute(create_snapshots_table)

        print("Создаём индексы для ускорения запросов...")
        await conn.execute(create_indexes)

        print("\nТаблицы успешно созданы!")

        # Проверим создание
        tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
            """
        )

        print("\nСозданные таблицы:")
        for table in tables:
            print(f"• {table['table_name']}")

        await conn.close()

    except asyncpg.InvalidPasswordError:
        print("Ошибка: Неверный пароль для подключения к БД")
        print(
            "Проверьте DATABASE_URL в файле .env: "
            f"{DATABASE_URL[:50]}..."
            )

    except asyncpg.ConnectionDoesNotExistError:
        print("Ошибка: Не удалось подключиться к БД")
        print("Проверьте:")
        print("   1. Запущен ли PostgreSQL: pg_ctl status")
        print("   2. Правильно ли указаны данные в DATABASE_URL")
        print("   3. Существует ли база 'video_stats'")

    except Exception as e:
        print(
            f"Неизвестная ошибка: {type(e).__name__}: {e}"
        )


async def main():
    await create_tables()

if __name__ == "__main__":
    asyncio.run(main())
