"""
Скрипт для загрузки данных из JSON-файла в базу данных PostgreSQL.
"""
import json
import logging
from datetime import datetime
from typing import Dict, List

import asyncpg
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.batch_size = 100

    def _parse_datetime(self, dt_str: str) -> datetime | None:
        """Преобразует строку в формате ISO 8601 в объект datetime."""
        if dt_str is None:
            return None

        # Если строка заканчивается на 'Z', заменяем на '+00:00'
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'

        return datetime.fromisoformat(dt_str)

    async def load_json_file(self, filepath: str) -> None:
        logger.info(f"Начинаю загрузку данных из файла: {filepath}")

        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        videos = data.get('videos', [])
        logger.info(f"Найдено {len(videos)} видео для обработки.")

        conn = await asyncpg.connect(self.db_url)

        try:
            total_snapshots = 0
            for i in range(0, len(videos), self.batch_size):
                batch = videos[i:i + self.batch_size]
                await self._process_batch(conn, batch)
                total_snapshots += sum(
                    len(v.get('snapshots', [])) for v in batch
                )
                logger.info(
                    f"Обработано {min(i + self.batch_size, len(videos))}/"
                    f"{len(videos)} видео"
                )

            logger.info(
                f"Загрузка завершена. Успешно загружено: "
                f"{len(videos)} видео, {total_snapshots} снапшотов"
            )

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            raise
        finally:
            await conn.close()

    async def _process_batch(self, conn, batch: List[Dict]):
        async with conn.transaction():
            for video in batch:
                # Преобразуем строки дат в объекты datetime для видео
                video['video_created_at'] = self._parse_datetime(
                    video['video_created_at']
                )
                video['created_at'] = self._parse_datetime(video['created_at'])
                video['updated_at'] = self._parse_datetime(video['updated_at'])

                await self._insert_video(conn, video)

                # Обрабатываем снапшоты этого видео
                for snapshot in video.get('snapshots', []):
                    snapshot['created_at'] = self._parse_datetime(
                        snapshot['created_at']
                    )
                    snapshot['updated_at'] = self._parse_datetime(
                        snapshot['updated_at']
                    )

                    await self._insert_snapshot(conn, snapshot)

    async def _insert_video(self, conn, video: Dict):
        try:
            await conn.execute(
                """
                INSERT INTO videos (
                    id, creator_id, video_created_at, views_count,
                    likes_count, comments_count, reports_count,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                )
                ON CONFLICT (id) DO UPDATE SET
                    views_count = EXCLUDED.views_count,
                    updated_at = EXCLUDED.updated_at
                """,
                video['id'],
                video['creator_id'],
                video['video_created_at'],
                video['views_count'],
                video['likes_count'],
                video['comments_count'],
                video['reports_count'],
                video['created_at'],
                video['updated_at']
            )
        except Exception as e:
            logger.error(f"Ошибка при вставке видео {video['id']}: {e}")
            raise

    async def _insert_snapshot(self, conn, snapshot: Dict):
        try:
            await conn.execute(
                """
                INSERT INTO video_snapshots (
                    id, video_id, views_count, likes_count,
                    comments_count, reports_count,
                    delta_views_count, delta_likes_count,
                    delta_comments_count, delta_reports_count,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                )
                ON CONFLICT (id) DO UPDATE SET
                    views_count = EXCLUDED.views_count,
                    delta_views_count = EXCLUDED.delta_views_count,
                    updated_at = EXCLUDED.updated_at
                """,
                snapshot['id'],
                snapshot['video_id'],
                snapshot['views_count'],
                snapshot['likes_count'],
                snapshot['comments_count'],
                snapshot['reports_count'],
                snapshot['delta_views_count'],
                snapshot['delta_likes_count'],
                snapshot['delta_comments_count'],
                snapshot['delta_reports_count'],
                snapshot['created_at'],
                snapshot['updated_at']
            )
        except Exception as e:
            logger.error(f"Ошибка при вставке снапшота {snapshot['id']}: {e}")
            raise


async def main():
    # Настройки подключения
    DB_URL = "postgresql://video_user:video_pass123@localhost:5432/video_stats"
    JSON_FILE_PATH = "videos.json"

    loader = DataLoader(DB_URL)
    await loader.load_json_file(JSON_FILE_PATH)

if __name__ == "__main__":
    asyncio.run(main())
