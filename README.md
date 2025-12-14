# Video Analytics Bot
Telegram-бот, который по запросу на естественном русском языке возвращает аналитику по статистике видео из PostgreSQL базы данных

## Технологический стек

- **Backend**: Python 3.9+
- **Telegram Bot Framework**: Aiogram 3.7+
- **Database**: PostgreSQL 17+
- **LLM для NLP**: Ollama с локальной моделью (например, `qwen2.5:0.5b`)
- **Асинхронный драйвер БД**: asyncpg

## Предварительные требования

Перед началом убедитесь, что на системе установлены:
1. **Python 3.9+** (проверьте: `python --version`)
2. **PostgreSQL 17+** (проверьте: `psql --version`)
3. **Git** (для клонирования репозитория)
4. **Ollama** (для работы локальной LLM модели)

## Пошаговая инструкция по разворачиванию проекта

### 1. Клонирование репозитория и настройка окружения

```bash
# Клонируйте репозиторий
git clone https://github.com/yourusername/video_analytics_bot.git
cd video_analytics_bot

# Создайте и активируйте виртуальное окружение
py -3.9 -m venv venv

# Для Windows (Bash/Git Bash):
source venv/Scripts/activate
# Для Linux/macOS:
source venv/bin/activate

# Установите зависимости
pip install -r requirements.txt
```

### 2. Настройка базы данных PostgreSQL
```bash
# Подключитесь к PostgreSQL как суперпользователь
psql -U postgres

# В интерактивной консоли PostgreSQL выполните:
CREATE DATABASE video_stats;
CREATE USER video_user WITH PASSWORD 'video_pass123';
GRANT ALL PRIVILEGES ON DATABASE video_stats TO video_user;
\q

# Предоставьте дополнительные права пользователю video_user
psql -U postgres -d video_stats
-- Предоставляем права на использование схемы public
GRANT USAGE ON SCHEMA public TO video_user;
-- Предоставляем права на создание объектов в схеме public
GRANT CREATE ON SCHEMA public TO video_user;
-- Предоставляем все права на будущие таблицы
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO video_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO video_user;
-- Если таблицы уже частично созданы, предоставляем права на существующие объекты
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO video_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO video_user;
\q
```

### 3. Установка и настройка Ollama
```bash
# Установите Ollama с официального сайта:
# https://ollama.com/download

# Скачайте легкую модель
ollama pull qwen2.5:0.5b

# Запустите Ollama сервер (обычно запускается автоматически как служба)
# При необходимости запустите вручную:
ollama serve
```

### 4. Настройка конфигурационных файлов
```bash
# Скопируйте пример конфигурации
cp .env.example .env

# Отредактируйте .env файл, указав свои настройки:
nano .env  # или используйте любой текстовый редактор
```

Содержимое файла .env:

```env
# Telegram Bot Token (получите у @BotFather)
TELEGRAM_BOT_TOKEN=ваш_токен_бота

# Database configuration
DATABASE_URL=postgresql://video_user:video_pass123@localhost:5432/video_stats

# LLM Model for Ollama
LLM_MODEL=qwen2.5:0.5b
```

### 5. Инициализация базы данных и загрузка тестовых данных
```bash
# Создайте таблицы в базе данных
python scripts/init_db.py

# Загрузите тестовые данные из JSON-файла
# Убедитесь, что файл videos.json находится в корне проекта
python scripts/load_data.py
```

### 6. Запуск Telegram-бота
```bash
# Запустите бота
python app/main.py
```

Ожидаемый вывод при успешном запуске:

```text
2025-12-14 15:30:45,123 - __main__ - INFO - Запуск бота...
2025-12-14 15:30:45,456 - __main__ - INFO - Подключение к БД установлено
2025-12-14 15:30:46,789 - __main__ - INFO - Бот успешно запущен!
2025-12-14 15:30:46,789 - aiogram.dispatcher - INFO - Start polling
```

## Использование бота
1. Найдите бота в Telegram по username, указанному при создании
2. Отправьте команду /start для получения приветственного сообщения
3. Задавайте вопросы на естественном русском языке, например:
- Сколько всего видео есть в системе?
- Сколько видео у креатора с id abc123?
- Сколько видео набрало больше 100000 просмотров?
- На сколько просмотров выросли все видео 28 ноября 2025?
- Сколько разных видео получали новые просмотры 27 ноября 2025?

Важно: Бот возвращает только числовой ответ без дополнительного текста.

## Структура проекта
```text
video_analytics_bot/
├── app/                   # Основное приложение
│   ├── __init__.py
│   ├── main.py            # Точка входа, запуск бота
│   ├── bot.py             # Обработчики Telegram-бота
│   ├── query_processor.py # Преобразование текста в SQL (LLM)
│   ├── database.py        # Работа с PostgreSQL
│   └── config.py          # Конфигурация
├── scripts/               # Вспомогательные скрипты
│   ├── init_db.py         # Создание таблиц в БД
│   └── load_data.py       # Загрузка данных из JSON
├── requirements.txt       # Зависимости Python
├── videos.json            # Тестовые данные
├── .env.example           # Шаблон конфигурации
├── .gitignore
└── README.md              # Этот файл
```

## Архитектура и ключевые решения
Преобразование естественного языка в SQL

Проблема: Нужно преобразовывать свободные текстовые запросы пользователей в корректные SQL-запросы.

Решение: Использование LLM (Large Language Model) с тщательно составленным системным промптом.

Реализация:

1. Системный промпт содержит:
- Детальное описание схемы базы данных (2 таблицы: videos и video_snapshots)
- Строгие правила формирования ответа (только SQL, одно число, форматы дат)
- Примеры корректных преобразований
2. Очистка ответа LLM: Модель иногда добавляет префиксы ("SQL:", "Запрос:"), поэтому реализована агрессивная очистка:
- Удаление блоков кода с обратными кавычками
- Удаление префиксов через регулярные выражения
- Поиск начала SQL-запроса по ключевым словам (SELECT, INSERT и т.д.)

## Выбор LLM провайдера
Проблема: Облачные API (OpenAI, Yandex GPT) имеют ограничения по квотам и требуют оплаты.

Решение: Использование локальной модели через Ollama:

- Бесплатно и без лимитов
- Работает оффлайн
- Быстрый отклик для небольших моделей
- Конфиденциальность данных

Выбранная модель: qwen2.5:0.5b — оптимальна для задачи, баланс между качеством и скоростью.

## Оптимизация работы с базой данных
1. Индексы для ускорения часто используемых запросов:

```sql
CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
```

2. Пул соединений через asyncpg для эффективной работы в асинхронном режиме.
3. Пакетная загрузка данных из JSON для производительности.

## Автор
Шапошников Илья Андреевич

ilia.a.shaposhnikov@gmail.com