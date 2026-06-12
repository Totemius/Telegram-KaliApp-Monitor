# Telegram KaliApp Monitor - A System Of Telegram Channels/Chat Auto-Archivation 
[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-13+-blue.svg)](https://postgresql.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

KaliApp — это мощная, production-ориентированная система мониторинга, архивации и анализа Telegram-чатов (каналы, супергруппы, группы). Основная идея: вы берёте любой Telegram-канал и автоматически публикуете его содержимое на своём сайте/домене, минуя блокировки. Решение для надежной публикации контента из Telegram с настраиваемой инфраструктурой.

## 👥 Кому подходит

- **Издателям** — защита от блокировок основного канала
- **Читателям** — доступ к контенту без VPN
- **Администраторам** — полный контроль над инфраструктурой

## ✨ Преимущества

| Возможность | Описание |
|-------------|---------|
| ✅ **Стабильность** | Работает при сбоях и замедлениях Telegram |
| ✅ **Доступность** | Не требует VPN и регистрации |
| ✅ **Управление** | Полностью управляемый контур публикаций |
| ✅ **Автономность** | Публикации в реальном времени без участия пользователя |

## 🛡️ Без юридических рисков

Настройка хранения данных:

- **База данных:** PostgreSQL (РФ)
- **Медиафайлы:** S3 + CDN (РФ)
- **Инфраструктура:** Полностью локализована

## 📦 Требования

- **Python 3.9+**
- **PostgreSQL 13+** (с расширениями pg_trgm, vector)
- **S3-совместимое хранилище** (Yandex Cloud, Beget, AWS)
- **Telegram API** (api_id и api_hash с my.telegram.org)

## 🔧 Установка

1. Клонируйте репозиторий
git clone https://github.com/Totemius/Telegram-KaliApp-Monitor.git
cd Telegram-KaliApp-Monitor

2. Установите зависимости
pip install -r requirements.txt

3. Создайте файл .env (см. пример ниже)
cp .env.example .env

4. Инициализируйте базу данных
python -c "import asyncio; from db_module import init_db; asyncio.run(init_db())"

5. Запустите программу
python main.py

## ⚙️ Конфигурация (.env)

# Telegram API

API_ID=12345678
API_HASH=your_api_hash
PHONE=+79991234567

# Database

DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=telegram_mirror

# S3 Storage (опционально, для медиа)

S3_BUCKET=your-bucket
S3_PUBLIC_URL=https://cdn.yourdomain.com
S3_ENDPOINT=https://s3.yandexcloud.net
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret

## 📁 Структура проекта

# Telegram-KaliApp-Monitor

├── main.py                 # Главное меню и оркестрация

├── telegram_client.py      # Работа с Telegram API

├── realtime_recorder.py    # Запись в реальном времени

├── db_module.py           # Работа с PostgreSQL

├── s3_uploader.py         # Загрузка медиа в S3

├── s3_recovery.py         # Восстановление пропущенных загрузок

├── process_event_queue.py # Фоновая обработка (NER, ключевые слова)

├── import_module.py       # Массовый импорт сообщений

├── config.py              # Конфигурация

├── logger.py              # Логирование

├── utils.py               # Вспомогательные функции

├── requirements.txt       # Зависимости

└── .env.example           # Пример конфигурации


