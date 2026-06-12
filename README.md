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
| ✅ **Гибкость адресации** | Любое имя домена (.ru, .com и другие) |
| ✅ **Архивация** | Полный онлайн-архив постов и медиа |
| ✅ **Форматы** | Режим приложения и мульти-канал |
| ✅ **Дизайн** | Кастомизация интерфейса |
| ✅ **Аналитика** | Статистика и интеграции |

## ⚠️ Что это не делает

❌ Не альтернатива Telegram
❌ Не новая социальная сеть
❌ Не агрегатор каналов
❌ Не рекламная сеть

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

# 1. Клонируйте репозиторий
git clone https://github.com/Totemius/Telegram-KaliApp-Monitor.git

cd telegram-mirror-system

# 2. Установите зависимости:
pip install -r requirements.txt

# 3. Создайте файл .env (см. пример ниже):
cp .env.example .env

# 4. Инициализируйте базу данных:
python -c "import asyncio; from db_module import init_db; asyncio.run(init_db())"

# 5. Запустите программу:
python main.py

## ⚙️ Конфигурация (.env)

# ===== Telegram API =====

Получить на https://my.telegram.org/apps

API_ID=

API_HASH=

PHONE=

# ===== База данных PostgreSQL =====

DB_HOST=localhost

DB_USER=postgres

DB_PASSWORD=

DB_NAME=telegram_mirror

# ===== S3 хранилище (опционально) =====

Для загрузки медиафайлов в облако:

S3_BUCKET=

S3_PUBLIC_URL=

S3_ENDPOINT=https://s3.yandexcloud.net

S3_REGION=ru-central-1

AWS_ACCESS_KEY_ID=

AWS_SECRET_ACCESS_KEY=

S3_UPLOAD_ON_FINISH=true

S3_MAX_QUEUE_SIZE=1000

# ===== Настройки пула соединений =====

POOL_MIN_SIZE=5

POOL_MAX_SIZE=20

POOL_COMMAND_TIMEOUT=60

# ===== S3 Recovery =====

S3_RECOVERY_BATCH_SIZE=50

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

## 🎮 Использование

После запуска main.py откроется интерактивное меню:

1. Инициализация БД
2. Запуск Telegram клиента
3. Запись в БД [OFF]
4. Управление источниками
5. Импорт в БД
6. Просмотр и поиск

## 🔄 Архитектура

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Telegram API                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    telegram_client.py + realtime_recorder.py                │
│                    (Python - получение и сохранение сообщений)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL Database                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐       │
│  │   chats      │ │   messages   │ │   users      │ │  media_files │       │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘       │
│                                                                             │
│  🔔 NOTIFY (new_message, edit_message, delete_message, media_ready, media_status)
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         server-mirror.js (Node.js)                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  • LISTEN на уведомления PostgreSQL                                  │   │
│  │  • HTTP API для истории и медиа (/api/channel/posts, /api/media)    │   │
│  │  • WebSocket сервер для реального времени                            │   │
│  │  • Буфер событий (Event Replay Buffer) - 1000 последних событий      │   │
│  │  • Управление подписками клиентов на каналы                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
            ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
            │  Browser    │   │  Browser    │   │  Browser    │
            │  core.js    │   │  core.js    │   │  core.js    │
            │  (Client 1) │   │  (Client 2) │   │  (Client N) │
            └─────────────┘   └─────────────┘   └─────────────┘

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Telegram   │────▶│   Система   │────▶│ PostgreSQL  │
│    API      │     │  Mirror     │     │  (архив)    │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │  S3 + CDN   │◀────│   Медиа     │
                    │  (файлы)    │     │   файлы     │
                    └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Веб-сайт  │
                    │  (HTML/JS)  │
                    └─────────────┘

Пример записи сообщения
├── Telegram API
├── 
│   ├── Сканирование fresh каждые N секунд
│   ├── Сортировка по времени
│   └── Инициирование перемещений
├── Очистка (cleaner_thread)
│   ├── Рекурсивный обход archive
│   ├── Удаление старых файлов
│   └── Очистка пустых директорий
└── Пул копирования (CharonWorkerPool)
    ├── 1-32 рабочих потоков
    ├── RingBuffer задач
    └── Адаптивная регулировка

## 🖼️ Демонстрация работы

Публикации в реальном времени и последние посты (до 100):

| Источник | Демо |
|----------|------|
| Раньше всех. Ну почти. | [Перейти](https://labubugram.github.io/tg/bbbreaking/) |
| ТАСС | [Перейти](https://labubugram.github.io/tg/tass_agency/) |
| РИА Новости | [Перейти](https://labubugram.github.io/tg/rian_ru/) |
| РБК. Новости. Главное | [Перейти](https://labubugram.github.io/tg/rbc_news/) |
| Дмитрий Медведев | [Перейти](https://labubugram.github.io/tg/medvedev_telegram/) |
| AGDchan | [Перейти](https://labubugram.github.io/tg/Agdchan/) |
| Светское советское! | [Перейти](https://labubugram.github.io/tg/svetskoe/) |
| Disclose.tv | [Перейти](https://labubugram.github.io/tg/disclosetv/) |
| Fotros Resistance | [Перейти](https://labubugram.github.io/tg/FotrosResistancee/) |
| HardMeme Cafe | [Перейти](https://labubugram.github.io/tg/hardmemecafe/) |
| Insider Paper | [Перейти](https://labubugram.github.io/tg/insiderpaper/) |
| Machine-Dependent | [Перейти](https://labubugram.github.io/tg/machinedependent/) |
| N + 1 | [Перейти](https://labubugram.github.io/tg/nplusone/) |
| One America News Network | [Перейти](https://labubugram.github.io/tg/OANNTV/) |
| opennet.ru | [Перейти](https://labubugram.github.io/tg/opennet_ru/) |
| OpenNews | [Перейти](https://labubugram.github.io/tg/opennews/) |
| SecLab советы | [Перейти](https://labubugram.github.io/tg/SecLabm/) |
| SecurityLab.ru | [Перейти](https://labubugram.github.io/tg/SecLabNews/) |
| Sou Wan | [Перейти](https://labubugram.github.io/tg/SouWan/) |
| War Monitor | [Перейти](https://labubugram.github.io/tg/warmonitors/) |
| WarCabinet | [Перейти](https://labubugram.github.io/tg/warcabinet/) |
| ZОРЧ🚩 | [Перейти](https://labubugram.github.io/tg/ZOp4_telega/) |
| а вот мой яндекс кошелек | [Перейти](https://labubugram.github.io/tg/lastoppo/) |
| Александр ДроZденко | [Перейти](https://labubugram.github.io/tg/drozdenko_au_lo/) |
| Александр Панчин | [Перейти](https://labubugram.github.io/tg/ScienceInquisition/) |
| Безвольные каменщики | [Перейти](https://labubugram.github.io/tg/kamenschiki/) |
| БП онлайн | [Перейти](https://labubugram.github.io/tg/bponline/) |
| Бэкдор | [Перейти](https://labubugram.github.io/tg/whackdoor/) |
| Восьмидесятые | [Перейти](https://labubugram.github.io/tg/knopka_az5/) |
| Высокоранговые мемы | [Перейти](https://labubugram.github.io/tg/memy_meme/) |
| Девяностые | [Перейти](https://labubugram.github.io/tg/devianostyye/) |
| Екатерина Мизулина | [Перейти](https://labubugram.github.io/tg/ekaterina_mizulina/) |
| Закрытый космос | [Перейти](https://labubugram.github.io/tg/roscosmos_press/) |
| Запястье Пумы | [Перейти](https://labubugram.github.io/tg/wristpuma/) |
| Код Дурова | [Перейти](https://labubugram.github.io/tg/d_code/) |
| Крис, где мемы? | [Перейти](https://labubugram.github.io/tg/christymemes/) |
| Лепра | [Перейти](https://labubugram.github.io/tg/Lepragram/) |
| Лингвистические истории | [Перейти](https://labubugram.github.io/tg/linguisticstory/) |
| Любовь не взаимна ❤️‍🩹 | [Перейти](https://labubugram.github.io/tg/not_in_love/) |
| Мальчиш Плохиш | [Перейти](https://labubugram.github.io/tg/malchish_bad/) |
| мурохамма | [Перейти](https://labubugram.github.io/tg/muroxamma/) |
| Пикчи разной степени абсурдности | [Перейти](https://labubugram.github.io/tg/picchiiiiii/) |
| Старая Москва | [Перейти](https://labubugram.github.io/tg/old_Moscow/) |
| Страдающее Средневековье | [Перейти](https://labubugram.github.io/tg/pophistory/) |
| Философский Кукож | [Перейти](https://labubugram.github.io/tg/soymem/) |
| Фонтанка SPB Online | [Перейти](https://labubugram.github.io/tg/fontankaspb/) |
| Фуфайки Клок | [Перейти](https://labubugram.github.io/tg/for5oclock/) |
| Хабр | [Перейти](https://labubugram.github.io/tg/habr_com/) |
| Хронограф | [Перейти](https://labubugram.github.io/tg/chronograph_life/) |
| Шкаф с кассетами | [Перейти](https://labubugram.github.io/tg/videoshkaf/) |
| Эксплойт | [Перейти](https://labubugram.github.io/tg/exploitex/) |
| Эпоха 90-х | [Перейти](https://labubugram.github.io/tg/LostGen/) |
| мелана:) неизданное | [Перейти](https://labubugram.github.io/tg/vasiileevaa/) |



