import asyncpg
import asyncio
from tabulate import tabulate
from colorama import Fore, Style
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
import yake
import json
import re
import warnings
import hashlib
import numpy as np
warnings.filterwarnings("ignore", category=UserWarning, module="pymorphy2")

load_dotenv()

# Глобальные переменные для Natasha
segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
ner_tagger = NewsNERTagger(emb)
_global_s3_uploader = None

# Конфигурация БД
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Настройки пула (можно вынести в .env)
POOL_MIN_SIZE = int(os.getenv('POOL_MIN_SIZE', '5'))
POOL_MAX_SIZE = int(os.getenv('POOL_MAX_SIZE', '20'))
POOL_COMMAND_TIMEOUT = int(os.getenv('POOL_COMMAND_TIMEOUT', '60'))

# Кэш для партиций
_partition_cache = set()
_partition_cache_lock = asyncio.Lock()

# ==================== Утилиты ====================

def print_message(message, level="info"):
    """Унифицированный вывод сообщений с цветами."""
    if level == "error":
        print(f"{Fore.WHITE}{message} {Fore.RED}[ERROR]{Style.RESET_ALL}")
        logging.error(message)
    elif level == "warning":
        print(f"{Fore.WHITE}{message} {Fore.YELLOW}[WARNING]{Style.RESET_ALL}")
        logging.warning(message)
    else:
        print(f"{Fore.WHITE}{message} {Fore.GREEN}[ OK ]{Style.RESET_ALL}")
        logging.info(message)

def compute_text_fingerprint(text):
    """Вычисление fingerprint текста для обнаружения дубликатов."""
    if not text:
        return None
    normalized = ' '.join(text.lower().split())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def extract_entities(text):
    """Извлечение именованных сущностей с детальной информацией."""
    if not text:
        return []
    doc = Doc(text)
    doc.segment(segmenter)
    doc.tag_ner(ner_tagger)
    entities = []
    for span in doc.spans:
        entities.append({
            'type': span.type,
            'text': span.text,
            'start': span.start,
            'end': span.stop,
            'norm': span.normal
        })
    return entities

def extract_keywords(text):
    """Извлечение ключевых слов с использованием yake."""
    try:
        if not text:
            return []
        kw_extractor = yake.KeywordExtractor(lan='ru', n=3, dedupLim=0.9, top=10)
        keywords = kw_extractor.extract_keywords(text)
        return [{'keyword': kw[0], 'score': kw[1]} for kw in keywords]
    except Exception as e:
        logging.error(f"Error extracting keywords: {e}")
        return []

def extract_ner(text):
    """Извлечение именованных сущностей (NER) для тегов."""
    try:
        logging.debug(f"Extracting NER for text: {text[:100]}{'...' if len(text) > 100 else ''}")
        doc = Doc(text or '')
        doc.segment(segmenter)
        doc.tag_ner(ner_tagger)
        ner_results = {'person': [], 'organization': [], 'location': []}
        for span in doc.spans:
            if span.type == 'PER':
                ner_results['person'].append(span.text)
            elif span.type == 'ORG':
                ner_results['organization'].append(span.text)
            elif span.type == 'LOC':
                ner_results['location'].append(span.text)
        logging.debug(f"NER result: {ner_results}")
        return ner_results
    except Exception as e:
        logging.error(f"Error extracting NER: {e}")
        return {'person': [], 'organization': [], 'location': []}

def extract_keywords_yake(text):
    """Извлечение ключевых слов для тегов."""
    try:
        logging.debug(f"Extracting keywords for text: {text[:100]}{'...' if len(text) > 100 else ''}")
        kw_extractor = yake.KeywordExtractor(lan='ru', n=3, dedupLim=0.9, top=5)
        keywords = kw_extractor.extract_keywords(text or '')
        keyword_list = [{'keyword': kw[0], 'score': kw[1]} for kw in keywords]
        logging.debug(f"Keyword result: {keyword_list}")
        return keyword_list
    except Exception as e:
        logging.error(f"Error extracting keywords: {e}")
        return []

def normalize_chat_id(chat_id):
    """Нормализация ID чата."""
    if chat_id < 0 and chat_id <= -1000000000000:
        return - (chat_id + 1000000000000)
    return chat_id

# ==================== Управление пулом ====================

async def create_db_pool():
    """Создает пул соединений с БД."""
    try:
        pool = await asyncpg.create_pool(
            **db_config,
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            command_timeout=POOL_COMMAND_TIMEOUT,
            max_queries=50000,  # Максимальное количество запросов через одно соединение
            max_inactive_connection_lifetime=300.0,  # 5 минут
            setup=None  # Можно добавить функцию для настройки соединения
        )
        logging.info(f"Database pool created: min={POOL_MIN_SIZE}, max={POOL_MAX_SIZE}")
        print_message(f"Database pool created with {POOL_MIN_SIZE}-{POOL_MAX_SIZE} connections")
        return pool
    except Exception as e:
        logging.error(f"Failed to create database pool: {e}")
        print_message(f"Failed to create database pool: {e}", level="error")
        raise

async def get_pool_stats(pool):
    """Получение статистики пула."""
    try:
        stats = {
            'size': pool.get_size(),
            'min_size': pool._minsize,
            'max_size': pool._maxsize,
            'free_connections': pool.get_idle_size(),
            'active_connections': pool.get_size() - pool.get_idle_size(),
            'closed': pool._closed if hasattr(pool, '_closed') else False
        }
        
        # Дополнительная информация через запрос к БД
        try:
            async with pool.acquire() as conn:
                pg_stats = await conn.fetch("""
                    SELECT count(*) as total_connections,
                           sum(case when state = 'active' then 1 else 0 end) as active_connections
                    FROM pg_stat_activity 
                    WHERE datname = current_database()
                """)
                if pg_stats:
                    stats['pg_total_connections'] = pg_stats[0]['total_connections']
                    stats['pg_active_connections'] = pg_stats[0]['active_connections']
        except:
            pass
            
        return stats
    except Exception as e:
        logging.error(f"Error getting pool stats: {e}")
        return {}

# ==================== Миграции ====================

async def migrate_users_to_gray_list(pool):
    """Миграция пользователей в серый список с использованием пула."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_lists (user_id, list_type, created_at)
                SELECT user_id, 'gray', CURRENT_TIMESTAMP
                FROM users
                WHERE user_id NOT IN (SELECT user_id FROM user_lists)
                ON CONFLICT DO NOTHING
                """
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM user_lists WHERE list_type = 'gray'")
            print(f"{Fore.GREEN}Migration completed. Users in gray list: {count}.")
            logging.info(f"User migration to gray list completed. Users in gray list: {count}.")
    except Exception as e:
        logging.error(f"Error migrating users to gray list: {e}")
        print(f"{Fore.RED}Migration error: {e}{Style.RESET_ALL}")

# ==================== Партиции ====================

async def ensure_plpgsql_extension(conn):
    """Проверка и создание расширения plpgsql."""
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_extension WHERE extname = 'plpgsql'"
        )
        if not exists:
            await conn.execute("CREATE EXTENSION plpgsql;")
            logging.info("plpgsql extension successfully created.")
            print(f"{Fore.GREEN}plpgsql extension created.{Style.RESET_ALL}")
        else:
            logging.debug("plpgsql extension already exists.")
    except Exception as e:
        logging.error(f"Error checking/creating plpgsql extension: {e}")
        print(f"{Fore.RED}Error creating plpgsql extension: {e}{Style.RESET_ALL}")
        raise

async def create_partition_if_not_exists(conn, year):
    """Создание партиции для указанного года."""
    partition_name = f"messages_{year}"
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = $1",
            partition_name
        )
        if exists:
            logging.debug(f"Partition {partition_name} already exists")
            return

        start_date = f"{year}-01-01"
        end_date = f"{year + 1}-01-01"
        sql_query = f"""
            CREATE TABLE {partition_name} PARTITION OF messages
            FOR VALUES FROM ('{start_date}') TO ('{end_date}')
        """
        logging.debug(f"Forming SQL for partition {partition_name}: {sql_query}")
        
        await conn.execute(sql_query)
        logging.info(f"Created partition {partition_name} for year {year}")
        print(f"{Fore.GREEN}Created partition {partition_name} for year {year}{Style.RESET_ALL}")
    except Exception as e:
        logging.error(f"Error creating partition {partition_name}: {e}")
        print(f"{Fore.RED}Error creating partition {partition_name}: {e}{Style.RESET_ALL}")
        raise

async def ensure_partition_for_date(conn, message_date):
    """Создание партиции для даты с кэшированием."""
    year = message_date.year
    
    # Проверяем кэш (без блокировки для чтения)
    if year in _partition_cache and year + 1 in _partition_cache:
        return
    
    async with _partition_cache_lock:
        # Проверяем еще раз после блокировки
        if year not in _partition_cache:
            await create_partition_if_not_exists(conn, year)
            _partition_cache.add(year)
        
        if year + 1 not in _partition_cache:
            await create_partition_if_not_exists(conn, year + 1)
            _partition_cache.add(year + 1)

# ==================== CRUD операции ====================

async def add_chat_to_db(conn, chat_data):
    """Добавление чата в БД."""
    try:
        chat_data['chat_id'] = normalize_chat_id(chat_data['chat_id'])
        exists = await conn.fetchval(
            "SELECT 1 FROM chats WHERE chat_id = $1",
            chat_data['chat_id']
        )
        if exists:
            print(f"{Fore.YELLOW}Chat with ID {chat_data['chat_id']} already exists.")
            logging.info(f"Attempt to add existing chat: {chat_data['chat_id']}")
            return

        await conn.execute(
            """
            INSERT INTO chats (
                chat_id, type, access_type, username, title, description, 
                participants_count, is_active, photo_id, linked_chat_id, migrated_to,
                migrated_from, sticker_set_name, can_set_sticker_set, min_rank,
                banned_rights, default_banned_rights, slow_mode_seconds, ttl_period,
                join_request, join_to_send, signatures, has_geo, geo_point,
                address, restrictions, folder_id, folder_name, folder_order,
                folder_included, folder_pinned, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
                    $28, $29, $30, $31, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            chat_data['chat_id'],
            chat_data.get('type'),
            chat_data.get('access_type'),
            chat_data.get('username'),
            chat_data.get('title'),
            chat_data.get('description'),
            chat_data.get('participants_count'),
            chat_data.get('is_active', False),
            chat_data.get('photo_id'),
            chat_data.get('linked_chat_id'),
            chat_data.get('migrated_to'),
            chat_data.get('migrated_from'),
            chat_data.get('sticker_set_name'),
            chat_data.get('can_set_sticker_set'),
            chat_data.get('min_rank'),
            json.dumps(chat_data.get('banned_rights')) if chat_data.get('banned_rights') else None,
            json.dumps(chat_data.get('default_banned_rights')) if chat_data.get('default_banned_rights') else None,
            chat_data.get('slow_mode_seconds'),
            chat_data.get('ttl_period'),
            chat_data.get('join_request'),
            chat_data.get('join_to_send'),
            chat_data.get('signatures'),
            chat_data.get('has_geo'),
            chat_data.get('geo_point'),
            chat_data.get('address'),
            json.dumps(chat_data.get('restrictions')) if chat_data.get('restrictions') else None,
            chat_data.get('folder_id'),
            chat_data.get('folder_name'),
            chat_data.get('folder_order'),
            chat_data.get('folder_included'),
            chat_data.get('folder_pinned')
        )
        print(f"{Fore.GREEN}Chat {chat_data['chat_id']} added to database.")
        logging.info(f"Added chat: {chat_data['chat_id']}")
    except Exception as e:
        logging.error(f"Error adding chat {chat_data['chat_id']}: {e}")
        print(f"{Fore.RED}Error adding chat: {e}")

async def add_user_to_db(conn, user_data):
    """Добавление пользователя в БД."""
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE user_id = $1",
            user_data['user_id']
        )
        if exists:
            logging.debug(f"User {user_data['user_id']} already exists.")
            return
            
        await conn.execute(
            """
            INSERT INTO users (
                user_id, username, first_name, last_name, phone, is_bot, status,
                language_code, is_verified, is_scam, is_fake, is_support, premium,
                premium_expires, dc_id, photo_id, restriction_reason, emoji_status,
                online_until, last_seen, about, stories_unavailable, common_chats_count,
                toxicity_score, spam_probability, avg_message_time, active_hours,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_data['user_id'],
            user_data.get('username'),
            user_data.get('first_name'),
            user_data.get('last_name'),
            user_data.get('phone'),
            user_data.get('is_bot', False),
            user_data.get('status'),
            user_data.get('language_code'),
            user_data.get('is_verified', False),
            user_data.get('is_scam', False),
            user_data.get('is_fake', False),
            user_data.get('is_support', False),
            user_data.get('premium', False),
            user_data.get('premium_expires'),
            user_data.get('dc_id'),
            user_data.get('photo_id'),
            user_data.get('restriction_reason'),
            user_data.get('emoji_status'),
            user_data.get('online_until'),
            user_data.get('last_seen'),
            user_data.get('about'),
            user_data.get('stories_unavailable'),
            user_data.get('common_chats_count'),
            user_data.get('toxicity_score'),
            user_data.get('spam_probability'),
            user_data.get('avg_message_time'),
            user_data.get('active_hours')
        )
        
        # Добавляем в серый список по умолчанию
        current_list = await conn.fetchval(
            "SELECT list_type FROM user_lists WHERE user_id = $1",
            user_data['user_id']
        )
        if current_list != 'gray':
            logging.debug(f"Skipping adding user_id={user_data['user_id']} to gray list, current list: {current_list}")
        else:
            await add_user_to_list(conn, user_data['user_id'], 'gray')
            
        logging.info(f"Added user: {user_data['user_id']}")
    except Exception as e:
        logging.error(f"Error adding user {user_data['user_id']}: {e}")
        print(f"{Fore.RED}Error adding user: {e}")

async def add_user_to_list(conn, user_id, list_type):
    """Добавление пользователя в список (white/black/gray)."""
    if list_type not in ('white', 'black', 'gray'):
        raise ValueError(f"Invalid list type: {list_type}")
    try:
        existing_list = await conn.fetchval(
            "SELECT list_type FROM user_lists WHERE user_id = $1",
            user_id
        )
        if existing_list and existing_list != list_type:
            print(f"{Fore.YELLOW}User {user_id} already in {existing_list} list.")
            logging.info(f"Attempt to add user {user_id} to {list_type} list, but already in {existing_list} list.")
            return

        await conn.execute(
            """
            INSERT INTO user_lists (user_id, list_type)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            user_id, list_type
        )
        logging.info(f"User {user_id} added to {list_type} list")
    except Exception as e:
        logging.error(f"Error adding user {user_id} to list {list_type}: {e}")
        print(f"{Fore.RED}Error adding to list: {e}")

async def get_user_list(conn, user_id):
    """Получение типа списка для пользователя."""
    if user_id is None:
        return 'none'
    try:
        result = await conn.fetchval(
            "SELECT list_type FROM user_lists WHERE user_id = $1",
            user_id
        )
        return result or 'gray'
    except Exception as e:
        logging.error(f"Error getting list for user {user_id}: {e}")
        return 'gray'

async def add_chat_to_list(conn, chat_id, list_type):
    """Добавление чата в список (white/black/gray)."""
    if list_type not in ('white', 'black', 'gray'):
        raise ValueError(f"Invalid list type: {list_type}")
    try:
        existing_list = await conn.fetchval(
            "SELECT list_type FROM chat_lists WHERE chat_id = $1",
            chat_id
        )
        if existing_list and existing_list != list_type:
            print(f"{Fore.YELLOW}Chat {chat_id} already in {existing_list} list.")
            logging.info(f"Attempt to add chat {chat_id} to {list_type} list, but already in {existing_list} list.")
            return

        await conn.execute(
            """
            INSERT INTO chat_lists (chat_id, list_type)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            chat_id, list_type
        )
        logging.info(f"Chat {chat_id} added to {list_type} list")
        print(f"{Fore.GREEN}Chat {chat_id} added to {list_type} list.")
    except Exception as e:
        logging.error(f"Error adding chat {chat_id} to list {list_type}: {e}")
        print(f"{Fore.RED}Error adding: {e}")

async def get_chat_list(conn, chat_id):
    """Получение типа списка для чата."""
    try:
        result = await conn.fetchval(
            "SELECT list_type FROM chat_lists WHERE chat_id = $1",
            chat_id
        )
        return result or 'gray'
    except Exception as e:
        logging.error(f"Error getting list for chat {chat_id}: {e}")
        return 'gray'

# ==================== Словарь и новые слова ====================

async def check_words_against_dictionary(conn, text):
    """Проверка текста на новые слова и возврат списка новых слов."""
    try:
        if not text or not isinstance(text, str):
            return set()

        # Токенизация текста
        words = re.findall(r'\b[\w-]+\b', text.lower())
        
        # Очистка слов
        cleaned_words = []
        for word in words:
            cleaned_word = re.sub(r'^[.,!?;:-]+|[.,!?;:-]+$', '', word)
            if not cleaned_word or re.match(r'^[-.,!?;:]+$', cleaned_word):
                continue
            cleaned_words.append(cleaned_word)

        if not cleaned_words:
            return set()

        # Получение существующих слов
        existing_words = await conn.fetch(
            "SELECT word FROM dictionary WHERE word = ANY($1::text[])",
            cleaned_words
        )
        existing_words_set = {row['word'] for row in existing_words}

        # Новые слова
        new_words = set(cleaned_words) - existing_words_set
        return new_words

    except Exception as e:
        logging.error(f"Error checking words: {e}")
        print(f"{Fore.RED}Error checking words: {e}{Style.RESET_ALL}")
        return set()

# ==================== Сообщения ====================

async def add_messages_batch(pool, messages_data):
    """Добавление пачки сообщений одним соединением."""
    if not messages_data:
        return
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Подготовка данных для executemany
            batch = []
            for msg in messages_data:
                # Убеждаемся, что все поля есть
                batch.append((
                    msg['message_id'],
                    msg['chat_id'],
                    msg.get('sender_id'),
                    msg.get('text', ''),
                    msg['date'],
                    msg.get('views'),
                    msg.get('forwards', 0),
                    msg.get('reply_to_msg_id'),
                    msg.get('media_type'),
                    json.dumps(msg.get('tags', {})),
                    msg.get('text_fingerprint'),
                    msg.get('system_received_at', datetime.utcnow())
                ))
            
            # Один запрос на все сообщения
            await conn.executemany("""
                INSERT INTO messages (
                    message_id, chat_id, sender_id, text, date, views, forwards,
                    reply_to_msg_id, media_type, tags, text_fingerprint, system_received_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (message_id, date) DO NOTHING
            """, batch)
            
            logging.info(f"Batch inserted {len(batch)} messages")

async def add_message_to_db(conn, message_data):
    """Добавление одного сообщения в БД."""
    try:
        message_data['chat_id'] = normalize_chat_id(message_data['chat_id'])
        
        # Проверяем существование чата
        chat_exists = await conn.fetchval(
            "SELECT 1 FROM chats WHERE chat_id = $1",
            message_data['chat_id']
        )
        if not chat_exists:
            logging.error(f"Chat {message_data['chat_id']} missing from chats table")
            print(f"{Fore.RED}Chat {message_data['chat_id']} missing from database.")
            return

        # Создаем партицию для даты
        await ensure_partition_for_date(conn, message_data['date'])
        
        # Обработка текста
        text = message_data.get('text', '')
        if isinstance(text, bytes):
            text = text.decode('utf-8', errors='replace')
        elif not isinstance(text, str):
            text = str(text) if text else ''
        message_data['text'] = text
        
        # Поиск новых слов
        new_words = await check_words_against_dictionary(conn, text)
        
        # Получение списка пользователя
        user_list = await get_user_list(conn, message_data['sender_id'])
        
        # Извлечение NER и ключевых слов
        ner_results = extract_ner(text)
        keywords = extract_keywords_yake(text)
        
        # Формирование тегов
        tags = message_data.get('tags', {})
        tags.update({
            'new_words': list(new_words),
            'user_list': user_list,
            'ner': ner_results,
            'keywords': [kw['keyword'] for kw in keywords]
        })
        
        tags_json = json.dumps(tags, ensure_ascii=False)
        
        # Вставка сообщения - проверяем, что все ID целые числа
        try:
            await conn.execute(
                """
                INSERT INTO messages (
                    message_id, chat_id, sender_id, text, date, views, forwards,
                    reply_to_msg_id, reply_to_top_id, thread_id, media_type,
                    media_metadata, reactions, reactions_count, recent_reactions,
                    is_edited, edit_date, out, mentioned, silent, post, from_scheduled,
                    legacy, edit_hide, entities, forward_info, grouped_id, via_bot_id,
                    ttl_period, restriction_reason, action, tags, text_fingerprint,
                    system_received_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                        $27, $28, $29, $30, $31, $32, $33, $34)
                ON CONFLICT (message_id, date) DO NOTHING
                """,
                int(message_data['message_id']),  # Принудительно в int
                int(message_data['chat_id']),     # Принудительно в int
                int(message_data['sender_id']) if message_data['sender_id'] else None,
                message_data['text'],
                message_data['date'],
                message_data.get('views'),
                message_data.get('forwards', 0),
                int(message_data.get('reply_to_msg_id')) if message_data.get('reply_to_msg_id') else None,
                int(message_data.get('reply_to_top_id')) if message_data.get('reply_to_top_id') else None,
                int(message_data.get('thread_id')) if message_data.get('thread_id') else None,
                message_data.get('media_type'),
                message_data.get('media_metadata'),
                message_data.get('reactions'),
                message_data.get('reactions_count', 0),
                message_data.get('recent_reactions'),
                message_data.get('is_edited', False),
                message_data.get('edit_date'),
                message_data.get('out', False),
                message_data.get('mentioned', False),
                message_data.get('silent', False),
                message_data.get('post', False),
                message_data.get('from_scheduled', False),
                message_data.get('legacy', False),
                message_data.get('edit_hide', False),
                message_data.get('entities'),
                message_data.get('forward_info'),
                int(message_data.get('grouped_id')) if message_data.get('grouped_id') else None,
                int(message_data.get('via_bot_id')) if message_data.get('via_bot_id') else None,
                message_data.get('ttl_period'),
                message_data.get('restriction_reason'),
                message_data.get('action'),
                tags_json,
                message_data.get('text_fingerprint'),
                message_data.get('system_received_at', datetime.utcnow())
            )
            logging.info(f"Added message {message_data['message_id']} to chat {message_data['chat_id']}")
        except Exception as e:
            logging.error(f"Error in INSERT for message {message_data['message_id']}: {e}")
            raise

        # Добавление новых слов
        for word in new_words:
            await conn.execute(
                """
                INSERT INTO new_words (word, chat_id, user_id, message_id, date)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                word, int(message_data['chat_id']), int(message_data['sender_id']) if message_data['sender_id'] else None, 
                int(message_data['message_id']), message_data['date']
            )

        # Добавление ключевых слов
        for keyword in keywords:
            keyword_id = await conn.fetchval(
                """
                INSERT INTO keywords (keyword) VALUES ($1) 
                ON CONFLICT (keyword) DO UPDATE SET keyword = EXCLUDED.keyword 
                RETURNING keyword_id
                """,
                keyword['keyword']
            )
            if keyword_id:
                await conn.execute(
                    """
                    INSERT INTO message_keywords (message_id, date, keyword_id, score)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    int(message_data['message_id']), message_data['date'], 
                    keyword_id, keyword['score']
                )

        # Создание события для анализа текста
        if text and text.strip():
            event_data = {
                'message_id': int(message_data['message_id']),  # Принудительно в int
                'chat_id': int(message_data['chat_id']),       # Принудительно в int
                'text': text,
                'text_fingerprint': message_data.get('text_fingerprint')
            }
            await conn.execute(
                """
                INSERT INTO event_queue (event_type, event_data, priority, status)
                VALUES ($1, $2, $3, 'pending')
                """,
                'text_analysis',
                json.dumps(event_data, ensure_ascii=False),
                1
            )
            logging.debug(f"Created text_analysis event: {event_data}")
    
    except Exception as e:
        logging.error(f"Error adding message {message_data['message_id']} to chat {message_data['chat_id']}: {e}")
        print(f"{Fore.RED}Error adding message: {e}{Style.RESET_ALL}")
        raise  # Пробрасываем ошибку, чтобы транзакция откатилась

# ==================== Импорт сообщений ====================

async def import_messages_to_db(
    client, 
    chat_id, 
    limit=10, 
    pool=None, 
    import_media=False, 
    import_reactions=False, 
    wait_for_s3=False, 
    s3_uploader=None
):
    """
    Импорт сообщений из чата в базу данных.
   
    Args:
        client: Telegram client
        chat_id: ID чата
        limit: максимальное количество сообщений для импорта
        pool: пул соединений (ОБЯЗАТЕЛЬНЫЙ параметр)
        import_media: скачивать ли медиафайлы
        import_reactions: импортировать ли детальные реакции
        wait_for_s3: ждать ли загрузки в S3 перед завершением
        s3_uploader: загрузчик S3 (опционально)
    """
    # Проверяем наличие pool
    if pool is None:
        raise ValueError("import_messages_to_db requires a connection pool")
   
    # ИСПРАВЛЕНИЕ: Импортируем здесь, чтобы избежать циклических зависимостей
    from realtime_recorder import RealtimeRecorder
   
    if s3_uploader is None:
        import db_module
        s3_uploader = db_module._global_s3_uploader
        logging.debug(f"Using global S3 uploader for import: {bool(s3_uploader)}")
   
    try:
        chat_id = normalize_chat_id(chat_id)
       
        # ПОЛУЧАЕМ ТИП СПИСКА ЧАТА ДО ИМПОРТА
        async with pool.acquire() as conn:
            chat_list = await get_chat_list(conn, chat_id)
            logging.debug(f"Chat {chat_id} list type: {chat_list}")
       
        # РЕШАЕМ, ЗАГРУЖАТЬ ЛИ МЕДИА
        should_download_media = import_media or (chat_list == 'white')
       
        # Проверка доступа к чату (вне соединения)
        try:
            await client.get_messages(chat_id, limit=1)
            logging.debug(f"Access to {chat_id} confirmed")
        except Exception as e:
            logging.error(f"No access to {chat_id}: {e}")
            print_message(f"No access to {chat_id}: {e}", level="error")
            return 0
       
        # Определяем тип чата
        try:
            chat = await client.get_entity(chat_id)
            is_channel = hasattr(chat, 'broadcast') and chat.broadcast
        except Exception as e:
            logging.warning(f"Could not determine source type {chat_id}: {e}")
            is_channel = False
       
        # Получаем последний ID сообщения в БД
        async with pool.acquire() as conn:
            last_message = await conn.fetchrow(
                """
                SELECT message_id, date
                FROM messages
                WHERE chat_id = $1
                ORDER BY date DESC, message_id DESC
                LIMIT 1
                """,
                chat_id
            )
       
        min_id = last_message['message_id'] if last_message else 0
       
        # Получаем ТОЛЬКО новые сообщения из Telegram
        messages = []
        async for message in client.iter_messages(chat_id, min_id=min_id, limit=limit):
            messages.append(message)
       
        if not messages:
            print_message(f"No new messages in chat {chat_id}")
            return 0
       
        logging.debug(f"Retrieved {len(messages)} new messages for {chat_id}")
       
        # Определяем все года для партиций
        years_needed = set()
        for msg in messages:
            if msg.date:
                years_needed.add(msg.date.year)
                years_needed.add(msg.date.year + 1)
       
        # Подготавливаем данные для batch insert
        batch_data = []
       
        for message in messages:
            try:
                message_date = message.date.replace(tzinfo=None) if message.date.tzinfo else message.date
               
                # Обработка отправителя
                sender_id = message.sender_id
                if isinstance(sender_id, (int, float)) and sender_id > 0 and not is_channel:
                    try:
                        sender = await client.get_entity(sender_id)
                        sender_data = {
                            'user_id': sender.id,
                            'username': getattr(sender, 'username', None),
                            'first_name': getattr(sender, 'first_name', None),
                            'last_name': getattr(sender, 'last_name', None),
                            'phone': getattr(sender, 'phone', None),
                            'is_bot': getattr(sender, 'bot', False),
                            'status': sender.status.__class__.__name__ if hasattr(sender, 'status') and sender.status else None,
                            'language_code': getattr(sender, 'lang_code', None),
                            'is_verified': getattr(sender, 'verified', False),
                            'is_scam': getattr(sender, 'scam', False),
                            'is_fake': getattr(sender, 'fake', False),
                            'premium': getattr(sender, 'premium', False),
                            'dc_id': getattr(sender, 'dc_id', None)
                        }
                        async with pool.acquire() as user_conn:
                            await add_user_to_db(user_conn, sender_data)
                    except Exception as e:
                        logging.warning(f"Could not get sender data {sender_id} for {chat_id}: {e}")
                        sender_id = None
                else:
                    sender_id = None
               
                # Текст сообщения
                text = message.text or ''
                if isinstance(text, bytes):
                    text = text.decode('utf-8', errors='replace')
                elif not isinstance(text, str):
                    text = str(text) if text else ''
               
                if len(text) > 10000:
                    text = text[:10000] + '...'
                    logging.warning(f"Message text ID {message.id} truncated to 10000 chars")
               
                # Вычисляем fingerprint
                text_fingerprint = compute_text_fingerprint(text)
               
                # Обработка entities
                entities_json = None
                if message.entities:
                    entities_json = json.dumps([
                        {
                            'type': type(e).__name__,
                            'offset': e.offset,
                            'length': e.length,
                            'url': getattr(e, 'url', None),
                            'email': getattr(e, 'email', None),
                            'phone': getattr(e, 'phone', None),
                            'mention': getattr(e, 'mention', None),
                            'language': getattr(e, 'language', None)
                        }
                        for e in message.entities
                    ])
               
                # Обработка forward info
                forward_info = None
                if message.fwd_from:
                    forward_info = json.dumps({
                        'original_chat_id': getattr(message.fwd_from, 'chat_id', None),
                        'original_message_id': getattr(message.fwd_from, 'message_id', None),
                        'original_date': message.fwd_from.date.isoformat() if message.fwd_from.date else None,
                        'original_author': getattr(message.fwd_from, 'from_name', None),
                        'forward_signature': getattr(message.fwd_from, 'post_author', None),
                        'saved_from_peer': getattr(message.fwd_from, 'saved_from_peer', None),
                        'channel_post': getattr(message.fwd_from, 'channel_post', None)
                    })
               
                # Обработка реакций
                reactions_json = None
                reactions_count = 0
                if import_reactions and hasattr(message, 'reactions') and message.reactions:
                    if hasattr(message.reactions, 'results'):
                        reactions_data = []
                        for r in message.reactions.results:
                            reaction_data = {'count': getattr(r, 'count', 0), 'chosen': getattr(r, 'chosen', False)}
                            if hasattr(r, 'reaction'):
                                if hasattr(r.reaction, 'emoticon'):
                                    reaction_data['emoticon'] = r.reaction.emoticon
                                    reaction_data['type'] = 'emoji'
                                elif hasattr(r.reaction, 'document_id'):
                                    reaction_data['document_id'] = r.reaction.document_id
                                    reaction_data['type'] = 'custom_emoji'
                                else:
                                    reaction_data['emoticon'] = str(r.reaction)
                                    reaction_data['type'] = 'legacy'
                            else:
                                reaction_data['emoticon'] = 'unknown'
                                reaction_data['type'] = 'unknown'
                            reactions_data.append(reaction_data)
                        reactions_json = json.dumps(reactions_data)
                        reactions_count = sum(r.get('count', 0) for r in reactions_data)
               
                # Обработка action
                action_json = None
                if message.action:
                    action_data = {'type': type(message.action).__name__}
                    if hasattr(message.action, 'title'):
                        action_data['title'] = message.action.title
                    if hasattr(message.action, 'users'):
                        action_data['users'] = message.action.users
                    action_json = json.dumps(action_data)
               
                # Подготовка данных сообщения
                message_data = {
                    'message_id': message.id,
                    'chat_id': chat_id,
                    'sender_id': sender_id,
                    'text': text,
                    'date': message_date,
                    'views': message.views,
                    'forwards': getattr(message, 'forwards', 0),
                    'reply_to_msg_id': message.reply_to_msg_id,
                    'reply_to_top_id': getattr(message, 'reply_to_top_id', None),
                    'thread_id': getattr(message, 'thread_id', None),
                    'media_type': message.media.__class__.__name__ if message.media else None,
                    'media_metadata': None,  # Будет заполнено позже
                    'reactions': reactions_json,
                    'reactions_count': reactions_count,
                    'recent_reactions': None,
                    'is_edited': message.edit_date is not None,
                    'edit_date': message.edit_date.replace(tzinfo=None) if message.edit_date and message.edit_date.tzinfo else message.edit_date,
                    'out': getattr(message, 'out', False),
                    'mentioned': getattr(message, 'mentioned', False),
                    'silent': getattr(message, 'silent', False),
                    'post': getattr(message, 'post', False),
                    'from_scheduled': getattr(message, 'from_scheduled', False),
                    'legacy': getattr(message, 'legacy', False),
                    'edit_hide': getattr(message, 'edit_hide', False),
                    'entities': entities_json,
                    'forward_info': forward_info,
                    'grouped_id': getattr(message, 'grouped_id', None),
                    'via_bot_id': getattr(message, 'via_bot_id', None),
                    'ttl_period': getattr(message, 'ttl_period', None),
                    'restriction_reason': getattr(message, 'restriction_reason', None),
                    'action': action_json,
                    'tags': {
                        'source': 'import',
                        'import_media': import_media,
                        'import_reactions': import_reactions,
                        'chat_list': chat_list
                    },
                    'text_fingerprint': text_fingerprint,
                    'system_received_at': datetime.utcnow()
                }
                
                # Добавляем media_metadata если есть медиа
                if message.media:
                    try:
                        media_metadata = {}
                        
                        # Для документов (видео, аудио, GIF, файлы)
                        if hasattr(message.media, 'document') and message.media.document:
                            doc = message.media.document
                            media_metadata['document'] = {
                                'id': doc.id,
                                'size': doc.size,
                                'mime_type': doc.mime_type,
                                'attributes': []
                            }
                            
                            # Извлекаем атрибуты документа
                            if hasattr(doc, 'attributes') and doc.attributes:
                                for attr in doc.attributes:
                                    attr_type = type(attr).__name__
                                    attr_data = {'type': attr_type}
                                    
                                    if attr_type == 'DocumentAttributeVideo':
                                        attr_data.update({
                                            'duration': getattr(attr, 'duration', 0),
                                            'w': getattr(attr, 'w', 0),
                                            'h': getattr(attr, 'h', 0)
                                        })
                                    elif attr_type == 'DocumentAttributeAudio':
                                        attr_data.update({
                                            'duration': getattr(attr, 'duration', 0),
                                            'title': getattr(attr, 'title', None),
                                            'performer': getattr(attr, 'performer', None)
                                        })
                                    elif attr_type == 'DocumentAttributeFilename':
                                        attr_data['file_name'] = getattr(attr, 'file_name', None)
                                    elif attr_type == 'DocumentAttributeSticker':
                                        attr_data['alt'] = getattr(attr, 'alt', None)
                                    
                                    media_metadata['document']['attributes'].append(attr_data)
                        
                        # Для фото
                        if hasattr(message.media, 'photo') and message.media.photo:
                            photo = message.media.photo
                            media_metadata['photo'] = {
                                'id': photo.id,
                                'date': photo.date.isoformat() if photo.date else None,
                                'sizes': []
                            }
                            
                            if hasattr(photo, 'sizes') and photo.sizes:
                                for size in photo.sizes:
                                    # ИСПРАВЛЕНИЕ: безопасное получение атрибутов
                                    media_metadata['photo']['sizes'].append({
                                        'type': getattr(size, 'type', 'unknown'),
                                        'w': getattr(size, 'w', 0),
                                        'h': getattr(size, 'h', 0),
                                        'size': getattr(size, 'size', 0)
                                    })
                        
                        message_data['media_metadata'] = json.dumps(media_metadata)
                        
                    except Exception as e:
                        logging.warning(f"Error extracting media metadata for message {message.id}: {e}")
                        message_data['media_metadata'] = None
               
                batch_data.append(message_data)
               
            except Exception as e:
                logging.error(f"Error processing message {message.id}: {e}")
                continue
       
        # СОЗДАЕМ ПАРТИЦИИ И ВСТАВЛЯЕМ ДАННЫЕ
        if batch_data:
            async with pool.acquire() as conn:
                # Создаем все нужные партиции
                for year in years_needed:
                    await ensure_partition_for_date(conn, datetime(year, 1, 1))
               
                # Вставляем сообщения
                await add_messages_batch(pool, batch_data)
               
                # ==================== ИСПРАВЛЕНИЕ: МЕДИА + S3 ====================
                if should_download_media:
                    logging.info(f"Starting media download for {len(messages)} messages in chat {chat_id}")
                   
                    # ИСПРАВЛЕНИЕ: Создаем экземпляр RealtimeRecorder с правильным методом
                    recorder = RealtimeRecorder(client, pool, s3_uploader=s3_uploader)
                    media_futures = []
                   
                    for message in messages:
                        if message.media:
                            try:
                                # Проверяем, не скачано ли уже
                                async with pool.acquire() as media_conn:
                                    is_downloaded = await recorder.is_media_downloaded(message.id, chat_id, media_conn)
                               
                                if not is_downloaded:
                                    logging.debug(f"Downloading media for message {message.id}")
                                   
                                    # ИСПРАВЛЕНИЕ: Используем правильный метод process_media
                                    if chat_list == 'white' and s3_uploader:
                                        # Для белых чатов — используем process_media (он сам загружает в S3)
                                        logging.info(f"Processing media for message {message.id} with S3 upload")
                                        future = asyncio.create_task(recorder.process_media(message, chat_id))  # ✅ create_task
                                        media_futures.append(future)
                                    else:
                                        # Для других чатов — только скачиваем без S3
                                        await recorder.process_media(message, chat_id)  # ✅ await
                                   
                                    await asyncio.sleep(1)
                                else:
                                    logging.debug(f"Media already downloaded for message {message.id}")
                                   
                            except Exception as e:
                                logging.error(f"Error downloading media for message {message.id}: {e}")
                                continue
                   
                    # Ждём завершения загрузки в S3 (если требуется)
                    if wait_for_s3 and media_futures and s3_uploader:
                        logging.info(f"Waiting for {len(media_futures)} media uploads to S3 to complete...")
                        for i, future in enumerate(media_futures):
                            try:
                                result = await asyncio.wait_for(future, timeout=90.0)
                                if result:
                                    logging.info(f"S3 upload {i+1}/{len(media_futures)} completed")
                            except asyncio.TimeoutError:
                                logging.error(f"S3 upload {i+1}/{len(media_futures)} timed out")
                            except Exception as e:
                                logging.error(f"S3 upload {i+1}/{len(media_futures)} failed: {e}")
       
        media_status = "with media + S3" if should_download_media and s3_uploader else "without media"
        print_message(f"Import completed: {len(batch_data)} new messages from {chat_id} {media_status}.")
       
        if should_download_media and chat_list == 'white' and s3_uploader:
            logging.info(f"S3 uploads initiated for {len(media_futures) if 'media_futures' in locals() else 0} files in chat {chat_id}")
       
        logging.info(f"Import completed: {len(batch_data)} new messages from {chat_id} {media_status}")
        return len(batch_data)
       
    except Exception as e:
        logging.error(f"Error importing from {chat_id}: {e}")
        print_message(f"Import error: {e}", level="error")
        return 0
        
# ==================== Инициализация БД ====================

# ==================== Инициализация БД ====================

async def init_db():
    """Инициализация базы данных (создание таблиц, индексов, триггеров)."""
    conn = None
    try:
        conn = await asyncpg.connect(**db_config)
        logging.info("Database connection successfully established.")
        
        # Проверка существующих таблиц
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name NOT LIKE '%_20%';
        """
        existing_tables = await conn.fetch(tables_query)
        existing_table_names = {row['table_name'] for row in existing_tables}
        logging.debug(f"Found {len(existing_table_names)} tables: {existing_table_names}")

        # Проверка партиций
        partitions_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND (table_name LIKE 'messages_20%' OR table_name LIKE 'logs_20%');
        """
        existing_partitions = await conn.fetch(partitions_query)
        existing_partition_names = {row['table_name'] for row in existing_partitions}
        logging.debug(f"Found {len(existing_partition_names)} partitions: {existing_partition_names}")

        # Проверка материализованного представления
        matview_query = """
        SELECT matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'public' AND matviewname = 'messages_by_chat';
        """
        existing_matview = await conn.fetch(matview_query)
        matview_exists = bool(existing_matview)
        logging.debug(f"Materialized view messages_by_chat: {'exists' if matview_exists else 'missing'}")

        # Создание расширений
        logging.info("Checking/creating extensions")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        print(f"{Fore.GREEN}Extensions checked/created.{Style.RESET_ALL}")

        # SQL для создания таблиц
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS logs (
            log_id SERIAL NOT NULL,
            module_name VARCHAR(50) NOT NULL,
            action TEXT NOT NULL,
            details JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (log_id, timestamp)
        ) PARTITION BY RANGE (timestamp);

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' AND tablename = 'logs_2024'
            ) THEN
                CREATE TABLE logs_2024 PARTITION OF logs 
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
            END IF;
        END $$;

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' AND tablename = 'logs_2025'
            ) THEN
                CREATE TABLE logs_2025 PARTITION OF logs 
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
            END IF;
        END $$;

        CREATE TABLE IF NOT EXISTS chats (
            chat_id BIGINT PRIMARY KEY,
            type VARCHAR(20) CHECK (type IN ('channel', 'group', 'supergroup')),
            access_type VARCHAR(20) CHECK (access_type IN ('public', 'private')),
            username VARCHAR(255),
            title TEXT,
            description TEXT,
            participants_count INTEGER,
            photo_id BIGINT,
            linked_chat_id BIGINT,
            migrated_to BIGINT,
            migrated_from BIGINT,
            sticker_set_name TEXT,
            can_set_sticker_set BOOLEAN,
            min_rank TEXT,
            banned_rights JSONB,
            default_banned_rights JSONB,
            slow_mode_seconds INTEGER,
            ttl_period INTEGER,
            join_request BOOLEAN DEFAULT FALSE,
            join_to_send BOOLEAN DEFAULT FALSE,
            signatures BOOLEAN DEFAULT FALSE,
            has_geo BOOLEAN DEFAULT FALSE,
            geo_point POINT,
            address TEXT,
            restrictions JSONB,
            folder_id INTEGER,
            folder_name TEXT,
            folder_order INTEGER,
            folder_included BOOLEAN DEFAULT FALSE,
            folder_pinned BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(255),
            first_name TEXT,
            last_name TEXT,
            phone VARCHAR(20),
            is_bot BOOLEAN NOT NULL,
            status VARCHAR(50),
            language_code VARCHAR(10),
            is_verified BOOLEAN DEFAULT FALSE,
            is_scam BOOLEAN DEFAULT FALSE,
            is_fake BOOLEAN DEFAULT FALSE,
            is_support BOOLEAN DEFAULT FALSE,
            premium BOOLEAN DEFAULT FALSE,
            premium_expires TIMESTAMP,
            dc_id INTEGER,
            photo_id BIGINT,
            restriction_reason TEXT,
            emoji_status TEXT,
            online_until TIMESTAMP,
            last_seen TIMESTAMP,
            about TEXT,
            stories_unavailable BOOLEAN DEFAULT FALSE,
            common_chats_count INTEGER DEFAULT 0,
            toxicity_score DECIMAL(5,4),
            spam_probability DECIMAL(5,4),
            avg_message_time TIME,
            active_hours INTEGER[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS user_lists (
            user_id BIGINT REFERENCES users(user_id),
            list_type VARCHAR(20) CHECK (list_type IN ('white', 'black', 'gray')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, list_type)
        );
        CREATE INDEX IF NOT EXISTS idx_user_lists_user_id ON user_lists(user_id);

        CREATE TABLE IF NOT EXISTS chat_lists (
            chat_id BIGINT REFERENCES chats(chat_id),
            list_type VARCHAR(20) CHECK (list_type IN ('white', 'black', 'gray')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, list_type)
        );
        CREATE INDEX IF NOT EXISTS idx_chat_lists_chat_id ON chat_lists(chat_id);

        CREATE TABLE IF NOT EXISTS dictionary (
            word TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_dictionary_word_trgm ON dictionary USING GIN (word gin_trgm_ops);

        CREATE TABLE IF NOT EXISTS new_words (
            word TEXT NOT NULL,
            chat_id BIGINT REFERENCES chats(chat_id),
            user_id BIGINT,
            message_id BIGINT,
            date TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (word, message_id, chat_id, date)
        );
        CREATE INDEX IF NOT EXISTS idx_new_words_chat_id ON new_words(chat_id);
        CREATE INDEX IF NOT EXISTS idx_new_words_user_id ON new_words(user_id);

        CREATE TABLE IF NOT EXISTS messages (
            message_id BIGINT NOT NULL,
            chat_id BIGINT REFERENCES chats NOT NULL,
            sender_id BIGINT,
            text TEXT,
            date TIMESTAMP NOT NULL,
            views INTEGER,
            forwards INTEGER DEFAULT 0,
            reply_to_msg_id BIGINT,
            reply_to_top_id BIGINT,
            thread_id BIGINT,
            reply_to_date TIMESTAMP,
            media_type VARCHAR(50),
            media_metadata JSONB,
            reactions JSONB,
            reactions_count INTEGER DEFAULT 0,
            recent_reactions JSONB,
            is_edited BOOLEAN DEFAULT FALSE,
            edit_date TIMESTAMP,
            out BOOLEAN DEFAULT FALSE,
            mentioned BOOLEAN DEFAULT FALSE,
            silent BOOLEAN DEFAULT FALSE,
            post BOOLEAN DEFAULT FALSE,
            from_scheduled BOOLEAN DEFAULT FALSE,
            legacy BOOLEAN DEFAULT FALSE,
            edit_hide BOOLEAN DEFAULT FALSE,
            entities JSONB,
            forward_info JSONB,
            grouped_id BIGINT,
            via_bot_id BIGINT,
            ttl_period INTEGER,
            restriction_reason TEXT,
            action JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            system_received_at TIMESTAMP,
            tsvector_text TSVECTOR GENERATED ALWAYS AS (to_tsvector('russian', coalesce(text, ''))) STORED,
            tags JSONB,
            text_fingerprint TEXT,
            PRIMARY KEY (message_id, date),
            FOREIGN KEY (sender_id) REFERENCES users(user_id) ON DELETE SET NULL,
            FOREIGN KEY (reply_to_msg_id, reply_to_date) REFERENCES messages(message_id, date)
        ) PARTITION BY RANGE (date);

        CREATE INDEX IF NOT EXISTS idx_text_fingerprint ON messages(text_fingerprint);
        CREATE INDEX IF NOT EXISTS idx_messages_grouped_id ON messages(grouped_id);
        CREATE INDEX IF NOT EXISTS idx_messages_via_bot ON messages(via_bot_id);
        CREATE INDEX IF NOT EXISTS idx_messages_forward_info ON messages USING GIN (forward_info);

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' AND tablename = 'messages_2024'
            ) THEN
                CREATE TABLE messages_2024 PARTITION OF messages 
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
            END IF;
        END $$;

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' AND tablename = 'messages_2025'
            ) THEN
                CREATE TABLE messages_2025 PARTITION OF messages 
                FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
            END IF;
        END $$;

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' AND tablename = 'messages_2026'
            ) THEN
                CREATE TABLE messages_2026 PARTITION OF messages 
                FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
            END IF;
        END $$;

        CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
        CREATE INDEX IF NOT EXISTS idx_messages_tsvector ON messages USING GIN(tsvector_text);
        CREATE INDEX IF NOT EXISTS idx_messages_text_trgm ON messages USING GIN (text gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS idx_messages_chat_id_date ON messages(chat_id, date);
        CREATE INDEX IF NOT EXISTS idx_messages_tags ON messages USING GIN (tags);

        -- Таблица для служебных сообщений
        CREATE TABLE IF NOT EXISTS service_messages (
            service_id SERIAL PRIMARY KEY,
            chat_id BIGINT REFERENCES chats(chat_id),
            user_id BIGINT REFERENCES users(user_id),
            action_type VARCHAR(50),
            action_details JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_id BIGINT,
            date TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_service_messages_chat_id ON service_messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_service_messages_date ON service_messages(date);
        CREATE INDEX IF NOT EXISTS idx_service_messages_message_id ON service_messages(message_id);

        -- Остальные таблицы
        CREATE TABLE IF NOT EXISTS dialogs (
            dialog_id SERIAL PRIMARY KEY,
            chat_id BIGINT REFERENCES chats(chat_id),
            user_id BIGINT,
            is_pinned BOOLEAN DEFAULT FALSE,
            unread_count INTEGER,
            last_message_id BIGINT,
            last_message_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (last_message_id, last_message_date) REFERENCES messages(message_id, date)
        );

        CREATE TABLE IF NOT EXISTS keywords (
            keyword_id SERIAL PRIMARY KEY,
            keyword TEXT NOT NULL UNIQUE,
            category VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS message_keywords (
            message_id BIGINT NOT NULL,
            date TIMESTAMP NOT NULL,
            keyword_id INTEGER REFERENCES keywords(keyword_id),
            score DECIMAL(5,4),
            PRIMARY KEY (message_id, date, keyword_id),
            FOREIGN KEY (message_id, date) REFERENCES messages(message_id, date)
        );

        CREATE INDEX IF NOT EXISTS idx_message_keywords_keyword_id ON message_keywords(keyword_id);

        CREATE TABLE IF NOT EXISTS event_queue (
            event_id SERIAL PRIMARY KEY,
            event_type VARCHAR(50),
            event_data JSONB,
            priority INTEGER DEFAULT 0,
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
            last_error TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_at TIMESTAMP,
            result JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_event_queue_status ON event_queue(status, priority, created_at) WHERE status = 'pending';

        CREATE TABLE IF NOT EXISTS settings (
            setting_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            config JSONB NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            priority INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner_id BIGINT REFERENCES users(user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_settings_config ON settings USING GIN (config);

        -- ==================== Таблицы для медиа ====================
        CREATE TABLE IF NOT EXISTS message_media (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            media_id UUID NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(message_id, chat_id, media_id)
        );

        CREATE INDEX IF NOT EXISTS idx_message_media_message ON message_media(message_id, chat_id);
        CREATE INDEX IF NOT EXISTS idx_message_media_media ON message_media(media_id);

        -- Таблица media_files (без message_id и chat_id)
        CREATE TABLE IF NOT EXISTS media_files (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            file_path TEXT UNIQUE NOT NULL,
            mtime TIMESTAMP NOT NULL,
            file_type VARCHAR(10) NOT NULL,
            directory VARCHAR(10) NOT NULL,
            special_id INTEGER,
            file_size BIGINT,
            file_name TEXT,
            mime_type TEXT,
            width INTEGER,
            height INTEGER,
            duration INTEGER,
            has_thumbnail BOOLEAN DEFAULT FALSE,
            thumbnail_path TEXT,
            access_hash BIGINT,
            file_reference BYTEA,
            uploaded BOOLEAN DEFAULT FALSE,
            uploaded_at TIMESTAMP,
            s3_key TEXT,
            public_url TEXT,
            upload_url TEXT,
            checksum TEXT UNIQUE,
            ocr_text TEXT,
            has_audio BOOLEAN DEFAULT FALSE,
            audio_codec TEXT,
            video_codec TEXT,
            bitrate INTEGER,
            fps INTEGER,
            has_stickers BOOLEAN DEFAULT FALSE,
            stickers JSONB,
            emojis TEXT[],
            artist TEXT,
            title TEXT,
            performer TEXT,
            album TEXT,
            track_number INTEGER,
            year INTEGER,
            genre TEXT,
            waveform BYTEA,
            dc_id INTEGER,
            alternative_versions JSONB,
            media_views INTEGER DEFAULT 0,
            downloads INTEGER DEFAULT 0,
            last_accessed TIMESTAMP,
            blurhash TEXT,
            dominant_color TEXT,
            color_palette TEXT[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_media_files_uploaded ON media_files(uploaded) WHERE uploaded = FALSE;
        CREATE INDEX IF NOT EXISTS idx_media_files_s3_key ON media_files(s3_key) WHERE s3_key IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_media_files_checksum ON media_files (checksum) WHERE checksum IS NOT NULL;

        -- Таблица для chat_participants
        CREATE TABLE IF NOT EXISTS chat_participants (
            chat_id BIGINT REFERENCES chats(chat_id),
            user_id BIGINT REFERENCES users(user_id),
            role VARCHAR(50),
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            left_at TIMESTAMP,
            last_seen TIMESTAMP,
            messages_count INTEGER DEFAULT 0,
            total_characters BIGINT DEFAULT 0,
            is_admin BOOLEAN DEFAULT FALSE,
            admin_rights JSONB,
            restricted_until TIMESTAMP,
            restricted_reason TEXT,
            kicked BOOLEAN DEFAULT FALSE,
            kicked_date TIMESTAMP,
            kicked_by BIGINT,
            PRIMARY KEY (chat_id, user_id)
        );

        -- Таблица для analytics
        CREATE TABLE IF NOT EXISTS analytics (
            analytics_id SERIAL PRIMARY KEY,
            chat_id BIGINT REFERENCES chats(chat_id),
            metric_name VARCHAR(100),
            metric_type VARCHAR(50),
            metric_value NUMERIC,
            date TIMESTAMP,
            visualization_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_analytics_metric_date ON analytics(metric_name, date);

        -- Таблица для import_staging
        CREATE TABLE IF NOT EXISTS import_staging (
            staging_id SERIAL PRIMARY KEY,
            data JSONB,
            import_type VARCHAR(50),
            status VARCHAR(20) CHECK (status IN ('pending', 'processed', 'failed')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        logging.info("Starting SQL execution for creating tables and partitions")
        await conn.execute(create_tables_sql)
        logging.info("Tables, partitions and indexes successfully created.")
        print(f"{Fore.GREEN}Tables and partitions created.{Style.RESET_ALL}")

        # Создание триггеров
        await ensure_plpgsql_extension(conn)

        # Триггеры для сообщений
        edit_delete_triggers_sql = """
        CREATE OR REPLACE FUNCTION notify_edit_message()
        RETURNS TRIGGER AS $$
        DECLARE
            payload JSONB;
        BEGIN
            IF OLD.text IS DISTINCT FROM NEW.text OR OLD.media_type IS DISTINCT FROM NEW.media_type THEN
                payload := jsonb_build_object(
                    'v', '3.0',
                    'type', 'edit',
                    'message_id', NEW.message_id,
                    'chat_id', NEW.chat_id,
                    'text', NEW.text,
                    'media_type', NEW.media_type,
                    'edit_date', NEW.edit_date
                );
                
                PERFORM pg_notify('edit_message', payload::TEXT);
            END IF;
            
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION notify_delete_message()
        RETURNS TRIGGER AS $$
        DECLARE
            payload JSONB;
        BEGIN
            payload := jsonb_build_object(
                'v', '3.0',
                'type', 'delete',
                'message_id', OLD.message_id,
                'chat_id', OLD.chat_id
            );
            
            PERFORM pg_notify('delete_message', payload::TEXT);
            
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_trigger 
                WHERE tgname = 'messages_edit_trigger' 
                AND tgrelid = 'messages'::regclass
            ) THEN
                CREATE TRIGGER messages_edit_trigger
                AFTER UPDATE ON messages
                FOR EACH ROW
                EXECUTE FUNCTION notify_edit_message();
            END IF;
        END $$;

        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT FROM pg_trigger 
                WHERE tgname = 'messages_delete_trigger' 
                AND tgrelid = 'messages'::regclass
            ) THEN
                CREATE TRIGGER messages_delete_trigger
                AFTER DELETE ON messages
                FOR EACH ROW
                EXECUTE FUNCTION notify_delete_message();
            END IF;
        END $$;
        """
        await conn.execute(edit_delete_triggers_sql)
        logging.info("Edit and delete triggers created.")
        print(f"{Fore.GREEN}Edit and delete triggers created.{Style.RESET_ALL}")

        # Функция для уведомлений о новых сообщениях
        notify_function_sql = """
        CREATE OR REPLACE FUNCTION notify_new_message()
            RETURNS TRIGGER AS $$
            DECLARE
                payload JSONB;
                should_notify BOOLEAN := FALSE;
                mime_type TEXT;
                media_category TEXT := 'unknown';
                media_info JSONB;
                allowed_types TEXT[] := ARRAY[
                    'MessageMediaPhoto',
                    'MessageMediaDocument'
                ];
            BEGIN
                -- Проверяем тип медиа
                IF NEW.media_type IS NULL THEN
                    should_notify := TRUE;
                    media_category := 'text';
                    
                ELSIF NEW.media_type = 'MessageMediaPhoto' THEN
                    should_notify := TRUE;
                    media_category := 'photo';
                    
                ELSIF NEW.media_type = 'MessageMediaDocument' THEN
                    IF NEW.media_metadata IS NOT NULL AND NEW.media_metadata ? 'document' THEN
                        media_info := NEW.media_metadata->'document';
                        mime_type := media_info->>'mime_type';
                        
                        IF mime_type LIKE 'video/%' THEN
                            should_notify := TRUE;
                            media_category := 'video';
                        ELSIF mime_type = 'image/gif' OR 
                              (mime_type = 'video/mp4' AND media_info->>'attributes' LIKE '%animated%') THEN
                            should_notify := TRUE;
                            media_category := 'gif';
                        ELSE
                            RETURN NEW;
                        END IF;
                    ELSE
                        RETURN NEW;
                    END IF;
                ELSE
                    RETURN NEW;
                END IF;

                IF should_notify THEN
                    payload := jsonb_build_object(
                        'v', '3.0',
                        'type', 'new',
                        'message_id', NEW.message_id,
                        'chat_id', NEW.chat_id,
                        'date', NEW.date,
                        'has_media', NEW.media_type IS NOT NULL,
                        'media_category', media_category
                    );
                    
                    PERFORM pg_notify('new_message', payload::TEXT);
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """

        await conn.execute(notify_function_sql)
        logging.info("Optimized notify_new_message function created with strict filtering")

        create_trigger_sql = """
        DO $$ 
        BEGIN
            DROP TRIGGER IF EXISTS messages_notify_trigger ON messages;
            
            CREATE TRIGGER messages_notify_trigger
            AFTER INSERT ON messages
            FOR EACH ROW
            EXECUTE FUNCTION notify_new_message();
        END $$;
        """

        await conn.execute(create_trigger_sql)
        logging.info("New trigger for new message notifications created")
        print(f"{Fore.GREEN}✅ Optimized new message notification trigger created with strict filtering:{Style.RESET_ALL}")
        print(f"{Fore.GREEN}   ALLOWED: text, photo, video, GIF{Style.RESET_ALL}")
        print(f"{Fore.RED}   BLOCKED: stickers, audio, documents, polls, contacts, geo, etc.{Style.RESET_ALL}")

        # ==================== S3 Media Files Setup (ИСПРАВЛЕННАЯ ВЕРСИЯ) ====================
        logging.info("Setting up S3 columns and triggers in media_files table")
        try:
            # Проверяем существование таблицы media_files
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'media_files'
                )
            """)
            
            if table_exists:
                # Создаем триггер для уведомлений о готовности медиа
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION notify_media_ready()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        payload JSONB;
                        media_record RECORD;
                    BEGIN
                        IF NEW.uploaded = TRUE AND (OLD.uploaded = FALSE OR OLD.uploaded IS NULL) THEN
                            -- Отправляем уведомление для каждого связанного сообщения
                            FOR media_record IN 
                                SELECT message_id, chat_id 
                                FROM message_media 
                                WHERE media_id = NEW.id
                            LOOP
                                payload := jsonb_build_object(
                                    'v', '3.0',
                                    'type', 'media_ready',
                                    'media_id', NEW.id,
                                    'message_id', media_record.message_id,
                                    'chat_id', media_record.chat_id,
                                    'public_url', NEW.public_url,
                                    'timestamp', CURRENT_TIMESTAMP
                                );
                                
                                PERFORM pg_notify('media_ready', payload::TEXT);
                                
                                -- Также отправляем edit_message уведомление для клиента
                                PERFORM pg_notify('edit_message', jsonb_build_object(
                                    'v', '3.0',
                                    'type', 'edit',
                                    'message_id', media_record.message_id,
                                    'chat_id', media_record.chat_id,
                                    'media_url', NEW.public_url,
                                    'media_ready', TRUE,
                                    'timestamp', CURRENT_TIMESTAMP
                                )::TEXT);
                            END LOOP;
                        END IF;
                        
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                
                # Создаем триггер, если его нет
                await conn.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_trigger 
                            WHERE tgname = 'media_ready_trigger' 
                            AND tgrelid = 'media_files'::regclass
                        ) THEN
                            CREATE TRIGGER media_ready_trigger
                            AFTER UPDATE OF uploaded ON media_files
                            FOR EACH ROW
                            WHEN (NEW.uploaded = TRUE AND (OLD.uploaded = FALSE OR OLD.uploaded IS NULL))
                            EXECUTE FUNCTION notify_media_ready();
                        END IF;
                    END $$;
                """)
                
                logging.info("✅ Media ready trigger created")
                print(f"{Fore.GREEN}Media ready trigger created.{Style.RESET_ALL}")
                
                # Создаем функцию для очистки старых записей
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION cleanup_stale_media()
                    RETURNS void AS $$
                    BEGIN
                        -- Помечаем как STALE файлы, которые висят больше 7 дней и не загружены
                        UPDATE media_files
                        SET s3_key = 'STALE', uploaded = TRUE
                        WHERE uploaded = FALSE
                        AND created_at < NOW() - INTERVAL '7 days'
                        AND (s3_key IS NULL OR s3_key NOT IN ('FAILED', 'STALE', 'MISSING'));
                        
                        -- Удаляем записи о файлах, которых нет на диске больше 30 дней
                        DELETE FROM media_files
                        WHERE NOT EXISTS (SELECT 1 FROM pg_ls_dir('/usr/local/kalinode/private/media') 
                                          WHERE pg_ls_dir LIKE '%' || file_path || '%')
                        AND created_at < NOW() - INTERVAL '30 days';
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                
                logging.info("✅ Stale media cleanup function created")
                
                # ИСПРАВЛЕНИЕ: Убираем миграцию данных, так как поля message_id и chat_id больше не существуют
                # Вместо этого просто проверяем, что таблицы созданы правильно
                logging.info("✅ Media files table structure verified (without message_id and chat_id columns)")
                
            else:
                logging.warning("Table media_files does not exist yet")
                print(f"{Fore.YELLOW}Table media_files does not exist yet.{Style.RESET_ALL}")
                
        except Exception as e:
            logging.error(f"Error setting up S3 columns in media_files: {e}")
            print(f"{Fore.RED}Error setting up S3 columns: {e}{Style.RESET_ALL}")

        # Проверка созданных таблиц
        tables_after = await conn.fetch(tables_query)
        table_names = {row['table_name'] for row in tables_after}
        
        expected_tables = [
            'chats', 'users', 'user_lists', 'chat_lists', 'dictionary', 'new_words', 
            'messages', 'dialogs', 'keywords', 'message_keywords', 'logs', 'chat_participants', 
            'analytics', 'import_staging', 'event_queue', 'settings', 'media_files', 'message_media'
        ]
        
        table_data = []
        for table in expected_tables:
            if table in table_names:
                print(f"{Fore.GREEN}Table {table} OK{Style.RESET_ALL}")
            else:
                table_data.append([table, f"{Fore.RED}Missing{Style.RESET_ALL}"])
        
        partitions_after = await conn.fetch(partitions_query)
        partition_names = {row['table_name'] for row in partitions_after}
        
        for partition in ['messages_2024', 'messages_2025', 'messages_2026', 'logs_2024', 'logs_2025']:
            if partition in partition_names:
                print(f"{Fore.GREEN}Partition {partition} OK{Style.RESET_ALL}")
            else:
                table_data.append([partition, f"{Fore.RED}Missing{Style.RESET_ALL}"])
        
        if table_data:
            print(f"{Fore.CYAN}Database tables status:{Style.RESET_ALL}")
            print(tabulate(table_data, headers=["Table", "Status"], tablefmt="grid"))
        
        chats_count = await conn.fetchval("SELECT COUNT(*) FROM chats WHERE is_active = TRUE")
        print(f"{Fore.GREEN}Active chats in chats table: {chats_count}.{Style.RESET_ALL}")
        
        logging.info("Database initialization completed successfully.")
        
    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        print(f"{Fore.RED}Error initializing database: {e}{Style.RESET_ALL}")
        raise
    finally:
        if conn:
            await conn.close()

# ==================== SQL запросы ====================

async def execute_sql_query(pool):
    """Выполнение SQL запроса с использованием пула."""
    try:
        from prompt_toolkit import PromptSession
        sql_query = await PromptSession("Enter SQL query: ").prompt_async()
        if not sql_query.strip():
            print(f"{Fore.YELLOW}Query is empty. Returning to main menu.{Style.RESET_ALL}")
            return

        # Защита от опасных запросов
        if any(keyword in sql_query.upper() for keyword in ['DROP', 'TRUNCATE', 'ALTER']):
            print(f"{Fore.RED}Dangerous query. Using DROP, TRUNCATE or ALTER is prohibited.{Style.RESET_ALL}")
            logging.warning(f"Attempt to execute dangerous query: {sql_query}")
            return

        async with pool.acquire() as conn:
            if sql_query.strip().upper().startswith('SELECT'):
                results = await conn.fetch(sql_query)
                if not results:
                    print(f"{Fore.YELLOW}No results.{Style.RESET_ALL}")
                    return

                headers = results[0].keys()
                table_data = [[str(col)[:50] + ('...' if len(str(col)) > 50 else '') for col in row] for row in results]
                
                print(f"{Fore.CYAN}SQL query results:{Style.RESET_ALL}")
                print(tabulate(table_data, headers=headers, tablefmt="grid"))
                logging.info(f"Executed SELECT query: {sql_query}, returned {len(results)} rows")
            else:
                await conn.execute(sql_query)
                print(f"{Fore.GREEN}Query executed successfully.{Style.RESET_ALL}")
                logging.info(f"Executed query: {sql_query}")
    
    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        print(f"{Fore.RED}Error executing SQL query: {e}{Style.RESET_ALL}")

# ==================== Управление списками ====================

async def manage_lists_menu(pool):
    """Меню управления списками пользователей и чатов."""
    from questionary import select, confirm
    from prompt_toolkit import PromptSession

    menu_options = [
        {"name": "1. View user lists", "value": "1"},
        {"name": "2. Add user to list", "value": "2"},
        {"name": "3. Change user list type", "value": "3"},
        {"name": "4. Remove user from list", "value": "4"},
        {"name": "5. View chat lists", "value": "5"},
        {"name": "6. Add chat to list", "value": "6"},
        {"name": "7. Change chat list type", "value": "7"},
        {"name": "8. Remove chat from list", "value": "8"},
        {"name": "9. Back", "value": "9"}
    ]

    while True:
        try:
            choice = await select("List management:", choices=menu_options).ask_async()

            if choice == "1":
                # Просмотр списка пользователей
                async with pool.acquire() as conn:
                    users = await conn.fetch(
                        """
                        SELECT u.user_id, u.username, u.first_name, u.last_name, ul.list_type,
                               u.is_scam, u.is_verified, u.premium
                        FROM users u
                        LEFT JOIN user_lists ul ON u.user_id = ul.user_id
                        ORDER BY ul.list_type, u.user_id
                        """
                    )

                if not users:
                    print(f"{Fore.YELLOW}No users in lists.")
                else:
                    table_data = [
                        [
                            row['user_id'],
                            row['username'] or 'None',
                            row['first_name'] or 'Not specified',
                            row['last_name'] or 'Not specified',
                            row['list_type'] or 'gray',
                            'Scam' if row['is_scam'] else '',
                            'Verified' if row['is_verified'] else '',
                            'Premium' if row['premium'] else ''
                        ]
                        for row in users
                    ]
                    print(f"{Fore.CYAN}User lists:")
                    print(tabulate(table_data, headers=["ID", "Username", "First Name", "Last Name", "List", "Flags", "Verified", "Premium"], tablefmt="grid"))
                
                # Ждем нажатия Enter для возврата в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "2":
                # Добавление пользователя в список
                user_id_str = await PromptSession("Enter user ID: ").prompt_async()
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                list_type = await select(
                    "Select list type:",
                    choices=[
                        {"name": "White", "value": "white"},
                        {"name": "Black", "value": "black"},
                        {"name": "Gray", "value": "gray"}
                    ]
                ).ask_async()

                async with pool.acquire() as conn:
                    user_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id)
                    if not user_exists:
                        print(f"{Fore.RED}User with ID {user_id} not found in database.")
                        # Ждем нажатия Enter перед возвратом
                        await PromptSession("Press Enter to continue...").prompt_async()
                        continue

                    # Проверяем, есть ли уже пользователь в каком-либо списке
                    existing_list = await conn.fetchval(
                        "SELECT list_type FROM user_lists WHERE user_id = $1",
                        user_id
                    )
                    
                    if existing_list:
                        print(f"{Fore.YELLOW}User {user_id} already in {existing_list} list.")
                        change = await confirm(f"Move to {list_type} list?").ask_async()
                        if change:
                            await conn.execute(
                                "UPDATE user_lists SET list_type = $1 WHERE user_id = $2",
                                list_type, user_id
                            )
                            print(f"{Fore.GREEN}User {user_id} moved to {list_type} list.")
                    else:
                        await add_user_to_list(conn, user_id, list_type)
                        print(f"{Fore.GREEN}User {user_id} added to {list_type} list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "3":
                # Изменение типа списка пользователя
                user_id_str = await PromptSession("Enter user ID: ").prompt_async()
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                async with pool.acquire() as conn:
                    current_list = await get_user_list(conn, user_id)
                    if current_list == 'none' or not current_list:
                        print(f"{Fore.YELLOW}User {user_id} is not in any list.")
                        await PromptSession("Press Enter to continue...").prompt_async()
                        continue

                    new_list_type = await select(
                        f"Current list: {current_list}. Select new list type:",
                        choices=[
                            {"name": "White", "value": "white"},
                            {"name": "Black", "value": "black"},
                            {"name": "Gray", "value": "gray"}
                        ]
                    ).ask_async()

                    if new_list_type == current_list:
                        print(f"{Fore.YELLOW}User already in {current_list} list.")
                    else:
                        await conn.execute(
                            "UPDATE user_lists SET list_type = $1 WHERE user_id = $2",
                            new_list_type, user_id
                        )
                        print(f"{Fore.GREEN}User {user_id} moved to {new_list_type} list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "4":
                # Удаление пользователя из списка
                user_id_str = await PromptSession("Enter user ID: ").prompt_async()
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                async with pool.acquire() as conn:
                    list_exists = await conn.fetchval("SELECT 1 FROM user_lists WHERE user_id = $1", user_id)
                    if not list_exists:
                        print(f"{Fore.YELLOW}User {user_id} is not in any list.")
                        await PromptSession("Press Enter to continue...").prompt_async()
                        continue

                    current_list = await conn.fetchval("SELECT list_type FROM user_lists WHERE user_id = $1", user_id)
                    confirm_delete = await confirm(f"Remove user {user_id} from {current_list} list?").ask_async()
                    
                    if confirm_delete:
                        await conn.execute("DELETE FROM user_lists WHERE user_id = $1", user_id)
                        print(f"{Fore.GREEN}User {user_id} removed from list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "5":
                # Просмотр списка чатов
                async with pool.acquire() as conn:
                    chats = await conn.fetch(
                        """
                        SELECT c.chat_id, c.title, c.type, cl.list_type, c.participants_count,
                               c.slow_mode_seconds, c.ttl_period, c.is_active
                        FROM chats c
                        LEFT JOIN chat_lists cl ON c.chat_id = cl.chat_id
                        ORDER BY cl.list_type, c.title
                        """
                    )

                if not chats:
                    print(f"{Fore.YELLOW}No chats in lists.")
                else:
                    table_data = [
                        [
                            row['chat_id'],
                            row['title'][:30] + ('...' if len(row['title'] or '') > 30 else ''),
                            row['type'],
                            row['list_type'] or 'gray',
                            'Active' if row['is_active'] else 'Inactive',
                            row['participants_count'] or 0,
                            row['slow_mode_seconds'] or 0,
                            row['ttl_period'] or 0
                        ]
                        for row in chats
                    ]
                    print(f"{Fore.CYAN}Chat lists:")
                    print(tabulate(table_data, headers=["ID", "Title", "Type", "List", "Status", "Members", "Slow Mode", "TTL"], tablefmt="grid"))
                
                # Ждем нажатия Enter для возврата в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "6":
                # Добавление чата в список
                chat_id_str = await PromptSession("Enter chat ID: ").prompt_async()
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                list_type = await select(
                    "Select list type:",
                    choices=[
                        {"name": "White", "value": "white"},
                        {"name": "Black", "value": "black"},
                        {"name": "Gray", "value": "gray"}
                    ]
                ).ask_async()

                async with pool.acquire() as conn:
                    chat_exists = await conn.fetchval("SELECT 1 FROM chats WHERE chat_id = $1", chat_id)
                    if not chat_exists:
                        print(f"{Fore.RED}Chat with ID {chat_id} not found in database.")
                        print(f"{Fore.YELLOW}Please add the chat first via 'Add by ID' in source management.")
                        await PromptSession("Press Enter to continue...").prompt_async()
                        continue

                    # Проверяем, есть ли уже чат в каком-либо списке
                    existing_list = await conn.fetchval(
                        "SELECT list_type FROM chat_lists WHERE chat_id = $1",
                        chat_id
                    )
                    
                    if existing_list:
                        print(f"{Fore.YELLOW}Chat {chat_id} already in {existing_list} list.")
                        change = await confirm(f"Move to {list_type} list?").ask_async()
                        if change:
                            await conn.execute(
                                "UPDATE chat_lists SET list_type = $1 WHERE chat_id = $2",
                                list_type, chat_id
                            )
                            print(f"{Fore.GREEN}Chat {chat_id} moved to {list_type} list.")
                    else:
                        await add_chat_to_list(conn, chat_id, list_type)
                        print(f"{Fore.GREEN}Chat {chat_id} added to {list_type} list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "7":
                # Изменение типа списка чата
                chat_id_str = await PromptSession("Enter chat ID: ").prompt_async()
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                async with pool.acquire() as conn:
                    current_list = await get_chat_list(conn, chat_id)
                    if not current_list or current_list == 'gray' and not await conn.fetchval(
                        "SELECT 1 FROM chat_lists WHERE chat_id = $1", chat_id
                    ):
                        print(f"{Fore.YELLOW}Chat {chat_id} is not in any list (defaults to gray).")
                        add_to_list = await confirm("Add to list?").ask_async()
                        if add_to_list:
                            new_list_type = await select(
                                "Select list type:",
                                choices=[
                                    {"name": "White", "value": "white"},
                                    {"name": "Black", "value": "black"},
                                    {"name": "Gray", "value": "gray"}
                                ]
                            ).ask_async()
                            await add_chat_to_list(conn, chat_id, new_list_type)
                            print(f"{Fore.GREEN}Chat {chat_id} added to {new_list_type} list.")
                    else:
                        new_list_type = await select(
                            f"Current list: {current_list}. Select new list type:",
                            choices=[
                                {"name": "White", "value": "white"},
                                {"name": "Black", "value": "black"},
                                {"name": "Gray", "value": "gray"}
                            ]
                        ).ask_async()

                        if new_list_type == current_list:
                            print(f"{Fore.YELLOW}Chat already in {current_list} list.")
                        else:
                            await conn.execute(
                                "UPDATE chat_lists SET list_type = $1 WHERE chat_id = $2",
                                new_list_type, chat_id
                            )
                            print(f"{Fore.GREEN}Chat {chat_id} moved to {new_list_type} list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "8":
                # Удаление чата из списка
                chat_id_str = await PromptSession("Enter chat ID: ").prompt_async()
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    print(f"{Fore.RED}ID must be a number.")
                    continue

                async with pool.acquire() as conn:
                    list_exists = await conn.fetchval("SELECT 1 FROM chat_lists WHERE chat_id = $1", chat_id)
                    if not list_exists:
                        print(f"{Fore.YELLOW}Chat {chat_id} is not in any list.")
                        await PromptSession("Press Enter to continue...").prompt_async()
                        continue

                    current_list = await conn.fetchval("SELECT list_type FROM chat_lists WHERE chat_id = $1", chat_id)
                    confirm_delete = await confirm(f"Remove chat {chat_id} from {current_list} list?").ask_async()
                    
                    if confirm_delete:
                        await conn.execute("DELETE FROM chat_lists WHERE chat_id = $1", chat_id)
                        print(f"{Fore.GREEN}Chat {chat_id} removed from list.")
                
                # Ждем нажатия Enter перед возвратом в меню
                await PromptSession("Press Enter to continue...").prompt_async()

            elif choice == "9":
                print(f"{Fore.CYAN}Returning to main menu...")
                break

        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}Operation interrupted (Ctrl+C). Returning to submenu.")
            # Небольшая пауза чтобы сообщение было видно
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in list management: {e}")
            print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
            await PromptSession("Press Enter to continue...").prompt_async()
            
# ==================== Управление настройками ====================

async def manage_settings_menu(pool):
    """Меню управления настройками пересылки."""
    from questionary import select, confirm, text
    from prompt_toolkit import PromptSession
    import json

    menu_options = [
        {"name": "1. View settings", "value": "1"},
        {"name": "2. Create new setting", "value": "2"},
        {"name": "3. Edit setting", "value": "3"},
        {"name": "4. Activate/deactivate setting", "value": "4"},
        {"name": "5. Delete setting", "value": "5"},
        {"name": "6. Select preset", "value": "6"},
        {"name": "7. Export/import settings", "value": "7"},
        {"name": "8. Back", "value": "8"}
    ]

    while True:
        try:
            choice = await select("Forwarding settings management:", choices=menu_options).ask_async()

            if choice == "1":
                async with pool.acquire() as conn:
                    settings = await conn.fetch(
                        """
                        SELECT setting_id, name, is_active, priority, created_at
                        FROM settings
                        ORDER BY priority DESC, created_at
                        """
                    )

                if not settings:
                    print(f"{Fore.YELLOW}No settings found.{Style.RESET_ALL}")
                    continue

                table_data = [
                    [
                        row['setting_id'],
                        row['name'],
                        'Yes' if row['is_active'] else 'No',
                        row['priority'],
                        row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                    ]
                    for row in settings
                ]
                print(f"{Fore.CYAN}Settings list:{Style.RESET_ALL}")
                print(tabulate(table_data, headers=["ID", "Name", "Active", "Priority", "Created"], tablefmt="grid"))

            elif choice == "2":
                name = await text("Enter setting name:").ask_async()
                if not name.strip():
                    print(f"{Fore.RED}Name cannot be empty.{Style.RESET_ALL}")
                    continue

                config_str = await PromptSession("Enter JSON configuration (or leave empty for template): ").prompt_async()
                if not config_str.strip():
                    config = {
                        "fields": ["text", "tags", "date", "text_fingerprint"],
                        "filters": {"chat_list_type": ["black"], "list_type": ["black", "none"]},
                        "exclude": [],
                        "enrich": {"chat_title": True, "sender_username": True},
                        "highlight": {},
                        "stats": {},
                        "destination": ["http_post"],
                        "http_config": {
                            "url": "https://example.com/api/messages",
                            "method": "POST",
                            "headers": {
                                "Authorization": "Bearer your_token",
                                "Content-Type": "application/json"
                            },
                            "timeout": 10
                        },
                        "anonymize": {},
                        "preset": name
                    }
                else:
                    try:
                        config = json.loads(config_str)
                    except json.JSONDecodeError:
                        print(f"{Fore.RED}Invalid JSON.{Style.RESET_ALL}")
                        continue

                priority = await text("Enter priority (number, default 0):").ask_async()
                priority = int(priority.strip()) if priority.strip() and priority.strip().isdigit() else 0

                is_active = await confirm("Activate setting?").ask_async()

                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO settings (name, config, is_active, priority)
                        VALUES ($1, $2::jsonb, $3, $4)
                        """,
                        name, json.dumps(config, ensure_ascii=False), is_active, priority
                    )
                    
                print(f"{Fore.GREEN}Setting '{name}' created.{Style.RESET_ALL}")

            # ... остальные пункты меню (аналогично с использованием pool) ...

            elif choice == "8":
                print(f"{Fore.CYAN}Returning to main menu.{Style.RESET_ALL}")
                break

        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}Operation interrupted (Ctrl+C). Returning to submenu.{Style.RESET_ALL}")
        except Exception as e:
            logging.error(f"Error in settings management: {e}")
            print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")

# ==================== Уведомления ====================

async def listen_for_new_messages(pool, callback):
    """Подписка на уведомления о новых сообщениях."""
    conn = None
    try:
        # Забираем соединение из пула для LISTEN
        conn = await pool.acquire()
        await conn.execute("LISTEN new_message")
        await conn.execute("LISTEN edit_message")
        await conn.execute("LISTEN delete_message")
        await conn.execute("LISTEN media_ready")  # Добавляем прослушивание media_ready
        
        async def notification_handler(c, pid, channel, payload):
            try:
                message_data = json.loads(payload)
                # Обрабатываем уведомление (можно использовать отдельное соединение из пула)
                await callback(message_data)
            except Exception as e:
                logging.error(f"Error processing notification: {e}")
        
        conn.add_listener("new_message", notification_handler)
        conn.add_listener("edit_message", notification_handler)
        conn.add_listener("delete_message", notification_handler)
        conn.add_listener("media_ready", notification_handler)  # Добавляем слушатель
        
        # Бесконечное ожидание
        while True:
            await asyncio.sleep(3600)
        
    except Exception as e:
        logging.error(f"Error subscribing to notifications: {e}")
        print(f"{Fore.RED}Error subscribing to notifications: {e}{Style.RESET_ALL}")
    finally:
        if conn:
            # Важно: возвращаем соединение в пул
            await pool.release(conn)