# process_event_queue.py
import asyncpg
import asyncio
import json
import logging
from datetime import datetime
from colorama import Fore, Style
from db_module import db_config, extract_ner, extract_keywords, print_message
from logger import setup_logging

# Конфигурация очереди
BATCH_SIZE = 10  # Количество событий для пакетной обработки
POLL_INTERVAL = 0.1  # Интервал опроса в секундах
MAX_RETRIES = 3  # Максимальное количество попыток

async def process_queue(pool):
    """
    Обработка очереди событий для анализа текста с использованием пула соединений.
    
    Args:
        pool: пул соединений с БД (обязательный параметр)
    """
    if pool is None:
        raise ValueError("process_queue requires a connection pool")
    
    logging.info("Starting optimized event queue processor")
    print_message("Event queue processor started", level="info")
    
    while True:
        try:
            # Получаем пачку событий для обработки
            events = await fetch_events_batch(pool)
            
            if not events:
                # Если нет событий, немного ждем
                await asyncio.sleep(POLL_INTERVAL)
                continue
            
            # Обрабатываем каждое событие
            for event in events:
                try:
                    await process_single_event(event, pool)
                except Exception as e:
                    logging.error(f"Error processing event {event['event_id']}: {e}")
                    # Обновляем статус ошибки
                    await update_event_status(
                        pool, 
                        event['event_id'], 
                        'failed', 
                        str(e)
                    )
            
            # Небольшая пауза между батчами
            await asyncio.sleep(0.01)
            
        except asyncio.CancelledError:
            logging.info("Event queue processor stopped")
            print_message("Event queue processor stopped", level="warning")
            break
        except Exception as e:
            logging.error(f"Critical error in queue processor: {e}")
            print_message(f"Queue processor error: {e}", level="error")
            await asyncio.sleep(5)  # Пауза перед повторной попыткой

async def fetch_events_batch(pool):
    """
    Получение пачки событий для обработки с использованием SKIP LOCKED.
    
    Args:
        pool: пул соединений с БД
        
    Returns:
        list: список событий для обработки
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Атомарное получение и блокировка событий
            events = await conn.fetch(f"""
                UPDATE event_queue
                SET status = 'processing',
                    attempts = attempts + 1,
                    started_at = CURRENT_TIMESTAMP
                WHERE event_id IN (
                    SELECT event_id
                    FROM event_queue
                    WHERE status = 'pending'
                    AND attempts < max_attempts
                    AND event_type = 'text_analysis'
                    ORDER BY priority DESC, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT {BATCH_SIZE}
                )
                RETURNING event_id, event_data, attempts, max_attempts, priority
            """)
            
            return [dict(event) for event in events]

async def process_single_event(event, pool):
    """
    Обработка одного события.
    
    Args:
        event: словарь с данными события
        pool: пул соединений с БД
    """
    event_id = event['event_id']
    event_data = json.loads(event['event_data'])
    
    # Валидация данных
    if not isinstance(event_data, dict):
        raise ValueError(f"Invalid event data format: {type(event_data)}")
    
    if 'text' not in event_data:
        raise ValueError(f"Missing 'text' field in event data")
    
    if 'message_id' not in event_data or 'chat_id' not in event_data:
        raise ValueError(f"Missing message_id or chat_id in event data")
    
    logging.debug(f"Processing event {event_id} for message {event_data['message_id']}")
    
    try:
        # Извлечение NER и ключевых слов (ВНЕ соединения)
        text = event_data['text']
        ner_results = extract_ner(text)
        keywords = extract_keywords(text)
        
        # Формируем теги
        tags = {
            'ner': ner_results,
            'keywords': [kw['keyword'] for kw in keywords],
            'keywords_with_scores': keywords,
            'processed_at': datetime.utcnow().isoformat()
        }
        
        # Обновляем сообщение в БД (короткое соединение)
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем теги сообщения
                result = await conn.execute("""
                    UPDATE messages 
                    SET tags = tags || $1::jsonb
                    WHERE message_id = $2 AND chat_id = $3
                """, json.dumps(tags), event_data['message_id'], event_data['chat_id'])
                
                # Проверяем, было ли обновлено сообщение
                if result == "UPDATE 0":
                    logging.warning(f"Message {event_data['message_id']} not found in chat {event_data['chat_id']}")
                
                # Добавляем ключевые слова в отдельную таблицу
                for kw in keywords:
                    await conn.execute("""
                        INSERT INTO message_keywords (message_id, date, keyword_id, score)
                        SELECT $1, m.date, k.keyword_id, $3
                        FROM messages m, keywords k
                        WHERE m.message_id = $1 
                          AND m.chat_id = $2
                          AND k.keyword = $4
                        ON CONFLICT DO NOTHING
                    """, event_data['message_id'], event_data['chat_id'], 
                         kw['score'], kw['keyword'])
                
                # Отправка уведомления о завершении обработки
                await conn.execute("""
                    SELECT pg_notify('text_analysis_complete', $1::text)
                """, json.dumps({
                    'message_id': event_data['message_id'],
                    'chat_id': event_data['chat_id'],
                    'tags': tags
                }))
                
                # Отмечаем событие как выполненное
                await conn.execute("""
                    UPDATE event_queue
                    SET status = 'completed',
                        completed_at = CURRENT_TIMESTAMP,
                        result = $1
                    WHERE event_id = $2
                """, json.dumps({'tags': tags}), event_id)
        
        logging.info(f"Successfully processed event {event_id} for message {event_data['message_id']}")
        
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in event {event_id}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing event {event_id}: {e}")
        raise

async def update_event_status(pool, event_id, status, error_message=None):
    """
    Обновление статуса события.
    
    Args:
        pool: пул соединений с БД
        event_id: ID события
        status: новый статус ('failed' или 'pending')
        error_message: сообщение об ошибке (опционально)
    """
    try:
        async with pool.acquire() as conn:
            if status == 'failed':
                # Проверяем, не превышен ли лимит попыток
                event = await conn.fetchrow("""
                    SELECT attempts, max_attempts 
                    FROM event_queue 
                    WHERE event_id = $1
                """, event_id)
                
                if event and event['attempts'] >= event['max_attempts']:
                    # Если превышен лимит, оставляем как failed
                    await conn.execute("""
                        UPDATE event_queue
                        SET status = 'failed',
                            last_error = $1,
                            error_at = CURRENT_TIMESTAMP
                        WHERE event_id = $2
                    """, error_message, event_id)
                    logging.warning(f"Event {event_id} failed permanently after {event['attempts']} attempts")
                else:
                    # Если есть еще попытки, возвращаем в pending
                    await conn.execute("""
                        UPDATE event_queue
                        SET status = 'pending',
                            last_error = $1,
                            error_at = CURRENT_TIMESTAMP
                        WHERE event_id = $2
                    """, error_message, event_id)
                    logging.warning(f"Event {event_id} failed, will retry (attempt {event['attempts'] + 1})")
            else:
                await conn.execute("""
                    UPDATE event_queue
                    SET status = $1
                    WHERE event_id = $2
                """, status, event_id)
                
    except Exception as e:
        logging.error(f"Error updating event {event_id} status: {e}")

async def get_queue_stats(pool):
    """
    Получение статистики очереди.
    
    Args:
        pool: пул соединений с БД
        
    Returns:
        dict: статистика очереди
    """
    try:
        async with pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COALESCE(AVG(EXTRACT(EPOCH FROM (completed_at - created_at))), 0) as avg_processing_time,
                    MAX(priority) as max_priority
                FROM event_queue
                WHERE event_type = 'text_analysis'
            """)
            
            return dict(stats) if stats else {
                'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0,
                'avg_processing_time': 0, 'max_priority': 0
            }
            
    except Exception as e:
        logging.error(f"Error getting queue stats: {e}")
        return {
            'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0,
            'avg_processing_time': 0, 'max_priority': 0
        }

async def cleanup_old_events(pool, days_to_keep=7):
    """
    Очистка старых завершенных событий.
    
    Args:
        pool: пул соединений с БД
        days_to_keep: количество дней для хранения завершенных событий
    """
    try:
        async with pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM event_queue
                WHERE status IN ('completed', 'failed')
                AND completed_at < CURRENT_TIMESTAMP - $1::interval
            """, f"{days_to_keep} days")
            
            logging.info(f"Cleaned up old events: {result}")
            
    except Exception as e:
        logging.error(f"Error cleaning up old events: {e}")

async def add_analysis_event(pool, message_id, chat_id, text, priority=1):
    """
    Добавление события для анализа текста.
    
    Args:
        pool: пул соединений с БД
        message_id: ID сообщения
        chat_id: ID чата
        text: текст для анализа
        priority: приоритет (по умолчанию 1)
    """
    try:
        event_data = {
            'message_id': message_id,
            'chat_id': chat_id,
            'text': text,
            'created_at': datetime.utcnow().isoformat()
        }
        
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO event_queue (
                    event_type, event_data, priority, status, max_attempts
                ) VALUES ($1, $2, $3, 'pending', $4)
            """, 'text_analysis', json.dumps(event_data), priority, MAX_RETRIES)
            
        logging.debug(f"Added analysis event for message {message_id}")
        
    except Exception as e:
        logging.error(f"Error adding analysis event: {e}")

# Для обратной совместимости
async def process_queue_legacy():
    """
    Устаревшая функция для обратной совместимости.
    Используйте process_queue(pool) вместо этой.
    """
    from db_module import create_db_pool
    
    print_message(
        "Warning: process_queue_legacy() is deprecated. "
        "Use process_queue(pool) instead.",
        level="warning"
    )
    
    # Создаем временный пул (не рекомендуется, но для совместимости)
    pool = await create_db_pool()
    try:
        await process_queue(pool)
    finally:
        await pool.close()

# Если файл запускается напрямую
if __name__ == '__main__':
    import asyncio
    from datetime import datetime
    from db_module import create_db_pool
    
    async def main():
        """Точка входа при прямом запуске."""
        setup_logging()
        print_message("Starting event queue processor as standalone", level="info")
        
        # Создаем пул соединений
        pool = await create_db_pool()
        
        try:
            # Запускаем обработчик очереди
            queue_task = asyncio.create_task(process_queue(pool))
            
            # Запускаем периодическую очистку (раз в день)
            async def periodic_cleanup():
                while True:
                    await asyncio.sleep(86400)  # 24 часа
                    await cleanup_old_events(pool)
            
            cleanup_task = asyncio.create_task(periodic_cleanup())
            
            # Ждем завершения
            await queue_task
            
        except KeyboardInterrupt:
            print_message("Received shutdown signal", level="warning")
            queue_task.cancel()
            cleanup_task.cancel()
        finally:
            await pool.close()
            print_message("Event queue processor stopped", level="info")
    
    asyncio.run(main())