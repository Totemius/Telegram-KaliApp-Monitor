# s3_recovery.py - исправленная версия для работы с улучшенным S3Uploader
import asyncio
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

class S3Recovery:
    def __init__(self, pool, s3_uploader, batch_size: int = 50):
        """
        Инициализация recovery-воркера для повторной попытки загрузки в S3.
        
        Args:
            pool: пул соединений с БД
            s3_uploader: экземпляр S3Uploader
            batch_size: количество файлов для проверки за один раз
        """
        self.pool = pool
        self.s3_uploader = s3_uploader
        self.batch_size = batch_size
        self.is_running = False
        self.recovery_task = None
        
    async def start(self):
        """Запуск recovery-воркера."""
        self.is_running = True
        self.recovery_task = asyncio.create_task(self._recovery_worker())
        logging.info("🔄 S3 recovery worker started")
        
    async def stop(self):
        """Остановка recovery-воркера."""
        self.is_running = False
        if self.recovery_task:
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                pass
        logging.info("🔄 S3 recovery worker stopped")
                
    async def _recovery_worker(self):
        """Проверка каждые 30 минут с умной сортировкой"""
        while self.is_running:
            try:
                await self._check_pending_uploads()
                await asyncio.sleep(1800)  # 30 минут
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Recovery worker error: {e}")
                await asyncio.sleep(60)
    
    async def _get_queue_sizes(self) -> tuple:
        """
        Получение размеров очередей из S3Uploader.
        
        Returns:
            tuple: (small_queue_size, large_queue_size, total_size)
        """
        try:
            # Пробуем получить статистику
            if hasattr(self.s3_uploader, 'get_queue_stats'):
                stats = await self.s3_uploader.get_queue_stats()
                small_size = stats.get('small_queue_size', 0)
                large_size = stats.get('large_queue_size', 0)
                return small_size, large_size, small_size + large_size
        except Exception as e:
            logging.debug(f"Could not get queue stats: {e}")
        
        # Fallback: проверяем наличие атрибутов напрямую
        small_size = 0
        large_size = 0
        
        if hasattr(self.s3_uploader, 'small_file_queue'):
            small_size = self.s3_uploader.small_file_queue.qsize()
        if hasattr(self.s3_uploader, 'large_file_queue'):
            large_size = self.s3_uploader.large_file_queue.qsize()
        
        return small_size, large_size, small_size + large_size
                
    async def _check_pending_uploads(self):
        """
        Поиск незагруженных файлов с приоритетом: сначала старые, потом большие.
        Адаптировано для работы с раздельными очередями S3Uploader.
        """
        
        # Получаем размеры очередей
        small_q, large_q, total_q = await self._get_queue_sizes()
        
        # Проверяем общий размер очередей
        if total_q > 500:
            logging.warning(f"⚠️ S3 queues large (small: {small_q}, large: {large_q}, total: {total_q}), skipping recovery")
            return
        
        # Также проверяем количество ожидающих событий
        if hasattr(self.s3_uploader, 'upload_events') and len(self.s3_uploader.upload_events) > 200:
            logging.warning(f"⚠️ Too many pending upload events ({len(self.s3_uploader.upload_events)}), skipping recovery")
            return
            
        async with self.pool.acquire() as conn:
            # Получаем файлы, ожидающие загрузки
            pending = await conn.fetch(f"""
                SELECT id, file_path, s3_key, mime_type, file_size,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) as age_seconds,
                       uploaded, created_at
                FROM media_files 
                WHERE uploaded = FALSE 
                AND created_at < NOW() - INTERVAL '1 hour'
                AND (s3_key IS NOT NULL AND s3_key != '' AND s3_key NOT IN ('FAILED', 'STALE', 'MISSING'))
                ORDER BY created_at ASC, file_size DESC
                LIMIT {self.batch_size}
            """)
            
            # Получаем файлы с пометкой FAILED, старше 24 часов
            failed_old = await conn.fetch(f"""
                SELECT id, file_path, s3_key, mime_type, file_size,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) as age_seconds
                FROM media_files 
                WHERE uploaded = FALSE 
                AND s3_key = 'FAILED'
                AND created_at < NOW() - INTERVAL '24 hours'
                ORDER BY created_at ASC
                LIMIT {self.batch_size // 2}
            """)
            
        if not pending and not failed_old:
            return
            
        total_files = len(pending) + len(failed_old)
        logging.info(f"🔄 Recovery: found {len(pending)} pending files, {len(failed_old)} failed old files")
        
        # Обрабатываем сначала pending (более перспективные)
        for row in pending:
            await self._process_recovery_file(row)
            
        # Потом пробуем восстановить failed
        for row in failed_old:
            await self._process_recovery_file(row, is_failed_retry=True)
    
    async def _process_recovery_file(self, row, is_failed_retry: bool = False):
        """
        Обработка одного файла для recovery.
        
        Args:
            row: строка из БД с данными файла
            is_failed_retry: True если это повторная попытка для FAILED файла
        """
        file_path = Path(row['file_path'])
        media_id = row['id']
        s3_key = row['s3_key']
        age_hours = row['age_seconds'] / 3600 if row['age_seconds'] else 0
        size_mb = row['file_size'] / (1024 * 1024) if row['file_size'] else 0
        file_size = row['file_size'] or 0
        
        # Проверяем, не обрабатывается ли уже этот файл
        if hasattr(self.s3_uploader, 'upload_events') and media_id in self.s3_uploader.upload_events:
            logging.debug(f"Recovery: media {media_id} already in queue, skipping")
            return
        
        # Проверяем, не в процессе ли загрузки
        if hasattr(self.s3_uploader, 'upload_progress') and media_id in self.s3_uploader.upload_progress:
            logging.debug(f"Recovery: media {media_id} already in progress, skipping")
            return
        
        if file_path.exists():
            # Файл существует - пробуем добавить в очередь
            # Определяем приоритет на основе возраста и размера
            priority = 5  # Базовый приоритет для recovery
            
            if is_failed_retry:
                priority = 8  # Более низкий приоритет для failed файлов
            elif age_hours > 24:
                priority = 3  # Высокий приоритет для очень старых файлов
            elif file_size > 10 * 1024 * 1024:  # > 10MB
                priority = 6  # Низкий приоритет для больших файлов
            else:
                priority = 4  # Средний приоритет для обычных файлов
            
            success = await self.s3_uploader.queue_media(
                media_id=media_id,
                file_path=file_path,
                s3_key=s3_key,
                content_type=row['mime_type'] or 'application/octet-stream',
                priority=priority,
                file_size=file_size  # Передаем размер для умной очереди
            )
            
            if success:
                status = "FAILED-retry" if is_failed_retry else "pending"
                logging.info(f"🔄 Recovery: requeued {status} media {media_id} "
                           f"(age: {age_hours:.1f}h, size: {size_mb:.1f}MB, priority: {priority}, key: {s3_key})")
            else:
                logging.warning(f"⚠️ Recovery: failed to queue media {media_id}")
        else:
            # Файл пропал - отмечаем как MISSING
            logging.error(f"❌ Recovery: file missing {row['file_path']}")
            
            async with self.pool.acquire() as conn:
                # Проверяем, есть ли другая запись с таким же checksum
                # Сначала получаем checksum для этого media_id
                checksum = await conn.fetchval(
                    "SELECT checksum FROM media_files WHERE id = $1",
                    media_id
                )
                
                if checksum:
                    # Ищем альтернативный путь с таким же checksum
                    alternate = await conn.fetchval("""
                        SELECT file_path FROM media_files 
                        WHERE checksum = $1 AND id != $2 AND uploaded = TRUE
                        LIMIT 1
                    """, checksum, media_id)
                    
                    if alternate:
                        # Нашли альтернативный путь (уже загруженный файл)
                        logging.info(f"🔄 Recovery: found alternate path for {media_id}: {alternate}")
                        
                        # Получаем public_url из альтернативной записи
                        alt_media = await conn.fetchrow("""
                            SELECT public_url, s3_key FROM media_files 
                            WHERE id = $1
                        """, alternate)
                        
                        if alt_media:
                            await conn.execute("""
                                UPDATE media_files 
                                SET file_path = $1,
                                    uploaded = TRUE,
                                    uploaded_at = CURRENT_TIMESTAMP,
                                    s3_key = $2,
                                    public_url = $3
                                WHERE id = $4
                            """, alternate, alt_media['s3_key'], alt_media['public_url'], media_id)
                            
                            # Отправляем media_ready для всех связанных сообщений
                            messages = await conn.fetch("""
                                SELECT message_id, chat_id 
                                FROM message_media 
                                WHERE media_id = $1
                            """, media_id)
                            
                            for msg in messages:
                                await conn.execute("""
                                    SELECT pg_notify('media_ready', $1)
                                """, json.dumps({
                                    'v': '3.0',
                                    'type': 'media_ready',
                                    'message_id': msg['message_id'],
                                    'chat_id': msg['chat_id'],
                                    'channel_id': msg['chat_id'],
                                    'media_id': str(media_id),
                                    'media_url': alt_media['public_url'],
                                    'timestamp': datetime.utcnow().isoformat()
                                }))
                            
                            logging.info(f"✅ Recovery: recovered media {media_id} from alternate path")
                            return
                
                # Помечаем как MISSING (безнадежно)
                await conn.execute("""
                    UPDATE media_files 
                    SET uploaded = FALSE,
                        s3_key = 'MISSING',
                        public_url = NULL
                    WHERE id = $1
                """, media_id)
                
                # Отправляем статус failed для связанных сообщений
                messages = await conn.fetch("""
                    SELECT message_id, chat_id 
                    FROM message_media 
                    WHERE media_id = $1
                """, media_id)
                
                for msg in messages:
                    await conn.execute("""
                        SELECT pg_notify('media_status', $1)
                    """, json.dumps({
                        'v': '3.0',
                        'type': 'media_status',
                        'message_id': msg['message_id'],
                        'chat_id': msg['chat_id'],
                        'channel_id': msg['chat_id'],
                        'media_id': str(media_id),
                        'status': 'failed',
                        'progress': 0,
                        'error': 'file_missing',
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                
                logging.warning(f"⚠️ Recovery: marked {media_id} as MISSING (file not found)")
    
    async def get_stats(self) -> dict:
        """Получение статистики recovery."""
        async with self.pool.acquire() as conn:
            # Статистика по незагруженным файлам
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_pending,
                    COUNT(*) FILTER (WHERE s3_key = 'FAILED') as failed_count,
                    COUNT(*) FILTER (WHERE s3_key = 'MISSING') as missing_count,
                    COUNT(*) FILTER (WHERE s3_key = 'STALE') as stale_count,
                    COUNT(*) FILTER (WHERE s3_key IS NULL OR s3_key = '') as no_key_count,
                    COUNT(*) FILTER (WHERE created_at < NOW() - INTERVAL '7 days') as older_than_week,
                    SUM(file_size) FILTER (WHERE uploaded = FALSE) as total_pending_bytes
                FROM media_files 
                WHERE uploaded = FALSE
            """)
            
            # Средний возраст незагруженных файлов
            avg_age = await conn.fetchval("""
                SELECT AVG(EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600)
                FROM media_files 
                WHERE uploaded = FALSE
            """)
            
            # Получаем размеры очередей из S3Uploader
            small_q, large_q, total_q = await self._get_queue_sizes()
            
            return {
                'total_pending': stats['total_pending'] if stats else 0,
                'failed': stats['failed_count'] if stats else 0,
                'missing': stats['missing_count'] if stats else 0,
                'stale': stats['stale_count'] if stats else 0,
                'no_key': stats['no_key_count'] if stats else 0,
                'older_than_week': stats['older_than_week'] if stats else 0,
                'total_pending_gb': (stats['total_pending_bytes'] / (1024**3)) if stats and stats['total_pending_bytes'] else 0,
                'avg_age_hours': round(avg_age, 1) if avg_age else 0,
                'small_queue_size': small_q,
                'large_queue_size': large_q,
                'total_queue_size': total_q,
                'pending_events': len(self.s3_uploader.upload_events) if hasattr(self.s3_uploader, 'upload_events') else 0
            }
    
    async def force_check(self):
        """Принудительная проверка (для ручного запуска)."""
        logging.info("🔄 Manual recovery check triggered")
        await self._check_pending_uploads()
        return await self.get_stats()