# s3_uploader.py - исправленная версия с поддержкой URL загрузки для webpage preview
import asyncio
import logging
import os
import json
import hashlib
import aiohttp
import aiofiles
import gc
import psutil
import xml.etree.ElementTree as ET
import random
import time
import tempfile

from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union
import hmac

class S3Uploader:
    def __init__(self, pool, bucket_name: str, endpoint_url: str, 
                 public_url_base: str,
                 region: str = 'ru1',
                 upload_on_finish: bool = True,
                 max_queue_size: int = 1000,
                 dedup_ttl: int = 300):
        """
        Инициализация S3 загрузчика.
        
        Args:
            pool: пул соединений с БД
            bucket_name: имя S3 бакета
            endpoint_url: URL эндпоинта S3
            public_url_base: базовый URL для публичного доступа
            region: регион S3 (для Beget: ru1)
            upload_on_finish: загружать ли файлы в S3
            max_queue_size: максимальный размер очереди
            dedup_ttl: время жизни для дедупликации
        """
        self.pool = pool
        self.bucket = bucket_name
        self.endpoint = endpoint_url.rstrip('/')
        self.public_url_base = public_url_base.rstrip('/')
        self.region = region
        self.upload_on_finish = upload_on_finish
        
        # Раздельные очереди для маленьких и больших файлов
        self.small_file_queue = asyncio.Queue(maxsize=max_queue_size)
        self.large_file_queue = asyncio.Queue(maxsize=max_queue_size)
        
        # Семафоры для контроля параллельных загрузок
        self.small_upload_semaphore = asyncio.Semaphore(5)   # 5 маленьких файлов одновременно
        self.large_upload_semaphore = asyncio.Semaphore(1)   # 1 большой файл одновременно
        
        self.is_running = False
        self.upload_task = None
        
        # События для отслеживания завершения загрузок
        self.upload_events: Dict[str, asyncio.Event] = {}
        self.upload_start_times: Dict[str, float] = {}  # Время начала загрузки
        
        # Прогресс загрузок
        self.upload_progress: Dict[str, float] = {}
        
        # AWS credentials
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not self.access_key or not self.secret_key:
            logging.error("❌ AWS credentials not found in environment variables")
            raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        
        # Константы
        self.LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB
        self.CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB для чанковой загрузки
        
        # Таймауты
        self.SMALL_FILE_TIMEOUT = 120      # 2 минуты для маленьких файлов
        self.LARGE_FILE_TIMEOUT = 600      # 10 минут для больших файлов
        
        # Защита от flood
        self.last_large_upload_time = 0
        self.large_upload_cooldown = 2     # секунды между большими файлами
        
        # Статистика
        self.stats = {
            'queued': 0,
            'uploaded': 0,
            'failed': 0,
            'in_progress': 0,
            'large_files': 0,
            'memory_pressure_events': 0,
            'files_cleaned_before_upload': 0,
            'recovery_attempts': 0,
            'url_downloads': 0
        }
        
        logging.info(f"S3 Uploader initialized: bucket={bucket_name}, endpoint={endpoint_url}, region={region}")
        
    # ==================== Контроль памяти ====================
    
    def _check_memory_pressure(self) -> bool:
        """Проверка на высокую нагрузку памяти."""
        try:
            mem = psutil.virtual_memory()
            if mem.percent > 85:
                logging.warning(f"⚠️ High memory pressure in S3 uploader: {mem.percent}% used")
                self.stats['memory_pressure_events'] += 1
                return True
            return False
        except Exception:
            return False
    
    async def _emergency_cleanup(self):
        """Экстренная очистка памяти."""
        logging.warning("🔄 Emergency memory cleanup in S3 uploader")
        gc.collect()
        await asyncio.sleep(1)
    
    def _cleanup_upload_event(self, media_id: str):
        """Очистка события загрузки."""
        if media_id in self.upload_events:
            del self.upload_events[media_id]
        if media_id in self.upload_start_times:
            del self.upload_start_times[media_id]
        if media_id in self.upload_progress:
            del self.upload_progress[media_id]
    
    # ==================== Загрузка по URL ====================
    
    async def download_from_url(self, url: str, dest_path: Path, max_retries: int = 3) -> bool:
        """
        Скачивание файла по URL с повторными попытками.
        
        Args:
            url: URL для скачивания
            dest_path: путь для сохранения
            max_retries: максимальное количество попыток
        
        Returns:
            bool: True если успешно
        """
        for attempt in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=60)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            logging.warning(f"HTTP {resp.status} for URL {url[:100]} (attempt {attempt + 1})")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return False
                        
                        # Проверяем Content-Type
                        content_type = resp.headers.get('Content-Type', '')
                        if not content_type.startswith('image/'):
                            logging.warning(f"URL does not point to image: {content_type} for {url[:100]}")
                            # Всё равно пробуем скачать
                        
                        async with aiofiles.open(dest_path, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(8192):
                                await f.write(chunk)
                        
                        # Проверяем, что файл не пустой
                        if dest_path.stat().st_size == 0:
                            logging.warning(f"Downloaded file is empty for URL {url[:100]}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return False
                        
                        self.stats['url_downloads'] += 1
                        logging.debug(f"Successfully downloaded from URL: {url[:100]}...")
                        return True
                        
            except asyncio.TimeoutError:
                logging.warning(f"Timeout downloading from URL {url[:100]} (attempt {attempt + 1})")
            except aiohttp.ClientError as e:
                logging.warning(f"Client error downloading from URL: {e} (attempt {attempt + 1})")
            except Exception as e:
                logging.error(f"Unexpected error downloading from URL: {e} (attempt {attempt + 1})")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        
        return False
    
    # ==================== Основные методы ====================
        
    async def start(self):
        """Запуск воркера загрузки."""
        self.is_running = True
        self.upload_task = asyncio.create_task(self._upload_worker_improved())
        logging.info("🚀 S3 uploader started (improved with priority queues and URL support)")
        
    async def stop(self):
        """Остановка воркера с ожиданием завершения очереди."""
        self.is_running = False
        if self.upload_task:
            try:
                # Ждем завершения текущих загрузок
                await asyncio.wait_for(self._drain_queues(), timeout=60.0)
                logging.info("S3 upload queues drained")
            except asyncio.TimeoutError:
                logging.warning("S3 upload queues drain timeout")
            except Exception as e:
                logging.error(f"Error draining queues: {e}")
            
            self.upload_task.cancel()
            try:
                await self.upload_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logging.error(f"Error cancelling upload task: {e}")
        
        logging.info(f"📊 S3 uploader stopped. Stats: {self.stats}")
    
    async def _drain_queues(self):
        """Ожидание опустошения очередей."""
        timeout = 60
        start = time.time()
        while (not self.small_file_queue.empty() or not self.large_file_queue.empty()) and (time.time() - start) < timeout:
            await asyncio.sleep(1)
    
    async def queue_media(self, media_id: str, file_path: Union[str, Path], 
                          s3_key: str, content_type: str, 
                          priority: int = 10,
                          file_size: Optional[int] = None,
                          is_url_preview: bool = False,
                          preview_url: Optional[str] = None) -> bool:
        """
        Добавление медиа в очередь на загрузку с разделением по размеру.
        
        Args:
            media_id: UUID медиафайла из БД
            file_path: путь к файлу на диске (или временный файл с URL)
            s3_key: готовый S3 ключ
            content_type: MIME-тип
            priority: приоритет (меньше число = выше приоритет)
            file_size: размер файла (если известен)
            is_url_preview: True если file_path содержит URL для скачивания
            preview_url: оригинальный URL превью (для метаданных)
        
        Returns:
            bool: True если добавлено в очередь
        """
        if not self.upload_on_finish:
            return False
        
        try:
            if isinstance(file_path, str):
                file_path = Path(file_path)
            
            # Для URL-задач файл может не существовать (будет создан позже)
            if not is_url_preview and not file_path.exists():
                logging.error(f"File not found: {file_path}")
                return False
            
            if file_size is None and not is_url_preview:
                file_size = file_path.stat().st_size
            elif file_size is None:
                file_size = 0
            
            size_mb = file_size / (1024 * 1024) if file_size else 0
            is_large = file_size >= self.LARGE_FILE_THRESHOLD if file_size else False
            
            # Создаем событие для отслеживания
            event = asyncio.Event()
            self.upload_events[media_id] = event
            self.upload_start_times[media_id] = time.time()
            
            task = {
                'media_id': media_id,
                'file_path': str(file_path),
                's3_key': s3_key,
                'content_type': content_type,
                'attempts': 0,
                'size_mb': round(size_mb, 2),
                'file_size': file_size,
                'is_large': is_large,
                'event': event,
                'priority': priority,
                'created_at': datetime.utcnow(),
                'queued_at': time.time(),
                'is_url_preview': is_url_preview,
                'preview_url': preview_url
            }
            
            # Разделяем по очередям
            if is_large:
                await self.large_file_queue.put(task)
                self.stats['large_files'] += 1
                logging.info(f"📦 Queued LARGE file {s3_key} ({size_mb:.2f} MB) - low priority")
            else:
                await self.small_file_queue.put(task)
                if is_url_preview:
                    logging.info(f"📦 Queued URL preview for {s3_key} - high priority")
                else:
                    logging.info(f"📦 Queued small file {s3_key} ({size_mb:.2f} MB) - high priority")
            
            self.stats['queued'] += 1
            return True
            
        except Exception as e:
            logging.error(f"❌ Error queueing media {media_id}: {e}")
            return False
    
    async def wait_for_upload(self, media_id: str, timeout: float = None) -> bool:
        """
        Ожидание завершения загрузки медиа в S3.
        Таймаут начинается с момента вызова этого метода, а не с момента постановки в очередь.
        
        Args:
            media_id: UUID медиафайла
            timeout: таймаут в секундах (None = использовать значения по умолчанию)
        
        Returns:
            bool: True если загрузка завершена успешно
        """
        # Быстрая проверка в БД
        async with self.pool.acquire() as conn:
            uploaded = await conn.fetchval(
                "SELECT uploaded FROM media_files WHERE id = $1",
                media_id
            )
            if uploaded:
                self._cleanup_upload_event(media_id)
                return True
        
        if media_id not in self.upload_events:
            return False
        
        # Определяем таймаут
        if timeout is None:
            async with self.pool.acquire() as conn:
                file_size = await conn.fetchval(
                    "SELECT file_size FROM media_files WHERE id = $1",
                    media_id
                )
            if file_size and file_size >= self.LARGE_FILE_THRESHOLD:
                timeout = self.LARGE_FILE_TIMEOUT
            else:
                timeout = self.SMALL_FILE_TIMEOUT
        
        try:
            logging.debug(f"Waiting for S3 upload of media {media_id} (timeout={timeout}s)")
            await asyncio.wait_for(self.upload_events[media_id].wait(), timeout)
            
            async with self.pool.acquire() as conn:
                uploaded = await conn.fetchval(
                    "SELECT uploaded FROM media_files WHERE id = $1",
                    media_id
                )
            
            self._cleanup_upload_event(media_id)
            return uploaded or False
            
        except asyncio.TimeoutError:
            async with self.pool.acquire() as conn:
                uploaded = await conn.fetchval(
                    "SELECT uploaded FROM media_files WHERE id = $1",
                    media_id
                )
            
            if uploaded:
                logging.info(f"Media {media_id} was uploaded despite timeout")
                self._cleanup_upload_event(media_id)
                return True
            
            logging.warning(f"⏰ Timeout waiting for media {media_id} upload")
            return False
        except asyncio.CancelledError:
            logging.debug(f"Wait for upload of media {media_id} cancelled")
            raise
        except Exception as e:
            logging.error(f"❌ Error waiting for media {media_id}: {e}")
            return False
    
    # ==================== Улучшенный воркер ====================
    
    async def _upload_worker_improved(self):
        """
        Улучшенный воркер с приоритетом маленьких файлов и паузами между большими.
        """
        while self.is_running:
            try:
                if self._check_memory_pressure():
                    await self._emergency_cleanup()
                
                # Приоритет 1: маленькие файлы (всегда в первую очередь)
                if not self.small_file_queue.empty():
                    task = await self.small_file_queue.get()
                    
                    wait_time = time.time() - task.get('queued_at', time.time())
                    if wait_time > 30:
                        logging.info(f"File waited {wait_time:.0f}s, processing now")
                    
                    async with self.small_upload_semaphore:
                        await self._upload_with_progress(task)
                    self.small_file_queue.task_done()
                    continue
                
                # Приоритет 2: большие файлы с паузой между ними
                if not self.large_file_queue.empty():
                    now = time.time()
                    if now - self.last_large_upload_time >= self.large_upload_cooldown:
                        task = await self.large_file_queue.get()
                        async with self.large_upload_semaphore:
                            await self._upload_large_file_chunked(task)
                        self.large_file_queue.task_done()
                        self.last_large_upload_time = now
                    else:
                        await asyncio.sleep(0.5)
                    continue
                
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                logging.debug("Upload worker cancelled")
                break
            except Exception as e:
                logging.error(f"❌ Worker error: {e}")
                await asyncio.sleep(1)
    
    async def _upload_with_progress(self, task: dict):
        """
        Загрузка маленького файла с правильным управлением временными файлами.
        Поддерживает URL-задачи (preview).
        """
        self.stats['in_progress'] += 1
        file_path = Path(task['file_path'])
        actual_file_path = file_path
        is_url_task = task.get('is_url_preview', False)
        
        try:
            # Для URL-задач: скачиваем файл по URL
            if is_url_task:
                # Читаем URL из временного файла
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        url = await f.read()
                except Exception as e:
                    logging.error(f"Failed to read URL from file: {e}")
                    await self._handle_failed_upload(task, 0, reason="url_file_read_failed")
                    return
                
                # Создаем временный файл для скачанного изображения
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
                    actual_file_path = Path(tmp_file.name)
                
                logging.info(f"🌐 Downloading from URL for task {task['media_id']}: {url[:100]}...")
                
                success = await self.download_from_url(url, actual_file_path)
                
                if not success:
                    await self._handle_failed_upload(task, 0, reason="url_download_failed")
                    # НЕ УДАЛЯЕМ ФАЙЛЫ - ОС почистит
                    return
                
                # Обновляем размер файла
                file_size = actual_file_path.stat().st_size
                task['file_size'] = file_size
                task['size_mb'] = file_size / (1024 * 1024)
                task['file_path'] = str(actual_file_path)
                
                # Вычисляем хеш для дедупликации
                sha256 = hashlib.sha256()
                async with aiofiles.open(actual_file_path, 'rb') as f:
                    while True:
                        chunk = await f.read(8192)
                        if not chunk:
                            break
                        sha256.update(chunk)
                file_hash = sha256.hexdigest()
                
                # Генерируем новый s3_key на основе хеша
                media_type = task.get('media_type', 'webpage_preview')
                extension = '.jpg'
                new_s3_key = self.generate_s3_key(media_type, file_hash, extension)
                task['s3_key'] = new_s3_key
                task['file_hash'] = file_hash
                
                # Проверяем, не загружен ли уже файл с таким хешем
                async with self.pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id, uploaded, public_url FROM media_files WHERE checksum = $1",
                        file_hash
                    )
                    
                    if existing:
                        logging.info(f"URL preview already exists with checksum {file_hash[:8]}...")
                        await self._mark_uploaded(
                            task['media_id'],
                            new_s3_key,
                            f"{self.public_url_base}/{new_s3_key}"
                        )
                        if 'event' in task and task['event']:
                            task['event'].set()
                        self.stats['uploaded'] += 1
                        # НЕ УДАЛЯЕМ ФАЙЛЫ - ОС почистит
                        return
                
                logging.info(f"📸 Downloaded URL preview for {task['media_id']}: {file_size} bytes")
                
                # НЕ УДАЛЯЕМ ВРЕМЕННЫЙ URL-ФАЙЛ - ОС почистит
            
            # ПРОВЕРКА: ждем, пока файл появится (если его нет)
            if not actual_file_path.exists():
                logging.warning(f"File not found before upload, waiting: {actual_file_path}")
                for wait_attempt in range(10):
                    await asyncio.sleep(0.5)
                    if actual_file_path.exists():
                        logging.info(f"File appeared after {wait_attempt + 1} attempts")
                        break
                
                if not actual_file_path.exists():
                    logging.error(f"File still missing before upload: {actual_file_path}")
                    await self._handle_failed_upload(task, 0, reason="file_missing_permanent")
                    return
            
            # Загружаем в S3
            success = await self._upload_to_beget(task, actual_file_path)
            
            if success:
                await self._mark_uploaded(
                    task['media_id'],
                    task['s3_key'],
                    f"{self.public_url_base}/{task['s3_key']}"
                )
                
                # НЕ УДАЛЯЕМ ФАЙЛ - ОС почистит
                
                if 'event' in task and task['event']:
                    task['event'].set()
                
                self.stats['uploaded'] += 1
                logging.info(f"✅ Uploaded {task['s3_key']} ({task['size_mb']:.2f} MB)")
            else:
                await self._handle_failed_upload(task, task.get('attempts', 0), reason="upload_failed")
                
        except asyncio.CancelledError:
            logging.debug(f"Upload with progress for {task.get('s3_key', 'unknown')} cancelled")
            raise
        except Exception as e:
            logging.error(f"Upload error: {e}")
            await self._handle_failed_upload(task, task.get('attempts', 0), reason=str(e))
        finally:
            self.stats['in_progress'] -= 1
    
    async def _upload_large_file_chunked(self, task: dict):
        """
        Чанковая загрузка больших файлов с правильным управлением временными файлами.
        """
        self.stats['in_progress'] += 1
        file_path = Path(task['file_path'])
        file_size = task['file_size']
        chunk_size = self.CHUNK_SIZE
        upload_id = None
        
        # ПРОВЕРКА: ждем, пока файл появится
        if not file_path.exists():
            logging.warning(f"File not found before chunked upload, waiting: {file_path}")
            for wait_attempt in range(10):
                await asyncio.sleep(0.5)
                if file_path.exists():
                    logging.info(f"File appeared after {wait_attempt + 1} attempts")
                    break
            
            if not file_path.exists():
                logging.error(f"File still missing before chunked upload: {file_path}")
                await self._handle_failed_upload(task, 0, reason="file_missing")
                self.stats['in_progress'] -= 1
                return
        
        logging.info(f"Starting chunked upload for {task['s3_key']} ({task['size_mb']:.2f} MB)")
        
        try:
            upload_id = await self._initiate_multipart_upload(task['s3_key'])
            parts = []
            part_number = 1
            uploaded = 0
            
            async with aiofiles.open(file_path, 'rb') as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    
                    etag = await self._upload_part(upload_id, part_number, chunk, task['s3_key'])
                    parts.append({'ETag': etag, 'PartNumber': part_number})
                    
                    uploaded += len(chunk)
                    progress = (uploaded / file_size) * 100
                    self.upload_progress[task['media_id']] = progress
                    
                    if part_number % 5 == 0:
                        logging.info(f"Upload progress {task['s3_key']}: {progress:.1f}%")
                    
                    part_number += 1
                    
                    if part_number % 10 == 0 and self._check_memory_pressure():
                        gc.collect()
                    
                    await asyncio.sleep(0)
            
            await self._complete_multipart_upload(upload_id, parts, task['s3_key'])
            
            await self._mark_uploaded(
                task['media_id'],
                task['s3_key'],
                f"{self.public_url_base}/{task['s3_key']}"
            )
            
            # НЕ УДАЛЯЕМ ФАЙЛ - ОС почистит
            
            if 'event' in task and task['event']:
                task['event'].set()
            
            self.stats['uploaded'] += 1
            logging.info(f"✅ Completed chunked upload for {task['s3_key']}")
            
        except asyncio.CancelledError:
            logging.debug(f"Chunked upload for {task['s3_key']} cancelled")
            if upload_id:
                await self._abort_multipart_upload(upload_id, task['s3_key'])
            raise
        except Exception as e:
            logging.error(f"Chunked upload failed for {task['s3_key']}: {e}")
            if upload_id:
                await self._abort_multipart_upload(upload_id, task['s3_key'])
            await self._handle_failed_upload(task, task.get('attempts', 0), reason=str(e))
        finally:
            if task['media_id'] in self.upload_progress:
                del self.upload_progress[task['media_id']]
            self.stats['in_progress'] -= 1
    
    # ==================== Мультипарт загрузка ====================
    
    async def _initiate_multipart_upload(self, s3_key: str) -> str:
        """Инициация мультипарт загрузки с правильной подписью."""
        url = f"{self.endpoint}/{self.bucket}/{s3_key}?uploads"
        
        date = datetime.utcnow()
        amz_date = date.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = date.strftime('%Y%m%d')
        
        host = self.endpoint.replace('https://', '').replace('http://', '')
        payload_hash = hashlib.sha256(b'').hexdigest()
        
        canonical_uri = f'/{self.bucket}/{s3_key}'
        canonical_querystring = 'uploads='
        canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        canonical_request = f"POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        
        signing_key = self._get_signing_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        
        authorization_header = f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        
        headers = {
            'Host': host,
            'x-amz-date': amz_date,
            'x-amz-content-sha256': payload_hash,
            'Authorization': authorization_header
        }
        
        logging.debug(f"Initiating multipart upload for {s3_key}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        root = ET.fromstring(text)
                        upload_id_elem = root.find('.//{*}UploadId')
                        if upload_id_elem is not None:
                            upload_id = upload_id_elem.text
                            logging.debug(f"Got upload_id: {upload_id}")
                            return upload_id
                        for elem in root.iter():
                            if 'UploadId' in elem.tag:
                                return elem.text
                        raise Exception("UploadId not found in response")
                    else:
                        text = await resp.text()
                        logging.error(f"Initiate multipart upload failed: {resp.status} - {text[:200]}")
                        raise Exception(f"Failed to initiate multipart upload: {resp.status}")
        except asyncio.TimeoutError:
            logging.error(f"Timeout initiating multipart upload for {s3_key}")
            raise
        except aiohttp.ClientError as e:
            logging.error(f"Client error initiating multipart upload for {s3_key}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error initiating multipart upload for {s3_key}: {e}")
            raise
    
    async def _upload_part(self, upload_id: str, part_number: int, chunk: bytes, s3_key: str) -> str:
        """Загрузка одного чанка с правильной подписью."""
        url = f"{self.endpoint}/{self.bucket}/{s3_key}?partNumber={part_number}&uploadId={upload_id}"
        
        date = datetime.utcnow()
        amz_date = date.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = date.strftime('%Y%m%d')
        
        host = self.endpoint.replace('https://', '').replace('http://', '')
        payload_hash = hashlib.sha256(chunk).hexdigest()
        
        canonical_uri = f'/{self.bucket}/{s3_key}'
        canonical_querystring = f'partNumber={part_number}&uploadId={upload_id}'
        canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        canonical_request = f"PUT\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        
        signing_key = self._get_signing_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        
        authorization_header = f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        
        headers = {
            'Host': host,
            'Content-Length': str(len(chunk)),
            'x-amz-date': amz_date,
            'x-amz-content-sha256': payload_hash,
            'Authorization': authorization_header
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=headers, data=chunk) as resp:
                    if resp.status in [200, 201]:
                        etag = resp.headers.get('ETag', '').strip('"')
                        logging.debug(f"Uploaded part {part_number}, ETag: {etag}")
                        return etag
                    else:
                        text = await resp.text()
                        logging.error(f"Upload part {part_number} failed: {resp.status} - {text[:200]}")
                        raise Exception(f"Failed to upload part {part_number}: {resp.status}")
        except asyncio.TimeoutError:
            logging.error(f"Timeout uploading part {part_number} for {s3_key}")
            raise
        except aiohttp.ClientError as e:
            logging.error(f"Client error uploading part {part_number} for {s3_key}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error uploading part {part_number} for {s3_key}: {e}")
            raise
    
    async def _complete_multipart_upload(self, upload_id: str, parts: list, s3_key: str):
        """Завершение мультипарт загрузки с правильной подписью."""
        url = f"{self.endpoint}/{self.bucket}/{s3_key}?uploadId={upload_id}"
        
        date = datetime.utcnow()
        amz_date = date.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = date.strftime('%Y%m%d')
        
        host = self.endpoint.replace('https://', '').replace('http://', '')
        
        xml_parts = []
        for part in sorted(parts, key=lambda x: x['PartNumber']):
            xml_parts.append(f'<Part><PartNumber>{part["PartNumber"]}</PartNumber><ETag>"{part["ETag"]}"</ETag></Part>')
        
        xml_body = f'<CompleteMultipartUpload>{"".join(xml_parts)}</CompleteMultipartUpload>'
        
        payload_hash = hashlib.sha256(xml_body.encode()).hexdigest()
        
        canonical_uri = f'/{self.bucket}/{s3_key}'
        canonical_querystring = f'uploadId={upload_id}'
        canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        canonical_request = f"POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        
        signing_key = self._get_signing_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        
        authorization_header = f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        
        headers = {
            'Host': host,
            'Content-Type': 'application/xml',
            'Content-Length': str(len(xml_body)),
            'x-amz-date': amz_date,
            'x-amz-content-sha256': payload_hash,
            'Authorization': authorization_header
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=xml_body) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logging.error(f"Complete multipart upload failed: {resp.status} - {text[:200]}")
                        raise Exception(f"Failed to complete multipart upload: {resp.status}")
        except asyncio.TimeoutError:
            logging.error(f"Timeout completing multipart upload for {s3_key}")
            raise
        except aiohttp.ClientError as e:
            logging.error(f"Client error completing multipart upload for {s3_key}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error completing multipart upload for {s3_key}: {e}")
            raise
    
    async def _abort_multipart_upload(self, upload_id: str, s3_key: str):
        """Отмена мультипарт загрузки при ошибке."""
        try:
            url = f"{self.endpoint}/{self.bucket}/{s3_key}?uploadId={upload_id}"
            async with aiohttp.ClientSession() as session:
                await session.delete(url)
        except Exception as e:
            logging.error(f"Failed to abort multipart upload: {e}")
    
    # ==================== Вспомогательные методы ====================
    
    def _get_signing_key(self, date_stamp: str) -> bytes:
        """Получение ключа для подписи."""
        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
        
        k_date = sign(('AWS4' + self.secret_key).encode('utf-8'), date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, 's3')
        return sign(k_service, 'aws4_request')
    
    def generate_s3_key(self, media_type: str, file_hash: str, extension: str) -> str:
        """Генерация S3 ключа на основе типа и хеша."""
        s3_type_map = {
            'photos': 'images',
            'videos': 'video',
            'audio': 'audio',
            'documents': 'docs',
            'webpage_preview': 'previews',
            'other': 'other'
        }
        s3_type = s3_type_map.get(media_type, 'other')
        ext_no_dot = extension.lstrip('.')
        return f"media/{s3_type}/{file_hash[:2]}/{file_hash}.{ext_no_dot}"
    
    async def _upload_to_beget(self, task: dict, file_path: Path = None) -> bool:
        """
        Прямая загрузка в Beget через HTTP запрос с AWS4 подписью.
        """
        try:
            if file_path is None:
                file_path = Path(task['file_path'])
            
            if not file_path.exists():
                logging.error(f"File not found: {file_path}")
                return False
            
            async with aiofiles.open(file_path, 'rb') as f:
                file_data = await f.read()
            
            upload_url = f"{self.endpoint}/{self.bucket}/{task['s3_key']}"
            
            date = datetime.utcnow()
            amz_date = date.strftime('%Y%m%dT%H%M%SZ')
            date_stamp = date.strftime('%Y%m%d')
            
            payload_hash = hashlib.sha256(file_data).hexdigest()
            host = self.endpoint.replace('https://', '').replace('http://', '')
            
            headers = {
                'Host': host,
                'Content-Type': task['content_type'],
                'Content-Length': str(len(file_data)),
                'x-amz-date': amz_date,
                'x-amz-content-sha256': payload_hash,
                'x-amz-acl': 'public-read'
            }
            
            canonical_uri = f'/{self.bucket}/{task["s3_key"]}'
            canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
            signed_headers = 'host;x-amz-content-sha256;x-amz-date'
            
            canonical_request = f"PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
            
            algorithm = 'AWS4-HMAC-SHA256'
            credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
            string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
            
            signing_key = self._get_signing_key(date_stamp)
            signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
            
            authorization_header = f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
            headers['Authorization'] = authorization_header
            
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.put(upload_url, headers=headers, data=file_data) as resp:
                    if resp.status in [200, 201, 204]:
                        return True
                    else:
                        text = await resp.text()
                        logging.error(f"S3 upload failed with status {resp.status}: {text[:200]}")
                        return False
                        
        except asyncio.TimeoutError:
            logging.error(f"S3 upload timeout for {task['s3_key']}")
            return False
        except asyncio.CancelledError:
            logging.debug(f"S3 upload for {task['s3_key']} cancelled")
            raise
        except aiohttp.ClientError as e:
            logging.error(f"S3 client error: {e}")
            return False
        except Exception as e:
            logging.error(f"S3 upload error: {e}")
            return False
    
    async def _handle_failed_upload(self, task: dict, current_attempt: int, reason: str = None):
        """
        Обработка неудачной загрузки с повторными попытками.
        """
        max_attempts = 3
        attempt = task.get('attempts', 0) + 1
        task['attempts'] = attempt
        
        if attempt < max_attempts:
            wait_time = 5 * attempt + random.uniform(0, 2)
            logging.warning(f"🔄 Retry {attempt}/{max_attempts} for {task.get('s3_key', 'unknown')} in {wait_time:.0f}s (reason: {reason})")
            
            await asyncio.sleep(wait_time)
            
            file_path = Path(task['file_path'])
            if not file_path.exists() and not task.get('is_url_preview', False):
                # Проверяем еще раз через небольшую задержку
                await asyncio.sleep(1)
                if not file_path.exists():
                    logging.error(f"File missing during retry (confirmed): {file_path}")
                    await self._mark_missing(task['media_id'], reason="file_missing_during_retry")
                    if 'event' in task and task['event']:
                        task['event'].set()
                    return
                else:
                    logging.info(f"File appeared after wait: {file_path}")
            
            # Возвращаем в очередь
            if task.get('is_large', False):
                await self.large_file_queue.put(task)
            else:
                await self.small_file_queue.put(task)
        else:
            logging.error(f"❌ Failed {task.get('s3_key', 'unknown')} after {max_attempts} attempts (reason: {reason})")
            
            if 'event' in task and task['event']:
                task['event'].set()
            
            self.stats['failed'] += 1
            await self._mark_failed(task['media_id'], reason=reason)
            
            # НЕ УДАЛЯЕМ ФАЙЛ - ОС почистит
    
    async def _mark_missing(self, media_id: str, reason: str = None):
        """Отметка медиа как MISSING (файл пропал)."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE media_files 
                    SET uploaded = FALSE,
                        s3_key = 'MISSING',
                        public_url = NULL
                    WHERE id = $1
                """, media_id)
                
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
                        'status': 'missing',
                        'error': reason or 'file_not_found',
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                    
                logging.warning(f"⚠️ Marked media {media_id} as MISSING (reason: {reason})")
        except Exception as e:
            logging.error(f"Failed to mark missing: {e}")
    
    async def _mark_uploaded(self, media_id: str, s3_key: str, public_url: str):
        """Отметка медиа как загруженного."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE media_files 
                    SET uploaded = TRUE, 
                        uploaded_at = CURRENT_TIMESTAMP,
                        s3_key = $1,
                        public_url = $2
                    WHERE id = $3
                """, s3_key, public_url, media_id)
                
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
                        'media_url': public_url,
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                    
                    await conn.execute("""
                        SELECT pg_notify('media_status', $1)
                    """, json.dumps({
                        'v': '3.0',
                        'type': 'media_status',
                        'message_id': msg['message_id'],
                        'chat_id': msg['chat_id'],
                        'channel_id': msg['chat_id'],
                        'media_id': str(media_id),
                        'status': 'ready',
                        'progress': 100,
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                    
                logging.info(f"✅ Marked media {media_id} as uploaded, linked to {len(messages)} messages")
                        
        except Exception as e:
            logging.error(f"Failed to mark uploaded in DB: {e}")
    
    async def _mark_failed(self, media_id: str, reason: str = None):
        """Отметка медиа как неудачно загруженного."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE media_files 
                    SET uploaded = FALSE,
                        s3_key = 'FAILED'
                    WHERE id = $1
                """, media_id)
                
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
                        'error': reason,
                        'progress': 0,
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                    
                logging.warning(f"⚠️ Marked media {media_id} as FAILED (reason: {reason})")
        except Exception as e:
            logging.error(f"Failed to mark failed in DB: {e}")
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Получение статистики очереди."""
        return {
            'small_queue_size': self.small_file_queue.qsize(),
            'large_queue_size': self.large_file_queue.qsize(),
            'pending_events': len(self.upload_events),
            **self.stats
        }
    
    async def clear_failed_events(self):
        """Очистка зависших событий."""
        now = time.time()
        expired = []
        
        for media_id, start_time in self.upload_start_times.items():
            age = now - start_time
            if age > 3600:
                expired.append(media_id)
        
        for media_id in expired:
            self._cleanup_upload_event(media_id)
            logging.warning(f"Cleared expired event for media {media_id}")
        
        return len(expired)