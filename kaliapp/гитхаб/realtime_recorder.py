# realtime_recorder.py - исправленная версия
import asyncio
import asyncpg
import logging
import time
import uuid
import subprocess
import sys
import json
import hashlib
import mimetypes
import os
import tempfile
import aiofiles
import psutil
import gc
import signal
import random
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Set, Any, List
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import (
    DocumentAttributeFilename, DocumentAttributeVideo, MessageMediaPhoto,
    MessageMediaDocument, MessageMediaGeo, MessageMediaContact, MessageMediaPoll,
    MessageMediaWebPage,
    MessageActionChatAddUser, MessageActionChatDeleteUser,
    MessageActionChatEditTitle, MessageActionChatEditPhoto, MessageActionPinMessage,
    DocumentAttributeSticker
)
from colorama import Fore, Style
from questionary import select, text
from db_module import (
    add_message_to_db, add_user_to_db, ensure_partition_for_date,
    normalize_chat_id, get_chat_list, print_message, compute_text_fingerprint,
    extract_entities, extract_keywords, add_messages_batch, import_messages_to_db
)
from s3_uploader import S3Uploader

logging.getLogger('telethon').setLevel(logging.INFO)


class RealtimeRecorder:
    
    NOTIFY_VERSION = '3.0'
    
    def __init__(self, client: TelegramClient, pool, s3_uploader: Optional[S3Uploader] = None):
        self.client = client
        self.pool = pool
        self.s3_uploader = s3_uploader
        self.is_recording = False
        self.recorded_chats = set()
        self.new_handler = None
        self.delete_handler = None
        self.edit_handler = None
        self.chat_types = {}
        
        # Множество для отслеживания обрабатываемых медиа
        self._processing_media = set()
        
        # Семафоры для контроля параллелизма
        self.import_semaphore = asyncio.Semaphore(2)
        self.media_semaphore = asyncio.Semaphore(2)
        self.process_semaphore = asyncio.Semaphore(5)
        
        # Контроль памяти
        self.memory_limit_mb = 512
        self.download_progress = {}
        
        # Кэши и состояние
        self.last_sync = 0
        self.last_checked_message = {}
        self.last_deleted_check = {}
        self.polling_task = None
        self.message_cache = {}
        self.user_cache = {}
        self.chat_cache = {}
        
        # Медиа пути
        self.base_media_path = Path('/usr/local/kalinode/private/media')
        self.base_media_path.mkdir(parents=True, exist_ok=True)
        
        # Настройки S3
        self.upload_on_finish = True
        self.upload_immediately = True
        
        # Таймауты
        self.S3_UPLOAD_TIMEOUT_SMALL = 120
        self.S3_UPLOAD_TIMEOUT_LARGE = 600
        self.LARGE_FILE_THRESHOLD = 100 * 1024 * 1024
        
        # Статистика
        self.stats = {
            'messages_processed': 0,
            'media_downloaded': 0,
            'media_uploaded_s3': 0,
            'errors': 0,
            'large_files_uploaded': 0,
            'notifications_sent': 0,
            'memory_pressure_events': 0,
            'flood_wait_events': 0,
            'api_calls_saved': 0,
            'webpage_previews': 0
        }

    # ==================== Контроль памяти ====================
    
    def _check_memory_pressure(self) -> bool:
        try:
            mem = psutil.virtual_memory()
            if mem.percent > 85:
                logging.warning(f"High memory pressure detected: {mem.percent}% used")
                self.stats['memory_pressure_events'] += 1
                return True
            return False
        except Exception:
            return False

    async def _emergency_cleanup(self):
        logging.warning("Emergency memory cleanup triggered")
        gc.collect()
        await asyncio.sleep(1)

    # ==================== Вспомогательные методы ====================
    
    def get_extension_from_mime(self, mime_type):
        if not mime_type:
            return None
        mime_map = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'video/mp4': '.mp4',
            'video/webm': '.webm',
            'video/quicktime': '.mov',
            'audio/mpeg': '.mp3',
            'audio/ogg': '.ogg',
            'audio/opus': '.opus',
            'application/pdf': '.pdf',
            'application/msword': '.doc',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
            'text/plain': '.txt'
        }
        return mime_map.get(mime_type.lower(), None)

    def get_media_type_from_message(self, message):
        """Определение типа медиа, включая WebPage preview"""
        if not message.media:
            return None
        
        # Проверка на MessageMediaWebPage
        if isinstance(message.media, MessageMediaWebPage):
            # Проверяем, есть ли фото-превью в WebPage
            if hasattr(message.media, 'webpage') and message.media.webpage:
                webpage = message.media.webpage
                if hasattr(webpage, 'photo') and webpage.photo:
                    return 'webpage_preview'
            return 'webpage'
        
        if message.photo:
            return 'photos'
        
        if hasattr(message.media, 'document'):
            doc = message.media.document
            mime_type = getattr(doc, 'mime_type', '').lower()
            
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeSticker):
                    return 'stickers'
            
            if mime_type.startswith('video/'):
                return 'videos'
            elif mime_type.startswith('audio/'):
                return 'audio'
            elif mime_type.startswith('image/'):
                return 'photos'
            else:
                return 'documents'
        
        if isinstance(message.media, MessageMediaPhoto):
            return 'photos'
        
        if isinstance(message.media, MessageMediaDocument):
            return 'documents'
        
        return 'other'

    async def extract_webpage_preview(self, message, chat_id) -> Optional[Dict[str, Any]]:
        """
        Извлечение URL превью из WebPage и передача в S3Uploader.
        S3Uploader сам скачает файл по URL.
        """
        try:
            if not isinstance(message.media, MessageMediaWebPage):
                return None
            
            webpage = message.media.webpage
            if not webpage or not hasattr(webpage, 'photo') or not webpage.photo:
                logging.debug(f"No photo preview in webpage for message {message.id}")
                return None
            
            photo = webpage.photo
            
            # Проверяем, есть ли у фото размеры для скачивания
            if not hasattr(photo, 'sizes') or not photo.sizes:
                logging.debug(f"No downloadable sizes for webpage preview in message {message.id}")
                return None
            
            # Берем самый большой размер для лучшего качества
            max_size = None
            max_dimension = 0
            for size in photo.sizes:
                w = getattr(size, 'w', 0)
                h = getattr(size, 'h', 0)
                dimension = w * h
                if dimension > max_dimension:
                    max_dimension = dimension
                    max_size = size
            
            if not max_size:
                max_size = photo.sizes[-1]
            
            # Получаем URL превью
            preview_url = getattr(max_size, 'url', None)
            if not preview_url:
                logging.debug(f"No URL in photo size for message {message.id}")
                return None
            
            logging.info(f"📸 Got webpage preview URL for message {message.id}: {preview_url[:80]}...")
            
            # Генерируем хеш на основе URL для дедупликации
            url_hash = hashlib.sha256(preview_url.encode()).hexdigest()[:32]
            extension = '.jpg'
            media_type = 'webpage_preview'
            s3_key = f"media/previews/{url_hash[:2]}/{url_hash}.jpg"
            
            # Создаем временный файл с URL для S3Uploader
            with tempfile.NamedTemporaryFile(delete=False, suffix='.url') as tmp_file:
                tmp_path = Path(tmp_file.name)
                async with aiofiles.open(tmp_path, 'w') as f:
                    await f.write(preview_url)
            
            media_info = {
                'temp_path': tmp_path,
                'file_hash': url_hash,
                'extension': extension,
                'media_type': media_type,
                's3_key': s3_key,
                'file_size': 0,  # Будет обновлено после скачивания
                'metadata': {
                    'url': getattr(webpage, 'url', ''),
                    'title': getattr(webpage, 'title', ''),
                    'description': getattr(webpage, 'description', ''),
                    'site_name': getattr(webpage, 'site_name', ''),
                    'preview_url': preview_url,
                    'width': getattr(max_size, 'w', 0),
                    'height': getattr(max_size, 'h', 0)
                },
                'content_type': 'image/jpeg',
                'original_file_name': f"preview_{message.id}.jpg",
                'is_url_preview': True  # Флаг для S3Uploader
            }
            
            return media_info
            
        except Exception as e:
            logging.error(f"Error extracting webpage preview for message {message.id}: {e}")
            import traceback
            traceback.print_exc()
        
        return None

    def detect_extension(self, file_path: Path) -> str:
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            ext = self.get_extension_from_mime(mime_type)
            if ext:
                return ext
        
        try:
            result = subprocess.run(
                ['file', '--mime-type', '-b', str(file_path)],
                capture_output=True, text=True, check=True, timeout=5
            )
            mime_type = result.stdout.strip()
            ext = self.get_extension_from_mime(mime_type)
            if ext:
                return ext
        except:
            pass
        
        if file_path.suffix:
            return file_path.suffix.lower()
        
        return '.bin'

    def generate_s3_key(self, media_type: str, file_hash: str, extension: str) -> str:
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

    def _get_original_filename(self, message) -> Optional[str]:
        if hasattr(message.media, 'document'):
            doc = message.media.document
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    return attr.file_name
        return None

    async def fetch_chats(self, list_type=None):
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT chat_id FROM chats WHERE is_active = TRUE"
                params = []
                if list_type:
                    query += " AND chat_id IN (SELECT chat_id FROM chat_lists WHERE list_type = $1)"
                    params.append(list_type)
                chats = await conn.fetch(query, *params)
                return {normalize_chat_id(row['chat_id']) for row in chats}
        except Exception as e:
            logging.error(f"Error fetching chat list: {e}")
            print_message(f"Error fetching chat list: {e}", level="error")
            return set()

    async def import_missed_messages(self, chat_id, limit=10, with_media=True):
        async with self.import_semaphore:
            chat_list = 'gray'
            if with_media:
                async with self.pool.acquire() as conn:
                    chat_list = await get_chat_list(conn, chat_id)
            
            should_download_media = with_media and (chat_list == 'white')
            
            logging.debug(f"Starting message import for {chat_id} with limit {limit}, media={should_download_media}")
            
            count = await import_messages_to_db(
                self.client,
                chat_id,
                limit,
                self.pool,
                import_media=should_download_media,
                import_reactions=False,
                s3_uploader=self.s3_uploader
            )
            
            logging.info(f"Imported {count} messages for: {chat_id} (media: {should_download_media})")
            return count

    # ==================== Медиа загрузка ====================
    
    async def download_media_streaming(self, message, chat_id) -> Optional[Dict[str, Any]]:
        async with self.media_semaphore:
            if self._check_memory_pressure():
                await self._emergency_cleanup()
                await asyncio.sleep(2)
            
            media_type = self.get_media_type_from_message(message)
            
            # Разрешаем download для фото/видео (не для webpage_preview)
            if media_type not in ['photos', 'videos']:
                logging.debug(f"Skipping download for {media_type} in message {message.id}")
                return None
            
            file_size = 0
            if hasattr(message.media, 'document') and message.media.document:
                file_size = message.media.document.size
            elif message.photo:
                file_size = sum(getattr(p, 'size', 0) for p in message.photo.sizes) if hasattr(message.photo, 'sizes') else 0
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp_file:
                tmp_path = Path(tmp_file.name)
            
            try:
                logging.info(f"Streaming download for message {message.id} (size: {file_size/(1024*1024):.1f}MB, type: {media_type})")
                
                sha256 = hashlib.sha256()
                downloaded = 0
                last_progress_log = time.time()
                
                async for chunk in self.client.iter_download(message.media):
                    async with aiofiles.open(tmp_path, 'ab') as f:
                        await f.write(chunk)
                    
                    sha256.update(chunk)
                    downloaded += len(chunk)
                    
                    if file_size > 10 * 1024 * 1024:
                        now = time.time()
                        if now - last_progress_log > 5:
                            progress = (downloaded / file_size) * 100 if file_size else 0
                            logging.info(f"Download progress for message {message.id}: {progress:.1f}% ({downloaded/(1024*1024):.1f}/{file_size/(1024*1024):.1f}MB)")
                            last_progress_log = now
                    
                    if downloaded % (10 * 1024 * 1024) == 0:
                        if self._check_memory_pressure():
                            await self._emergency_cleanup()
                
                file_hash = sha256.hexdigest()
                
                if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                    logging.error(f"Download failed for message {message.id}: file empty or missing")
                    return None
                
                extension = self.detect_extension(tmp_path)
                metadata = await self.get_media_metadata_fast(tmp_path, extension)
                s3_key = self.generate_s3_key(media_type, file_hash, extension)
                original_file_name = self._get_original_filename(message)
                
                media_info = {
                    'temp_path': tmp_path,
                    'file_hash': file_hash,
                    'extension': extension,
                    'media_type': media_type,
                    's3_key': s3_key,
                    'file_size': tmp_path.stat().st_size,
                    'metadata': metadata,
                    'content_type': mimetypes.guess_type(str(tmp_path))[0] or 'application/octet-stream',
                    'original_file_name': original_file_name or f"media_{message.id}{extension}"
                }
                
                self.stats['media_downloaded'] += 1
                return media_info
                
            except asyncio.CancelledError:
                logging.debug(f"Download cancelled for message {message.id}")
                raise
            except Exception as e:
                logging.error(f"Error in streaming download for message {message.id}: {e}")
                self.stats['errors'] += 1
                return None

    async def get_media_metadata_fast(self, file_path: Path, file_ext: str) -> Dict[str, Any]:
        metadata = {}
        try:
            if file_ext in ['.mp4', '.webm', '.mov']:
                try:
                    result = subprocess.run(
                        ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                         '-show_streams', '-select_streams', 'v:0', str(file_path)],
                        capture_output=True, text=True, timeout=10
                    )
                    probe_data = json.loads(result.stdout)
                    
                    for stream in probe_data.get('streams', []):
                        if stream.get('codec_type') == 'video':
                            metadata['width'] = int(stream.get('width', 0))
                            metadata['height'] = int(stream.get('height', 0))
                            metadata['duration'] = float(stream.get('duration', 0))
                            metadata['video_codec'] = stream.get('codec_name')
                            break
                except Exception as e:
                    logging.debug(f"Fast ffprobe failed: {e}")
            
            elif file_ext in ['.jpg', '.png', '.gif']:
                try:
                    from PIL import Image
                    with Image.open(file_path) as img:
                        metadata['width'], metadata['height'] = img.size
                except ImportError:
                    try:
                        result = subprocess.run(
                            ['identify', '-format', '%w %h', str(file_path)],
                            capture_output=True, text=True, timeout=5
                        )
                        parts = result.stdout.strip().split()
                        if len(parts) >= 2:
                            metadata['width'] = int(parts[0])
                            metadata['height'] = int(parts[1])
                    except:
                        pass
        
        except Exception as e:
            logging.error(f"Error in fast metadata extraction: {e}")
        
        return metadata

    async def save_media_to_db(self, conn, message_id: int, chat_id: int, media_info: Dict[str, Any]) -> Optional[str]:
        try:
            file_size = media_info['file_size']
            file_type = media_info['extension'].lstrip('.')
            metadata = media_info.get('metadata', {})
            
            track_number = metadata.get('track_number')
            if track_number is not None:
                try:
                    track_number = int(track_number)
                except (ValueError, TypeError):
                    track_number = None
            
            year = metadata.get('year')
            if year is not None:
                try:
                    year = int(year)
                except (ValueError, TypeError):
                    year = None
            
            bitrate = metadata.get('bitrate')
            if bitrate is not None:
                try:
                    bitrate = int(bitrate)
                except (ValueError, TypeError):
                    bitrate = None
            
            fps = metadata.get('fps')
            if fps is not None:
                try:
                    fps = int(fps)
                except (ValueError, TypeError):
                    fps = None

            existing = await conn.fetchrow(
                "SELECT id, uploaded, public_url FROM media_files WHERE checksum = $1",
                media_info['file_hash']
            )
            
            if existing:
                media_id = existing['id']
                logging.info(f"Found existing media with checksum {media_info['file_hash'][:8]}... (id: {media_id})")
                
                try:
                    await conn.execute("""
                        INSERT INTO message_media (message_id, chat_id, media_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (message_id, chat_id, media_id) DO NOTHING
                    """, message_id, chat_id, media_id)
                except Exception as e:
                    logging.warning(f"Error creating message_media link (might already exist): {e}")
                
                await conn.execute("""
                    UPDATE media_files
                    SET media_views = media_views + 1,
                        last_accessed = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, media_id)
                
                if existing['uploaded'] and existing['public_url']:
                    await self.send_media_ready_notification(
                        conn, message_id, chat_id, {
                            'media_id': str(media_id),
                            'public_url': existing['public_url']
                        }
                    )
                    return None
                
                return str(media_id)
            
            uuid_val = str(uuid.uuid4())
            temp_path_str = str(media_info['temp_path'])
            mtime = datetime.fromtimestamp(media_info['temp_path'].stat().st_mtime) if media_info['temp_path'].exists() else datetime.utcnow()
            
            try:
                await conn.execute("""
                    INSERT INTO media_files (
                        id, file_path, mtime, file_type, directory,
                        file_size, file_name, mime_type, width, height, duration,
                        checksum, has_audio, audio_codec, video_codec, bitrate, fps,
                        artist, title, album, track_number, year, genre,
                        s3_key, uploaded, created_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
                        $24, $25, CURRENT_TIMESTAMP
                    )
                """,
                uuid_val,
                temp_path_str,
                mtime,
                file_type,
                'media',
                file_size,
                media_info.get('original_file_name'),
                media_info.get('content_type'),
                metadata.get('width'),
                metadata.get('height'),
                metadata.get('duration'),
                media_info['file_hash'],
                metadata.get('has_audio', False),
                metadata.get('audio_codec'),
                metadata.get('video_codec'),
                bitrate,
                fps,
                metadata.get('artist'),
                metadata.get('title'),
                metadata.get('album'),
                track_number,
                year,
                metadata.get('genre'),
                media_info['s3_key'],
                False
                )
                
            except asyncpg.UniqueViolationError:
                logging.warning(f"Unique violation caught for checksum {media_info['file_hash'][:8]}..., fetching existing")
                
                existing = await conn.fetchrow(
                    "SELECT id, uploaded, public_url FROM media_files WHERE checksum = $1",
                    media_info['file_hash']
                )
                
                if existing:
                    uuid_val = str(existing['id'])
                    
                    try:
                        await conn.execute("""
                            INSERT INTO message_media (message_id, chat_id, media_id)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (message_id, chat_id, media_id) DO NOTHING
                        """, message_id, chat_id, uuid_val)
                    except Exception as e:
                        logging.warning(f"Error creating message_media link for existing media: {e}")
                    
                    await conn.execute("""
                        UPDATE media_files
                        SET media_views = media_views + 1,
                            last_accessed = CURRENT_TIMESTAMP
                        WHERE id = $1
                    """, uuid_val)
                    
                    if existing['uploaded'] and existing['public_url']:
                        await self.send_media_ready_notification(
                            conn, message_id, chat_id, {
                                'media_id': str(uuid_val),
                                'public_url': existing['public_url']
                            }
                        )
                    
                    return str(uuid_val)
                else:
                    raise
            
            try:
                await conn.execute("""
                    INSERT INTO message_media (message_id, chat_id, media_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (message_id, chat_id, media_id) DO NOTHING
                """, message_id, chat_id, uuid_val)
            except Exception as e:
                logging.warning(f"Error creating message_media link for new media: {e}")
            
            logging.info(f"Created new media {uuid_val} and link to message {message_id}")
            return uuid_val
            
        except Exception as e:
            logging.error(f"Error saving media to DB for message {message_id}: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def process_media(self, message, chat_id) -> Optional[Dict[str, Any]]:
        # Проверка на повторную обработку
        media_key = f"{message.id}_{chat_id}"
        if media_key in self._processing_media:
            logging.debug(f"Media for message {message.id} already being processed, skipping")
            return None
        
        self._processing_media.add(media_key)
        
        try:
            media_type = self.get_media_type_from_message(message)
            
            # Обработка WebPage preview (через URL)
            if media_type == 'webpage_preview':
                logging.info(f"Processing webpage preview for message {message.id} in chat {chat_id}")
                media_info = await self.extract_webpage_preview(message, chat_id)
                
                if not media_info:
                    await self.send_media_status_notification(
                        message.id, chat_id, None, 'failed', 0
                    )
                    return None
                
                await self.send_media_status_notification(
                    message.id, chat_id, None, 'processing', 50
                )
                
                async with self.pool.acquire() as conn:
                    media_uuid = await self.save_media_to_db(conn, message.id, chat_id, media_info)
                    
                    if not media_uuid:
                        logging.warning(f"Webpage preview already existed for message {message.id}")
                        return None
                    
                    logging.info(f"Created new webpage preview {media_uuid} for message {message.id}")
                
                await self.send_media_status_notification(
                    message.id, chat_id, media_uuid, 'uploading', 75
                )
                
                if self.s3_uploader and self.upload_on_finish:
                    success = await self.s3_uploader.queue_media(
                        media_id=media_uuid,
                        file_path=media_info['temp_path'],
                        s3_key=media_info['s3_key'],
                        content_type=media_info['content_type'],
                        priority=10,
                        file_size=0,
                        is_url_preview=True,
                        preview_url=media_info['metadata'].get('preview_url')
                    )
                    
                    if success:
                        self.stats['media_uploaded_s3'] += 1
                        self.stats['webpage_previews'] += 1
                        
                        timeout = self.S3_UPLOAD_TIMEOUT_SMALL
                        
                        task = asyncio.create_task(
                            self._wait_for_upload_and_notify(
                                media_uuid, message.id, chat_id, media_info['temp_path'], timeout
                            )
                        )
                        task.add_done_callback(lambda t: self._handle_task_exception(t, "wait_for_upload_and_notify"))
                        
                        return {'media_id': media_uuid, 'status': 'uploading', 'is_preview': True}
                    else:
                        await self.send_media_status_notification(
                            message.id, chat_id, media_uuid, 'failed', 0
                        )
                
                return None
            
            # Обычные фото/видео
            if media_type not in ['photos', 'videos']:
                logging.debug(f"Skipping media processing for {media_type} in message {message.id}")
                return None
            
            logging.info(f"Processing media for message {message.id} in chat {chat_id}")
            
            await self.send_media_status_notification(
                message.id, chat_id, None, 'downloading', 0
            )
            
            media_info = await self.download_media_streaming(message, chat_id)
            if not media_info:
                await self.send_media_status_notification(
                    message.id, chat_id, None, 'failed', 0
                )
                return None
            
            await self.send_media_status_notification(
                message.id, chat_id, None, 'processing', 50
            )
            
            async with self.pool.acquire() as conn:
                media_uuid = await self.save_media_to_db(conn, message.id, chat_id, media_info)
                
                if not media_uuid:
                    logging.warning(f"Media already existed and uploaded for message {message.id}, skipping queue")
                    
                    existing = await conn.fetchrow(
                        "SELECT id, public_url, uploaded FROM media_files WHERE checksum = $1",
                        media_info['file_hash']
                    )
                    
                    if existing and existing['uploaded']:
                        await self.send_media_ready_notification(
                            conn, message.id, chat_id, {
                                'media_id': str(existing['id']),
                                'public_url': existing['public_url']
                            }
                        )
                    
                    return None
                
                logging.info(f"Created new media {media_uuid} for message {message.id}")
            
            await self.send_media_status_notification(
                message.id, chat_id, media_uuid, 'uploading', 75
            )
            
            if self.s3_uploader and self.upload_on_finish:
                file_size = media_info['file_size']
                is_large = file_size >= self.LARGE_FILE_THRESHOLD
                priority = 5 if is_large else 10
                
                success = await self.s3_uploader.queue_media(
                    media_id=media_uuid,
                    file_path=media_info['temp_path'],
                    s3_key=media_info['s3_key'],
                    content_type=media_info['content_type'],
                    priority=priority,
                    file_size=file_size
                )
                
                if success:
                    self.stats['media_uploaded_s3'] += 1
                    
                    if is_large:
                        self.stats['large_files_uploaded'] += 1
                    
                    timeout = self.S3_UPLOAD_TIMEOUT_LARGE if is_large else self.S3_UPLOAD_TIMEOUT_SMALL
                    
                    task = asyncio.create_task(
                        self._wait_for_upload_and_notify(
                            media_uuid, message.id, chat_id, media_info['temp_path'], timeout
                        )
                    )
                    task.add_done_callback(lambda t: self._handle_task_exception(t, "wait_for_upload_and_notify"))
                    
                    if is_large:
                        logging.info(f"Waiting for large file upload (timeout={timeout}s)")
                    else:
                        logging.info(f"Small file queued, will notify when ready")
                    
                    return {'media_id': media_uuid, 'status': 'uploading'}
                else:
                    await self.send_media_status_notification(
                        message.id, chat_id, media_uuid, 'failed', 0
                    )
            
        except asyncio.CancelledError:
            logging.debug(f"Process media for message {message.id} cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in process_media for message {message.id}: {e}")
            import traceback
            traceback.print_exc()
            self.stats['errors'] += 1
            await self.send_media_status_notification(
                message.id, chat_id, None, 'failed', 0
            )
            return None
        finally:
            self._processing_media.discard(media_key)
        
        return None

    def _handle_task_exception(self, task, task_name):
        try:
            if task.cancelled():
                return
            exc = task.exception()
            if exc:
                logging.error(f"Task {task_name} failed with exception: {exc}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Error in task exception handler: {e}")

    async def _wait_for_upload_and_notify(self, media_uuid, message_id, chat_id, temp_path, timeout):
        """Ожидание загрузки с правильным таймаутом"""
        try:
            logging.info(f"Waiting for S3 upload of media {media_uuid} for message {message_id}")
            
            if not self.s3_uploader or not self.is_recording:
                logging.debug(f"Skipping notification for media {media_uuid}: recorder stopped")
                return
            
            success = await self.s3_uploader.wait_for_upload(media_uuid, timeout=timeout)
            
            if not self.is_recording:
                logging.debug(f"Recording stopped, skipping notification for media {media_uuid}")
                return
            
            if success:
                logging.info(f"✅ S3 upload completed for media {media_uuid} (message {message_id})")
                
                async with self.pool.acquire() as conn:
                    media_info = await conn.fetchrow(
                        "SELECT public_url FROM media_files WHERE id = $1",
                        media_uuid
                    )
                    
                    if media_info and media_info['public_url']:
                        await self.send_media_ready_notification(
                            conn, message_id, chat_id, {
                                'media_id': media_uuid,
                                'public_url': media_info['public_url']
                            }
                        )
                        
                        await self.send_media_status_notification(
                            message_id, chat_id, media_uuid, 'ready', 100
                        )
                    else:
                        logging.warning(f"Media {media_uuid} uploaded but no public_url found")
            else:
                logging.warning(f"⏰ Timeout or failure waiting for S3 upload of media {media_uuid}")
                await self.send_media_status_notification(
                    message_id, chat_id, media_uuid, 'failed', 0
                )
                
        except asyncio.CancelledError:
            logging.debug(f"Wait for upload for media {media_uuid} cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in _wait_for_upload_and_notify for media {media_uuid}: {e}")
            await self.send_media_status_notification(
                message_id, chat_id, media_uuid, 'failed', 0
            )

    # ==================== Уведомления ====================
    
    async def send_media_status_notification(self, message_id: int, chat_id: int,
                                           media_id: Optional[str], status: str, progress: int):
        try:
            notify_data = {
                'v': self.NOTIFY_VERSION,
                'type': 'media_status',
                'message_id': int(message_id),
                'chat_id': int(chat_id),
                'channel_id': int(chat_id),
                'media_id': media_id,
                'status': status,
                'progress': progress,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            payload = json.dumps(notify_data, default=str)
            
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "SELECT pg_notify('media_status', $1)",
                    payload
                )
            
            self.stats['notifications_sent'] += 1
            logging.debug(f"Sent media_status {status} for message {message_id}")
            
        except Exception as e:
            logging.error(f"Failed to send media_status notification: {e}")

    async def send_new_message_notification(self, conn, message_id, chat_id, date, has_media, media_type=None):
        try:
            notify_data = {
                'v': self.NOTIFY_VERSION,
                'type': 'new',
                'message_id': int(message_id),
                'chat_id': int(chat_id),
                'channel_id': int(chat_id),
                'date': date.isoformat() if date else datetime.utcnow().isoformat(),
                'has_media': has_media,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            if has_media and media_type:
                notify_data['media_type'] = media_type
            
            payload = json.dumps(notify_data, default=str)
            
            # Проверка длины payload (PostgreSQL NOTIFY ограничен ~8000 байт)
            if len(payload) > 7000:
                logging.warning(f"New notification payload too large ({len(payload)} bytes), removing optional fields")
                notify_data.pop('media_type', None)
                payload = json.dumps(notify_data, default=str)
                
                if len(payload) > 7000:
                    logging.error(f"New notification still too large ({len(payload)} bytes), sending minimal")
                    minimal_data = {
                        'v': self.NOTIFY_VERSION,
                        'type': 'new',
                        'message_id': int(message_id),
                        'chat_id': int(chat_id),
                        'channel_id': int(chat_id),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    payload = json.dumps(minimal_data, default=str)
            
            await conn.execute(
                "SELECT pg_notify('new_message', $1)",
                payload
            )
            
            self.stats['notifications_sent'] += 1
            logging.debug(f"Sent new message notification for {message_id}")
            
        except Exception as e:
            logging.error(f"Failed to send new_message notification: {e}")

    async def send_media_ready_notification(self, conn, message_id, chat_id, media_info):
        try:
            notify_data = {
                'v': self.NOTIFY_VERSION,
                'type': 'media_ready',
                'message_id': int(message_id),
                'chat_id': int(chat_id),
                'channel_id': int(chat_id),
                'media_id': media_info['media_id'],
                'media_url': media_info['public_url'],
                'timestamp': datetime.utcnow().isoformat()
            }
            
            payload = json.dumps(notify_data, default=str)
            
            await conn.execute(
                "SELECT pg_notify('media_ready', $1)",
                payload
            )
            
            self.stats['notifications_sent'] += 1
            logging.info(f"Sent media_ready notification for message {message_id}")
            
        except Exception as e:
            logging.error(f"Failed to send media_ready notification: {e}")

    async def send_edit_message_notification(self, conn, message_id, chat_id, edit_date, has_media=False, media_type=None):
        try:
            # Минимальное безопасное уведомление
            notify_data = {
                'v': self.NOTIFY_VERSION,
                'type': 'edit',
                'message_id': int(message_id),
                'chat_id': int(chat_id),
                'channel_id': int(chat_id),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Добавляем edit_date только если он есть
            if edit_date:
                notify_data['edit_date'] = edit_date.isoformat() if edit_date else datetime.utcnow().isoformat()
            
            # Добавляем has_media и media_type только если они небольшие
            if has_media:
                notify_data['has_media'] = has_media
                if media_type and len(media_type) < 50:
                    notify_data['media_type'] = media_type
            
            payload = json.dumps(notify_data, default=str)
            
            # Финальная проверка размера
            if len(payload) > 7000:
                logging.warning(f"Edit notification payload too large ({len(payload)} bytes), sending minimal")
                minimal_data = {
                    'v': self.NOTIFY_VERSION,
                    'type': 'edit',
                    'message_id': int(message_id),
                    'chat_id': int(chat_id),
                    'channel_id': int(chat_id),
                    'timestamp': datetime.utcnow().isoformat()
                }
                payload = json.dumps(minimal_data, default=str)
            
            await conn.execute(
                "SELECT pg_notify('edit_message', $1)",
                payload
            )
            
            self.stats['notifications_sent'] += 1
            logging.debug(f"Sent edit notification for message {message_id} (size={len(payload)} bytes)")
            
        except Exception as e:
            logging.error(f"Failed to send edit_message notification: {e}")

    async def send_delete_message_notification(self, conn, message_id, chat_id):
        try:
            notify_data = {
                'v': self.NOTIFY_VERSION,
                'type': 'delete',
                'message_id': int(message_id),
                'chat_id': int(chat_id),
                'channel_id': int(chat_id),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            payload = json.dumps(notify_data, default=str)
            
            await conn.execute(
                "SELECT pg_notify('delete_message', $1)",
                payload
            )
            
            self.stats['notifications_sent'] += 1
            logging.debug(f"Sent delete notification for message {message_id}")
            
        except Exception as e:
            logging.error(f"Failed to send delete_message notification: {e}")

    # ==================== Обработчики событий ====================
    
    async def new_message_handler(self, event):
        try:
            async with self.process_semaphore:
                message = event.message
                if not message:
                    return
                    
                chat_id = normalize_chat_id(message.chat_id)
                
                self.last_checked_message[chat_id] = max(
                    self.last_checked_message.get(chat_id, 0),
                    message.id
                )
                
                async with self.pool.acquire() as conn:
                    await self._process_single_message(message, chat_id, conn)
                    
        except asyncio.CancelledError:
            logging.debug("New message handler cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in new_message_handler: {e}")
            self.stats['errors'] += 1

    async def delete_message_handler(self, event):
        try:
            async with self.pool.acquire() as conn:
                chat_id = normalize_chat_id(event.chat_id) if hasattr(event, 'chat_id') else None
                if not chat_id:
                    return

                deleted_ids = getattr(event, 'deleted_ids', None)
                if not deleted_ids:
                    msg_id = getattr(event, 'message_id', None) or getattr(event, 'id', None)
                    if msg_id:
                        deleted_ids = [msg_id]
                    else:
                        return

                for msg_id in deleted_ids:
                    if not msg_id:
                        continue
                        
                    try:
                        await self.send_delete_message_notification(conn, msg_id, chat_id)
                        
                        result = await conn.execute(
                            "DELETE FROM messages WHERE message_id = $1 AND chat_id = $2",
                            msg_id, chat_id
                        )
                        deleted_count = int(result.split()[-1]) if result.startswith('DELETE') else 0

                        if deleted_count > 0:
                            logging.info(f"Deleted from DB: message_id={msg_id} chat={chat_id}")
                            
                            await conn.execute(
                                """
                                INSERT INTO service_messages (chat_id, action_type, action_details, message_id, date)
                                VALUES ($1, $2, $3, $4, $5)
                                """,
                                chat_id, 'message_deleted', json.dumps({'message_id': msg_id}),
                                msg_id, datetime.utcnow()
                            )
                        else:
                            logging.debug(f"Message {msg_id} already missing from DB")
                            
                    except Exception as e:
                        logging.error(f"Failed to send delete notification: {e}")
                        
        except asyncio.CancelledError:
            logging.debug("Delete message handler cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in delete_message_handler: {e}")
            self.stats['errors'] += 1

    async def edit_message_handler(self, event):
        try:
            async with self.pool.acquire() as conn:
                message = event.message
                if not message:
                    return
                    
                chat_id = normalize_chat_id(message.chat_id)
               
                exists = await conn.fetchval(
                    "SELECT 1 FROM messages WHERE message_id = $1 AND chat_id = $2",
                    message.id, chat_id
                )
               
                if not exists:
                    logging.warning(f"Attempt to edit non-existent message {message.id} in chat {chat_id}")
                    return
                
                text = message.text or ''
                if isinstance(text, bytes):
                    text = text.decode('utf-8', errors='replace')
                
                # Ограничиваем текст до безопасного размера
                if len(text) > 5000:
                    text = text[:5000] + '...[truncated]'
                    logging.debug(f"Truncated text for edited message {message.id} to 5000 chars")
                
                text_fingerprint = compute_text_fingerprint(text)
                entities_json = await self.extract_message_entities(message)
                
                await self.send_edit_message_notification(
                    conn,
                    message.id,
                    chat_id,
                    message.edit_date,
                    has_media=message.media is not None,
                    media_type=message.media.__class__.__name__ if message.media else None
                )
                
                result = await conn.execute(
                    """
                    UPDATE messages
                    SET text = $1, is_edited = TRUE, edit_date = $2, media_type = $3,
                        entities = $4, text_fingerprint = $5
                    WHERE message_id = $6 AND chat_id = $7
                    """,
                    text,
                    message.edit_date.replace(tzinfo=None) if message.edit_date else datetime.utcnow(),
                    message.media.__class__.__name__ if message.media else None,
                    entities_json,
                    text_fingerprint,
                    message.id,
                    chat_id
                )
                
                if int(result.split()[-1]) > 0:
                    logging.info(f"Edited: message_id={message.id} chat={chat_id}")
                   
                    await conn.execute(
                        """
                        INSERT INTO service_messages (chat_id, action_type, action_details, message_id, date)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        chat_id,
                        'message_edited',
                        json.dumps({
                            'message_id': message.id,
                            'edit_date': message.edit_date.isoformat() if message.edit_date else None
                        }),
                        message.id,
                        datetime.utcnow()
                    )
                    
                    chat_list = await get_chat_list(conn, chat_id)
                    
                    media_type = self.get_media_type_from_message(message)
                    if message.media and chat_list == 'white' and media_type in ['photos', 'videos', 'webpage_preview']:
                        task = asyncio.create_task(self.process_media(message, chat_id))
                        task.add_done_callback(lambda t: self._handle_task_exception(t, "process_media"))
                               
        except asyncio.CancelledError:
            logging.debug("Edit message handler cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in edit_message_handler: {e}")
            self.stats['errors'] += 1

    async def _process_single_message(self, message, chat_id, conn):
        try:
            # Проверка на стикеры
            if message.media:
                if hasattr(message.media, 'document') and message.media.document:
                    doc = message.media.document
                    if doc.attributes:
                        for attr in doc.attributes:
                            if isinstance(attr, DocumentAttributeSticker):
                                logging.info(f"Skipping sticker message {message.id} in chat {chat_id}")
                                return
            
            message_date = message.date.replace(tzinfo=None) if message.date.tzinfo else message.date
            
            chat_exists = await conn.fetchval(
                "SELECT 1 FROM chats WHERE chat_id = $1",
                chat_id
            )
            if not chat_exists:
                logging.error(f"Chat {chat_id} missing from chats table")
                return

            await ensure_partition_for_date(conn, message_date)

            if chat_id not in self.chat_types:
                try:
                    chat = await self.client.get_entity(chat_id)
                    is_channel = hasattr(chat, 'broadcast') and chat.broadcast
                    self.chat_types[chat_id] = is_channel
                except Exception as e:
                    logging.warning(f"Could not determine type for {chat_id}: {e}")
                    self.chat_types[chat_id] = False
            else:
                is_channel = self.chat_types[chat_id]

            sender_id = message.sender_id
            if isinstance(sender_id, (int, float)) and sender_id > 0 and not is_channel:
                try:
                    sender = await self.client.get_entity(sender_id)
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
                    await add_user_to_db(conn, sender_data)
                except Exception as e:
                    logging.warning(f"Could not get sender data {sender_id} for {chat_id}: {e}")
                    sender_id = None
            else:
                sender_id = None

            text = message.text or ''
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='replace')
            elif not isinstance(text, str):
                text = str(text) if text else ''

            text_fingerprint = compute_text_fingerprint(text)
            
            entities_json = await self.extract_message_entities(message)
            forward_info = await self.extract_forward_info(message)
            reactions_json = await self.extract_reactions(message)
            action_json = await self.extract_action_info(message)

            exists = await conn.fetchval(
                "SELECT 1 FROM messages WHERE message_id = $1 AND chat_id = $2",
                message.id, chat_id
            )
            
            if not exists:
                # Проверка на медиа (включая WebPage preview)
                has_media = message.media is not None
                
                if has_media and hasattr(message.media, 'document') and message.media.document:
                    for attr in message.media.document.attributes:
                        if isinstance(attr, DocumentAttributeSticker):
                            has_media = False
                            break
                
                # WebPage не считается медиа для хранения (но превью обработаем отдельно)
                if has_media and isinstance(message.media, MessageMediaWebPage):
                    has_media = False
                
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
                    'media_metadata': None,
                    'reactions': reactions_json,
                    'reactions_count': len(json.loads(reactions_json)) if reactions_json else 0,
                    'recent_reactions': reactions_json,
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
                    'tags': json.dumps({'settings_applied': True, 'source': 'realtime'}),
                    'text_fingerprint': text_fingerprint,
                    'system_received_at': datetime.utcnow()
                }
                
                bigint_fields = ['grouped_id', 'via_bot_id', 'reply_to_msg_id', 'reply_to_top_id', 'thread_id']
                for field in bigint_fields:
                    if message_data[field] is not None:
                        try:
                            int_val = int(message_data[field])
                            message_data[field] = int_val
                        except (ValueError, TypeError):
                            logging.error(f"Field '{field}' has non-integer value: {message_data[field]}")
                            message_data[field] = None
                
                await conn.execute("""
                    INSERT INTO messages (
                        message_id, chat_id, sender_id, text, date, views, forwards,
                        reply_to_msg_id, reply_to_top_id, thread_id, reply_to_date,
                        media_type, media_metadata, reactions, reactions_count, recent_reactions,
                        is_edited, edit_date, out, mentioned, silent, post,
                        from_scheduled, legacy, edit_hide, entities, forward_info,
                        grouped_id, via_bot_id, ttl_period, restriction_reason, action,
                        tags, text_fingerprint, system_received_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                        $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
                        $28, $29, $30, $31, $32, $33, $34, $35
                    )
                    ON CONFLICT (message_id, date) DO NOTHING
                """,
                message_data['message_id'],
                message_data['chat_id'],
                message_data['sender_id'],
                message_data['text'],
                message_data['date'],
                message_data['views'],
                message_data['forwards'],
                message_data['reply_to_msg_id'],
                message_data['reply_to_top_id'],
                message_data['thread_id'],
                None,
                message_data['media_type'],
                message_data['media_metadata'],
                message_data['reactions'],
                message_data['reactions_count'],
                message_data['recent_reactions'],
                message_data['is_edited'],
                message_data['edit_date'],
                message_data['out'],
                message_data['mentioned'],
                message_data['silent'],
                message_data['post'],
                message_data['from_scheduled'],
                message_data['legacy'],
                message_data['edit_hide'],
                message_data['entities'],
                message_data['forward_info'],
                message_data['grouped_id'],
                message_data['via_bot_id'],
                message_data['ttl_period'],
                message_data['restriction_reason'],
                message_data['action'],
                message_data['tags'],
                message_data['text_fingerprint'],
                message_data['system_received_at']
                )
                
                logging.info(f"Recorded message ID {message.id} in {chat_id}")
                
                chat_list = await get_chat_list(conn, chat_id)
                
                await self.send_new_message_notification(
                    conn,
                    message.id,
                    chat_id,
                    message_date,
                    has_media,
                    message.media.__class__.__name__ if message.media else None
                )
                
                # Обработка медиа (включая WebPage preview)
                media_type = self.get_media_type_from_message(message)
                if message.media and chat_list == 'white' and media_type in ['photos', 'videos', 'webpage_preview']:
                    task = asyncio.create_task(self.process_media(message, chat_id))
                    task.add_done_callback(lambda t: self._handle_task_exception(t, "process_media"))

        except asyncio.CancelledError:
            logging.debug(f"Process single message {message.id} cancelled")
            raise
        except Exception as e:
            logging.error(f"Error processing message {message.id}: {e}")
            raise

    # ==================== Извлечение данных ====================
    
    async def extract_message_entities(self, message):
        entities_data = []
        if message.entities:
            # Ограничиваем КОЛИЧЕСТВО entities, а не длину JSON
            max_entities = 30
            for e in message.entities[:max_entities]:
                entity = {
                    'type': type(e).__name__,
                    'offset': e.offset,
                    'length': e.length
                }
                if hasattr(e, 'url') and e.url:
                    # Ограничиваем длину URL, но не ломаем JSON
                    url = e.url
                    if len(url) > 150:
                        url = url[:150] + '...'
                    entity['url'] = url
                if hasattr(e, 'email') and e.email:
                    entity['email'] = e.email
                if hasattr(e, 'phone') and e.phone:
                    entity['phone'] = e.phone
                if hasattr(e, 'mention') and e.mention:
                    entity['mention'] = e.mention
                if hasattr(e, 'language') and e.language:
                    entity['language'] = e.language
                entities_data.append(entity)
            
            if len(message.entities) > max_entities:
                entities_data.append({'_warning': f'truncated from {len(message.entities)} entities'})
        
        result = json.dumps(entities_data, ensure_ascii=False) if entities_data else None
        
        # ✅ ИСПРАВЛЕНИЕ: Не обрезаем JSON строку!
        # Вместо этого, если JSON слишком большой - логируем и возвращаем None
        if result and len(result) > 10000:
            logging.warning(f"Entities JSON too large ({len(result)} bytes) for message {message.id}, skipping")
            return None
        
        return result

    async def extract_forward_info(self, message):
        if message.fwd_from:
            return json.dumps({
                'original_chat_id': getattr(message.fwd_from, 'chat_id', None),
                'original_message_id': getattr(message.fwd_from, 'message_id', None),
                'original_date': message.fwd_from.date.isoformat() if message.fwd_from.date else None,
                'original_author': getattr(message.fwd_from, 'from_name', None),
                'forward_signature': getattr(message.fwd_from, 'post_author', None),
                'saved_from_peer': getattr(message.fwd_from, 'saved_from_peer', None),
                'channel_post': getattr(message.fwd_from, 'channel_post', None)
            })
        return None

    async def extract_reactions(self, message):
        if hasattr(message, 'reactions') and message.reactions:
            reactions_data = []
            if hasattr(message.reactions, 'results'):
                for r in message.reactions.results:
                    reaction_data = {
                        'count': getattr(r, 'count', 0),
                        'chosen': getattr(r, 'chosen', False)
                    }
                    
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
            
            return json.dumps(reactions_data)
        return None

    async def extract_action_info(self, message):
        if message.action:
            action_data = {'type': type(message.action).__name__}
            
            if isinstance(message.action, MessageActionChatAddUser):
                action_data['users'] = getattr(message.action, 'users', [])
            elif isinstance(message.action, MessageActionChatDeleteUser):
                action_data['user_id'] = getattr(message.action, 'user_id', None)
            elif isinstance(message.action, MessageActionChatEditTitle):
                action_data['title'] = getattr(message.action, 'title', None)
            elif isinstance(message.action, MessageActionPinMessage):
                action_data['pinned'] = True
                
            return json.dumps(action_data)
        return None

    # ==================== Периодическая проверка (оптимизированная) ====================
    
    async def poll_chats(self, interval=300):
        """Периодическая проверка с разнесением по времени для снижения нагрузки на API"""
        first_run = True
        
        while self.is_recording:
            try:
                recorded_chats = await self.fetch_chats()
                white_chats = await self.fetch_chats(list_type='white')
                
                dialog_chats = set()
                async for dialog in self.client.iter_dialogs():
                    dialog_chats.add(normalize_chat_id(dialog.entity.id))

                non_dialog_chats = recorded_chats - dialog_chats
                
                # Проверка не-диалогов (без активной подписки) с задержками
                for idx, chat_id in enumerate(non_dialog_chats):
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    
                    try:
                        last_message_id = self.last_checked_message.get(chat_id, 0)
                        
                        new_messages = []
                        async for message in self.client.iter_messages(
                            chat_id, min_id=last_message_id, limit=50
                        ):
                            if message.id > last_message_id:
                                new_messages.append(message)
                                self.last_checked_message[chat_id] = max(
                                    self.last_checked_message.get(chat_id, 0),
                                    message.id
                                )
                        
                        if new_messages:
                            await self.process_messages_batch(chat_id, new_messages)
                            if chat_id in white_chats:
                                await self.check_missing_media(chat_id, new_messages)
                        
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                        
                    except FloodWaitError as e:
                        logging.warning(f"Flood wait on chat {chat_id}: {e.seconds}s")
                        self.stats['flood_wait_events'] += 1
                        await asyncio.sleep(e.seconds + 1)
                    except Exception as e:
                        logging.error(f"Error processing chat {chat_id} during poll: {e}")
                
                # Проверка белых чатов (подписанных) с разнесением
                for idx, chat_id in enumerate(white_chats):
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    
                    try:
                        now = time.time()
                        last_check = self.last_deleted_check.get(chat_id, 0)
                        
                        check_interval = 600 # 10 минут
                        if first_run or (now - last_check > check_interval):
                            jitter = random.uniform(-60, 60)
                            if first_run or (now - last_check > check_interval + jitter):
                                logging.info(f"🔍 Periodic check for deleted/edited messages in chat {chat_id}")
                                await self.check_deleted_and_edited_messages(chat_id)
                                self.last_deleted_check[chat_id] = now
                                
                                await asyncio.sleep(random.uniform(2.0, 5.0))
                        
                    except FloodWaitError as e:
                        logging.warning(f"Flood wait on chat {chat_id}: {e.seconds}s")
                        self.stats['flood_wait_events'] += 1
                        await asyncio.sleep(e.seconds + 1)
                    except Exception as e:
                        logging.error(f"Error checking deleted/edited for chat {chat_id}: {e}")
                
                first_run = False
                
                sleep_time = interval + random.uniform(-30, 30)
                try:
                    await asyncio.sleep(max(60, sleep_time))
                except asyncio.CancelledError:
                    logging.info("Polling task cancelled during sleep")
                    break
                
            except asyncio.CancelledError:
                logging.info("Polling task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in poll_chats: {e}")
                print_message(f"Error in polling chats: {e}", level="error")
                try:
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break

    async def check_missing_media(self, chat_id, messages):
        async with self.pool.acquire() as conn:
            for message in messages:
                if not message.media:
                    continue
                
                media_type = self.get_media_type_from_message(message)
                
                if media_type not in ['photos', 'videos', 'webpage_preview']:
                    logging.debug(f"Skipping {media_type} for message {message.id} in white chat {chat_id}")
                    continue
                
                exists = await self.is_media_downloaded(message.id, chat_id, conn)
                if not exists:
                    logging.info(f"Missing media found for message {message.id} in white chat {chat_id}, downloading...")
                    task = asyncio.create_task(self.process_media(message, chat_id))
                    task.add_done_callback(lambda t: self._handle_task_exception(t, "process_media"))

    async def is_media_downloaded(self, message_id, chat_id, conn):
        try:
            exists = await conn.fetchval("""
                SELECT 1 FROM message_media mm
                JOIN media_files mf ON mm.media_id = mf.id
                WHERE mm.message_id = $1 AND mm.chat_id = $2
            """, message_id, chat_id)
            return bool(exists)
        except Exception as e:
            logging.error(f"Error checking media duplication: {e}")
            return False

    async def process_messages_batch(self, chat_id, messages):
        async with self.process_semaphore:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    for message in messages:
                        try:
                            await self._process_single_message(message, chat_id, conn)
                            self.stats['messages_processed'] += 1
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            logging.error(f"Error processing message {message.id}: {e}")
                            self.stats['errors'] += 1
                            continue

    # ==================== Проверка удаленных/измененных (оптимизированная) ====================
    
    async def check_deleted_and_edited_messages(self, chat_id):
        """Проверка удаленных и измененных сообщений с пагинацией и защитой от flood wait"""
        try:
            logging.info(f"🔍 Starting periodic check for deleted/edited messages in chat {chat_id}")
            
            async with self.pool.acquire() as conn:
                db_messages = await conn.fetch("""
                    SELECT message_id, date, is_edited, text_fingerprint, text, media_type
                    FROM messages
                    WHERE chat_id = $1
                    ORDER BY date DESC
                    LIMIT 100
                """, chat_id)
            
            if not db_messages:
                logging.info(f"📭 No messages in DB for chat {chat_id} to check")
                return
            
            logging.info(f"📊 Checking {len(db_messages)} messages in chat {chat_id}")
            
            message_ids = [row['message_id'] for row in db_messages]
            
            chunks = [message_ids[i:i+20] for i in range(0, len(message_ids), 20)]
            
            all_telegram_messages = {}
            
            for i, chunk in enumerate(chunks):
                try:
                    if i > 0:
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                    
                    logging.info(f"📡 Fetching chunk {i+1}/{len(chunks)} with {len(chunk)} messages")
                    
                    async for msg in self.client.iter_messages(chat_id, ids=chunk):
                        if msg:
                            all_telegram_messages[msg.id] = msg
                            logging.info(f"✅ Found message {msg.id} in Telegram")
                    
                    self.stats['api_calls_saved'] += 1
                    
                except FloodWaitError as e:
                    logging.warning(f"Flood wait on chat {chat_id}: {e.seconds}s")
                    self.stats['flood_wait_events'] += 1
                    await asyncio.sleep(e.seconds + 1)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logging.error(f"Error fetching chunk {i} for chat {chat_id}: {e}")
                    continue
            
            # Проверка удаленных
            deleted_ids = []
            for msg_id in message_ids:
                if msg_id not in all_telegram_messages:
                    deleted_ids.append(msg_id)
            
            if deleted_ids:
                logging.info(f"🔍 Found {len(deleted_ids)} deleted messages in chat {chat_id}")
                
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        for msg_id in deleted_ids[:50]:
                            try:
                                await self.send_delete_message_notification(conn, msg_id, chat_id)
                                await conn.execute(
                                    "DELETE FROM messages WHERE message_id = $1 AND chat_id = $2",
                                    msg_id, chat_id
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                logging.error(f"Error processing deleted message {msg_id}: {e}")
            else:
                logging.info(f"✅ No deleted messages found in chat {chat_id}")
            
            # Проверка измененных - ОТДЕЛЬНАЯ ТРАНЗАКЦИЯ ДЛЯ КАЖДОГО СООБЩЕНИЯ
            edited_count = 0
            for row in db_messages:
                msg_id = row['message_id']
                
                # Пропускаем уже отмеченные как edited
                if row['is_edited']:
                    logging.debug(f"Message {msg_id} already marked as edited, skipping")
                    continue
                
                if msg_id in all_telegram_messages:
                    tg_msg = all_telegram_messages[msg_id]
                    
                    current_text = tg_msg.text or ''
                    
                    # Ограничиваем текст до безопасного размера
                    if len(current_text) > 5000:
                        current_text = current_text[:5000] + '...[truncated]'
                        logging.debug(f"Truncated text for message {msg_id} to 5000 chars")
                    
                    current_fingerprint = compute_text_fingerprint(current_text)
                    db_fingerprint = row['text_fingerprint']
                    
                    text_changed = (current_fingerprint != db_fingerprint)
                    
                    # Только если есть реальные изменения
                    if text_changed or tg_msg.edit_date:
                        logging.info(f"✏️ Found missed edited message {msg_id} in chat {chat_id}")
                        
                        edited_count += 1
                        
                        # ✅ ИСПРАВЛЕНИЕ: ОТДЕЛЬНАЯ ТРАНЗАКЦИЯ для каждого сообщения
                        try:
                            async with self.pool.acquire() as conn:
                                async with conn.transaction():
                                    # Безопасное получение entities
                                    entities_json = None
                                    try:
                                        entities_json = await self.extract_message_entities(tg_msg)
                                        # Проверяем, что entities_json - валидный JSON
                                        if entities_json:
                                            import json as json_module
                                            json_module.loads(entities_json)  # Проверка валидности
                                    except Exception as e:
                                        logging.warning(f"Invalid entities for message {msg_id}: {e}")
                                        entities_json = None
                                    
                                    try:
                                        await conn.execute("""
                                            UPDATE messages
                                            SET text = $1,
                                                is_edited = TRUE,
                                                edit_date = $2,
                                                text_fingerprint = $3,
                                                entities = $4
                                            WHERE message_id = $5 AND chat_id = $6
                                            AND is_edited = FALSE
                                        """,
                                        current_text,
                                        tg_msg.edit_date.replace(tzinfo=None) if tg_msg.edit_date else datetime.utcnow(),
                                        current_fingerprint[:100] if current_fingerprint else None,
                                        entities_json,
                                        msg_id, chat_id
                                        )
                                        logging.info(f"✅ Updated missed edit for message {msg_id}")
                                    except Exception as update_err:
                                        logging.error(f"Failed to update message {msg_id}: {update_err}")
                                        # Пробуем обновить только флаг (без entities)
                                        try:
                                            await conn.execute("""
                                                UPDATE messages
                                                SET is_edited = TRUE,
                                                    edit_date = $1
                                                WHERE message_id = $2 AND chat_id = $3
                                                AND is_edited = FALSE
                                            """,
                                            tg_msg.edit_date.replace(tzinfo=None) if tg_msg.edit_date else datetime.utcnow(),
                                            msg_id, chat_id
                                            )
                                            logging.info(f"✅ Minimal update (flag only) for message {msg_id}")
                                        except Exception as minimal_err:
                                            logging.error(f"Even minimal update failed for {msg_id}: {minimal_err}")
                        except Exception as tx_err:
                            logging.error(f"Transaction error for message {msg_id}: {tx_err}")
                            continue  # Переходим к следующему сообщению
            
            if edited_count > 0:
                logging.info(f"📝 Found and updated {edited_count} missed edited messages in chat {chat_id}")
            else:
                logging.info(f"✅ No missed edited messages found in chat {chat_id}")
            
        except asyncio.CancelledError:
            logging.debug(f"Check deleted/edited for chat {chat_id} cancelled")
            raise
        except Exception as e:
            logging.error(f"❌ Error checking deleted/edited messages for chat {chat_id}: {e}")
            import traceback
            traceback.print_exc()

    # ==================== Управление записью ====================
    
    async def start_recording(self):
        if self.is_recording:
            print_message("Recording already active.", level="warning")
            return

        self.recorded_chats = await self.fetch_chats()
        if not self.recorded_chats:
            print_message("No active sources for recording. Add via 'Recording list'.", level="warning")
            return

        white_chats = await self.fetch_chats(list_type='white')
        
        print_message(f"Checking for missed messages in {len(self.recorded_chats)} chats...")
        
        for chat_id in self.recorded_chats:
            try:
                async with self.pool.acquire() as conn:
                    last_message = await conn.fetchval(
                        "SELECT message_id FROM messages WHERE chat_id = $1 ORDER BY date DESC LIMIT 1",
                        chat_id
                    )
                
                if last_message:
                    with_media = (chat_id in white_chats)
                    
                    missed_count = 0
                    async for _ in self.client.iter_messages(chat_id, min_id=last_message, limit=1000):
                        missed_count += 1
                    
                    if missed_count > 0:
                        limit = min(missed_count, 50)
                        logging.info(f"Found {missed_count} missed messages in {chat_id}, importing {limit}")
                        await self.import_missed_messages(chat_id, limit=limit, with_media=with_media)
                    else:
                        logging.debug(f"No missed messages in {chat_id}")
                else:
                    logging.info(f"No messages in DB for {chat_id}, importing last 10")
                    await self.import_missed_messages(chat_id, limit=10, with_media=(chat_id in white_chats))
                    
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    
            except FloodWaitError as e:
                logging.warning(f"Flood wait importing {chat_id}: {e.seconds}s")
                self.stats['flood_wait_events'] += 1
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                logging.error(f"Error importing messages for {chat_id}: {e}")
                print_message(f"Error importing messages for {chat_id}: {e}", level="error")

        self.new_handler = self.new_message_handler
        self.delete_handler = self.delete_message_handler
        self.edit_handler = self.edit_message_handler

        self.client.add_event_handler(
            self.new_handler,
            events.NewMessage(chats=self.recorded_chats)
        )
        self.client.add_event_handler(
            self.delete_handler,
            events.MessageDeleted(chats=self.recorded_chats)
        )
        self.client.add_event_handler(
            self.edit_handler,
            events.MessageEdited(chats=self.recorded_chats)
        )

        self.is_recording = True
        print_message(f"Recording started, sources: {len(self.recorded_chats)} (white: {len(white_chats)}).")
        logging.info(f"Recording started for {len(self.recorded_chats)} chats, {len(white_chats)} white")

        self.polling_task = asyncio.create_task(self.poll_chats(interval=300))

    async def stop_recording(self):
        if not self.is_recording:
            print_message("Recording not active.", level="warning")
            return

        # Сначала отменяем polling_task
        if self.polling_task:
            self.polling_task.cancel()
            try:
                await asyncio.wait_for(self.polling_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logging.error(f"Error cancelling polling task: {e}")
            self.polling_task = None

        # Удаляем обработчики событий
        if self.new_handler:
            try:
                self.client.remove_event_handler(self.new_handler)
            except Exception as e:
                logging.error(f"Error removing new_handler: {e}")
            self.new_handler = None
            
        if self.delete_handler:
            try:
                self.client.remove_event_handler(self.delete_handler)
            except Exception as e:
                logging.error(f"Error removing delete_handler: {e}")
            self.delete_handler = None
            
        if self.edit_handler:
            try:
                self.client.remove_event_handler(self.edit_handler)
            except Exception as e:
                logging.error(f"Error removing edit_handler: {e}")
            self.edit_handler = None

        # Даем время на завершение текущих задач
        await asyncio.sleep(0.5)

        self.is_recording = False
        print_message("Recording and polling stopped.")
        logging.info("Recording and polling stopped")

    # ==================== Меню и статистика ====================
    
    async def recording_menu(self):
        menu_options = [
            {"name": f"1. {'Disable' if self.is_recording else 'Enable'} database recording [{'ON' if self.is_recording else 'OFF'}]", "value": "1"},
            {"name": "2. Show statistics", "value": "2"},
            {"name": "3. Back", "value": "3"}
        ]

        while True:
            try:
                choice = await select("Recording management:", choices=menu_options).ask_async()
                
                if choice == "1":
                    if self.is_recording:
                        await self.stop_recording()
                    else:
                        await self.start_recording()
                    menu_options[0]["name"] = f"1. {'Disable' if self.is_recording else 'Enable'} database recording [{'ON' if self.is_recording else 'OFF'}]"
                    
                elif choice == "2":
                    await self.show_statistics()
                    
                elif choice == "3":
                    print_message("Returning to main menu.")
                    break
                    
                else:
                    print_message("Invalid choice.", level="error")
                    
            except KeyboardInterrupt:
                print_message("Operation interrupted (Ctrl+C). Returning to submenu.", level="warning")
            except Exception as e:
                logging.error(f"Error in recording submenu: {e}")
                print_message(f"Error: {e}", level="error")

    async def show_statistics(self):
        print(f"{Fore.CYAN}=== Realtime Recorder Statistics ===")
        print(f"Messages processed: {self.stats['messages_processed']}")
        print(f"Media downloaded: {self.stats['media_downloaded']}")
        print(f"Media uploaded to S3: {self.stats['media_uploaded_s3']}")
        print(f"Large files uploaded: {self.stats.get('large_files_uploaded', 0)}")
        print(f"Webpage previews: {self.stats.get('webpage_previews', 0)}")
        print(f"Notifications sent: {self.stats.get('notifications_sent', 0)}")
        print(f"Memory pressure events: {self.stats.get('memory_pressure_events', 0)}")
        print(f"Flood wait events: {self.stats.get('flood_wait_events', 0)}")
        print(f"API calls saved: {self.stats.get('api_calls_saved', 0)}")
        print(f"Errors: {self.stats['errors']}")
        
        if self.s3_uploader:
            s3_stats = await self.s3_uploader.get_queue_stats()
            print(f"\n{Fore.CYAN}=== S3 Uploader Statistics ===")
            print(f"Small queue size: {s3_stats.get('small_queue_size', 0)}")
            print(f"Large queue size: {s3_stats.get('large_queue_size', 0)}")
            print(f"Uploaded: {s3_stats.get('uploaded', 0)}")
            print(f"Failed: {s3_stats.get('failed', 0)}")
            print(f"Pending events: {s3_stats.get('pending_events', 0)}")
        
        await asyncio.sleep(3)