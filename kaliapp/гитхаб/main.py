# main.py
import asyncio
import logging
import sys
import os
import json
import argparse
import signal
from datetime import datetime
from typing import Optional, Dict, Any, Set
from pathlib import Path

from prompt_toolkit import PromptSession
from questionary import select, confirm
from colorama import Fore, Style, init as colorama_init

from logger import setup_logging
from db_module import (
    init_db, execute_sql_query, db_config, manage_lists_menu, 
    migrate_users_to_gray_list, print_message, manage_settings_menu, 
    add_chat_to_list, get_chat_list, create_db_pool, get_pool_stats
)
from telegram_client import start_telegram_client, manage_sources_menu
from realtime_recorder import RealtimeRecorder
from import_module import import_to_db_menu
from process_event_queue import process_queue
from tabulate import tabulate
from http_sender import process_http_queue
from s3_uploader import S3Uploader
from s3_recovery import S3Recovery
import asyncpg
import warnings

# Игнорируем предупреждения
warnings.filterwarnings("ignore", category=UserWarning, module="pymorphy2")
warnings.filterwarnings("ignore", message="Using async sessions support is an experimental feature")

# Инициализация colorama
colorama_init(autoreset=True)

# ==================== Класс состояния приложения ====================

class AppState:
    """Управление состоянием приложения."""
    
    def __init__(self):
        self.client = None
        self.client_initialized = False
        self.recorder = None
        self.db_pool = None
        self.s3_uploader = None
        self.s3_recovery = None
        self.queue_task = None
        self.http_queue_task = None
        self.active_chats: Set[int] = set()
        self.is_running = True
        self.shutdown_in_progress = False
        self.state_file = Path("state.json")
        
    async def load(self) -> bool:
        """Загружает состояние из файла."""
        try:
            if not self.state_file.exists():
                return False
                
            with open(self.state_file, "r") as f:
                state = json.load(f)
            
            self.client_initialized = state.get("client_initialized", False)
            self.active_chats = set(state.get("active_chats", []))
            
            logging.info(f"State loaded: client_initialized={self.client_initialized}, "
                        f"active_chats={len(self.active_chats)}")
            return True
            
        except Exception as e:
            logging.error(f"Error loading state: {e}")
            return False
    
    async def save(self):
        """Сохраняет состояние в файл."""
        try:
            active_chats = []
            if self.db_pool and not self.db_pool._closed:
                try:
                    async with self.db_pool.acquire() as conn:
                        rows = await conn.fetch(
                            "SELECT chat_id FROM chats WHERE is_active = TRUE"
                        )
                        active_chats = [row['chat_id'] for row in rows]
                except Exception as e:
                    logging.error(f"Error fetching active chats for save: {e}")
                    active_chats = list(self.active_chats)
            else:
                active_chats = list(self.active_chats)
            
            state = {
                "client_initialized": self.client_initialized,
                "is_recording": self.recorder.is_recording if self.recorder else False,
                "active_chats": active_chats,
                "last_updated": datetime.now().isoformat()
            }
            
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, "w") as f:
                json.dump(state, f, indent=2)
            
            temp_file.replace(self.state_file)
            
            logging.info(f"State saved: client={self.client_initialized}, "
                        f"recording={state['is_recording']}, chats={len(active_chats)}")
            
        except Exception as e:
            logging.error(f"Error saving state: {e}")
    
    async def cleanup(self):
        """Корректная очистка всех ресурсов."""
        if self.shutdown_in_progress:
            return
            
        self.shutdown_in_progress = True
        self.is_running = False
        
        print_message("🛑 Shutting down components...", level="warning")
        logging.info("Starting graceful shutdown...")
        
        # 1. Сначала останавливаем запись
        if self.recorder and self.recorder.is_recording:
            print_message("Stopping recording...", level="warning")
            await self.recorder.stop_recording()
        
        # 2. Останавливаем фоновые задачи
        if self.queue_task and not self.queue_task.done():
            print_message("Cancelling queue processor...", level="warning")
            self.queue_task.cancel()
            try:
                await asyncio.wait_for(self.queue_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logging.error(f"Error cancelling queue task: {e}")
        
        if self.http_queue_task and not self.http_queue_task.done():
            print_message("Cancelling HTTP queue processor...", level="warning")
            self.http_queue_task.cancel()
            try:
                await asyncio.wait_for(self.http_queue_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logging.error(f"Error cancelling HTTP task: {e}")
        
        # 3. Останавливаем S3 компоненты
        if self.s3_recovery:
            print_message("Stopping S3 recovery...", level="warning")
            await self.s3_recovery.stop()
        
        if self.s3_uploader:
            print_message("Stopping S3 uploader...", level="warning")
            await self.s3_uploader.stop()
        
        # 4. Отключаем Telegram клиент
        if self.client and self.client_initialized:
            print_message("Disconnecting Telegram client...", level="warning")
            try:
                await asyncio.wait_for(self.client.disconnect(), timeout=5.0)
            except asyncio.TimeoutError:
                logging.warning("Telegram client disconnect timeout")
            except Exception as e:
                logging.error(f"Error disconnecting client: {e}")
            finally:
                try:
                    if hasattr(self.client, '_connection') and self.client._connection:
                        await self.client._connection.disconnect()
                except:
                    pass
        
        # 5. Сохраняем состояние
        await self.save()
        
        # 6. Закрываем пул соединений
        if self.db_pool and not self.db_pool._closed:
            print_message("Closing database connections...", level="warning")
            try:
                await asyncio.wait_for(self.db_pool.close(), timeout=5.0)
            except asyncio.TimeoutError:
                logging.warning("Database pool close timeout")
            except Exception as e:
                logging.error(f"Error closing database pool: {e}")
        
        # 7. Даем время на завершение всех задач
        await asyncio.sleep(0.5)
        
        logging.info("Shutdown complete")
        print_message("✅ Shutdown complete", level="info")


# ==================== Глобальное состояние ====================

app_state = AppState()


# ==================== Обработчики сигналов ====================

def signal_handler(sig, frame):
    """Обработчик сигналов для корректного завершения."""
    print()
    print_message(f"Received signal {sig}, initiating shutdown...", level="warning")
    
    if app_state.shutdown_in_progress:
        print_message("Forced exit", level="error")
        sys.exit(1)
    
    # Безопасное создание задачи из обработчика сигнала
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    
    loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown_and_exit()))


async def shutdown_and_exit():
    """Асинхронное завершение с выходом."""
    await app_state.cleanup()
    print_message("Goodbye! 👋", level="info")
    sys.exit(0)


# ==================== Меню просмотра и поиска ====================

async def search_and_view_menu():
    """Подменю для просмотра, поиска и аналитики."""
    menu_options = [
        {"name": "1. Статистика по чатам", "value": "1"},
        {"name": "2. Анализ ключевых слов", "value": "2"},
        {"name": "3. Анализ NER (именованные сущности)", "value": "3"},
        {"name": "4. Поиск сообщений", "value": "4"},
        {"name": "5. Статистика S3 загрузок", "value": "5"},
        {"name": "6. Назад", "value": "6"}
    ]

    while app_state.is_running:
        try:
            choice = await select("Просмотр и поиск:", choices=menu_options).ask_async()
            
            if not app_state.is_running:
                break

            if choice == "1":
                await show_chat_stats()
            elif choice == "2":
                await show_keyword_analysis()
            elif choice == "3":
                await show_ner_analysis()
            elif choice == "4":
                print_message("Поиск сообщений пока не реализован.", level="warning")
            elif choice == "5":
                await show_s3_stats()
            elif choice == "6":
                print_message("Возврат в главное меню.")
                break

        except KeyboardInterrupt:
            print_message("Операция прервана (Ctrl+C).", level="warning")
            break
        except Exception as e:
            logging.error(f"Ошибка в меню просмотра: {e}")
            print_message(f"Ошибка: {e}", level="error")


async def show_chat_stats():
    """Показ статистики по чатам."""
    page = 0
    page_size = 10
    
    while app_state.is_running:
        try:
            async with app_state.db_pool.acquire() as conn:
                stats = await conn.fetch("""
                    SELECT c.chat_id, c.title, COUNT(m.message_id) as message_count,
                           MAX(m.date) as last_message, 
                           COUNT(DISTINCT m.sender_id) as unique_senders,
                           c.participants_count
                    FROM chats c
                    LEFT JOIN messages m ON c.chat_id = m.chat_id
                    WHERE c.is_active = TRUE
                    GROUP BY c.chat_id, c.title, c.participants_count
                    ORDER BY message_count DESC
                    LIMIT $1 OFFSET $2
                """, page_size, page * page_size)
                
                total = await conn.fetchval("""
                    SELECT COUNT(DISTINCT c.chat_id)
                    FROM chats c
                    WHERE c.is_active = TRUE
                """)

            if not stats:
                print_message("Нет данных для анализа.", level="warning")
                return

            table_data = []
            for row in stats:
                last_msg = row['last_message'].strftime('%Y-%m-%d %H:%M') if row['last_message'] else 'Нет'
                members = row['participants_count'] or '?'
                table_data.append([
                    row['chat_id'],
                    (row['title'][:30] + '...') if row['title'] and len(row['title']) > 30 else row['title'] or 'Без названия',
                    row['message_count'],
                    f"{row['unique_senders']}/{members}",
                    last_msg
                ])

            total_pages = (total + page_size - 1) // page_size
            print(f"{Fore.CYAN}Статистика по активным чатам (Страница {page + 1} из {total_pages}):")
            print(tabulate(table_data, 
                          headers=["ID", "Название", "Сообщений", "Авторов/Участников", "Последнее"], 
                          tablefmt="grid"))

            if page + 1 >= total_pages:
                break

            next_page = await confirm("Показать следующую страницу?").ask_async()
            if not next_page:
                break
            page += 1
            
        except Exception as e:
            logging.error(f"Error in show_chat_stats: {e}")
            print_message(f"Ошибка: {e}", level="error")
            break


async def show_keyword_analysis():
    """Показ анализа ключевых слов."""
    page = 0
    page_size = 10
    
    while app_state.is_running:
        try:
            async with app_state.db_pool.acquire() as conn:
                keywords = await conn.fetch("""
                    SELECT m.tags->'keywords' as keywords, 
                           m.chat_id, 
                           c.title, 
                           m.date,
                           m.message_id
                    FROM messages m
                    JOIN chats c ON m.chat_id = c.chat_id
                    WHERE m.tags ? 'keywords' 
                      AND jsonb_array_length(m.tags->'keywords') > 0
                      AND c.is_active = TRUE
                    ORDER BY m.date DESC
                    LIMIT $1 OFFSET $2
                """, page_size, page * page_size)
                
                total = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM messages m
                    JOIN chats c ON m.chat_id = c.chat_id
                    WHERE m.tags ? 'keywords' 
                      AND jsonb_array_length(m.tags->'keywords') > 0
                      AND c.is_active = TRUE
                """)

            if not keywords:
                print_message("Ключевые слова не найдены.", level="warning")
                return

            table_data = []
            for row in keywords:
                kw_list = row['keywords']
                if kw_list:
                    kw_str = ', '.join(kw_list[:5])
                    if len(kw_list) > 5:
                        kw_str += f" ... (+{len(kw_list)-5})"
                    
                    table_data.append([
                        row['message_id'],
                        row['chat_id'],
                        (row['title'][:20] + '...') if row['title'] and len(row['title']) > 20 else row['title'] or '?',
                        kw_str[:50] + ('...' if len(kw_str) > 50 else ''),
                        row['date'].strftime('%Y-%m-%d %H:%M')
                    ])

            if not table_data:
                print_message("Ключевые слова не найдены.", level="warning")
                return

            total_pages = (total + page_size - 1) // page_size
            print(f"{Fore.CYAN}Последние ключевые слова (Страница {page + 1} из {total_pages}):")
            print(tabulate(table_data, 
                          headers=["Msg ID", "Chat ID", "Название", "Ключевые слова", "Дата"], 
                          tablefmt="grid"))

            if page + 1 >= total_pages:
                break

            next_page = await confirm("Показать следующую страницу?").ask_async()
            if not next_page:
                break
            page += 1
            
        except Exception as e:
            logging.error(f"Error in show_keyword_analysis: {e}")
            print_message(f"Ошибка: {e}", level="error")
            break


async def show_ner_analysis():
    """Показ анализа именованных сущностей."""
    page = 0
    page_size = 10
    
    while app_state.is_running:
        try:
            async with app_state.db_pool.acquire() as conn:
                ner_data = await conn.fetch("""
                    SELECT m.tags->'ner' as ner, 
                           m.chat_id, 
                           c.title, 
                           m.date,
                           m.message_id
                    FROM messages m
                    JOIN chats c ON m.chat_id = c.chat_id
                    WHERE m.tags ? 'ner'
                      AND c.is_active = TRUE
                    ORDER BY m.date DESC
                    LIMIT $1 OFFSET $2
                """, page_size, page * page_size)
                
                total = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM messages m
                    JOIN chats c ON m.chat_id = c.chat_id
                    WHERE m.tags ? 'ner'
                      AND c.is_active = TRUE
                """)

            if not ner_data:
                print_message("Именованные сущности не найдены.", level="warning")
                return

            table_data = []
            for row in ner_data:
                ner = row['ner']
                if not (ner.get('person') or ner.get('organization') or ner.get('location')):
                    continue
                    
                persons = ', '.join(ner.get('person', [])[:3])
                if len(ner.get('person', [])) > 3:
                    persons += f" ... (+{len(ner['person'])-3})"
                    
                orgs = ', '.join(ner.get('organization', [])[:3])
                if len(ner.get('organization', [])) > 3:
                    orgs += f" ... (+{len(ner['organization'])-3})"
                    
                locs = ', '.join(ner.get('location', [])[:3])
                if len(ner.get('location', [])) > 3:
                    locs += f" ... (+{len(ner['location'])-3})"
                
                table_data.append([
                    row['message_id'],
                    row['chat_id'],
                    (row['title'][:15] + '...') if row['title'] and len(row['title']) > 15 else row['title'] or '?',
                    persons[:20] or '-',
                    orgs[:20] or '-',
                    locs[:20] or '-',
                    row['date'].strftime('%Y-%m-%d %H:%M')
                ])

            if not table_data:
                print_message("Именованные сущности с данными не найдены.", level="warning")
                return

            total_pages = (total + page_size - 1) // page_size
            print(f"{Fore.CYAN}Именованные сущности (NER) (Страница {page + 1} из {total_pages}):")
            print(tabulate(table_data, 
                          headers=["Msg ID", "Chat ID", "Название", "Персоны", "Организации", "Локации", "Дата"], 
                          tablefmt="grid"))

            if page + 1 >= total_pages:
                break

            next_page = await confirm("Показать следующую страницу?").ask_async()
            if not next_page:
                break
            page += 1
            
        except Exception as e:
            logging.error(f"Error in show_ner_analysis: {e}")
            print_message(f"Ошибка: {e}", level="error")
            break


async def show_s3_stats():
    """Показ статистики S3 загрузок."""
    try:
        async with app_state.db_pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(*) FILTER (WHERE uploaded = TRUE) as uploaded_files,
                    COUNT(*) FILTER (WHERE uploaded = FALSE) as pending_files,
                    COUNT(*) FILTER (WHERE s3_key = 'MISSING') as missing_files,
                    COUNT(*) FILTER (WHERE s3_key = 'FAILED') as failed_files,
                    COALESCE(SUM(file_size) FILTER (WHERE uploaded = TRUE), 0) as uploaded_bytes,
                    COALESCE(SUM(file_size) FILTER (WHERE uploaded = FALSE), 0) as pending_bytes,
                    MAX(uploaded_at) as last_upload,
                    COUNT(DISTINCT file_type) as type_count
                FROM media_files
            """)
            
            if not stats:
                print_message("Нет данных о медиафайлах.", level="warning")
                return

            total = stats['total_files'] or 0
            uploaded = stats['uploaded_files'] or 0
            pending = stats['pending_files'] or 0
            missing = stats['missing_files'] or 0
            failed = stats['failed_files'] or 0
            
            uploaded_gb = stats['uploaded_bytes'] / (1024**3)
            pending_gb = stats['pending_bytes'] / (1024**3)
            
            upload_pct = (uploaded / total * 100) if total > 0 else 0
            
            print(f"{Fore.CYAN}=== S3 Upload Statistics ===")
            print(f"Total files: {total}")
            print(f"Uploaded to S3: {uploaded} ({upload_pct:.1f}%)")
            print(f"Pending upload: {pending}")
            print(f"Missing files: {missing}")
            print(f"Failed files: {failed}")
            print(f"Uploaded data: {uploaded_gb:.2f} GB")
            print(f"Pending data: {pending_gb:.2f} GB")
            print(f"Last upload: {stats['last_upload'] or 'Never'}")
            
            # Детали по типам файлов
            by_type = await conn.fetch("""
                SELECT file_type, 
                       COUNT(*) as count, 
                       SUM(file_size) as total_bytes,
                       COUNT(*) FILTER (WHERE uploaded = TRUE) as uploaded_count
                FROM media_files
                GROUP BY file_type
                ORDER BY count DESC
                LIMIT 10
            """)
            
            if by_type:
                print(f"\n{Fore.CYAN}Files by type:")
                type_data = []
                for row in by_type:
                    type_data.append([
                        row['file_type'] or 'unknown',
                        row['count'],
                        f"{row['uploaded_count']}/{row['count']}",
                        f"{row['total_bytes']/(1024**2):.1f} MB" if row['total_bytes'] else '0 MB'
                    ])
                print(tabulate(type_data, headers=["Type", "Count", "Uploaded", "Size"], tablefmt="grid"))
                
    except Exception as e:
        logging.error(f"Error getting S3 stats: {e}")
        print_message(f"Error getting S3 stats: {e}", level="error")


# ==================== Главное меню ====================

async def main_menu():
    """Главное меню программы."""
    
    menu_options = [
        {"name": "1. Инициализация БД", "value": "1"},
        {"name": "2. Запуск клиента", "value": "2"},
        {"name": f"3. Запись в БД [{'ON' if app_state.recorder and app_state.recorder.is_recording else 'OFF'}]", "value": "3"},
        {"name": "4. Список записи", "value": "4"},
        {"name": "5. Импорт в БД", "value": "5"},
        {"name": "6. Просмотр и поиск", "value": "6"},
        {"name": "7. Визуализация", "value": "7"},
        {"name": "8. Запрос SQL", "value": "8"},
        {"name": "9. Управление списками", "value": "9"},
        {"name": "10. Управление настройками", "value": "10"},
        {"name": "11. Статистика S3", "value": "11"},
        {"name": "12. Сохранить и выйти", "value": "12"},
        {"name": "13. Полное завершение", "value": "13"}
    ]

    s3_status = "ON" if app_state.s3_uploader else "OFF"
    menu_options[10]["name"] = f"11. Статистика S3 [S3:{s3_status}]"

    print_message("✅ Программа запущена. Используйте меню для управления.", level="info")
    logging.info("Main menu started")

    while app_state.is_running:
        try:
            if not hasattr(main_menu, 'counter'):
                main_menu.counter = 0
            main_menu.counter += 1
            
            if main_menu.counter % 10 == 0 and app_state.db_pool and not app_state.db_pool._closed:
                stats = await get_pool_stats(app_state.db_pool)
                logging.debug(f"Pool stats: {stats}")

            recording_status = 'ON' if (app_state.recorder and app_state.recorder.is_recording) else 'OFF'
            menu_options[2]["name"] = f"3. Запись в БД [{recording_status}]"

            choice = await select("Главное меню:", choices=menu_options).ask_async()
            
            if not app_state.is_running:
                break

            if choice == "1":
                await init_db()
                if app_state.db_pool:
                    await migrate_users_to_gray_list(app_state.db_pool)
                
            elif choice == "2":
                if not app_state.client_initialized:
                    client = await start_telegram_client()
                    if client:
                        app_state.client = client
                        app_state.client_initialized = True
                        app_state.recorder = RealtimeRecorder(
                            client, 
                            app_state.db_pool, 
                            s3_uploader=app_state.s3_uploader
                        )
                        print_message("✅ Telegram-клиент успешно запущен.", level="info")
                        logging.info("Telegram client started")
                    else:
                        print_message("❌ Не удалось запустить Telegram-клиент.", level="error")
                else:
                    print_message("⚠️ Клиент уже запущен.", level="warning")
                    
            elif choice == "3":
                if not app_state.client_initialized:
                    print_message("❌ Сначала запустите клиент (опция 2).", level="error")
                    continue
                    
                if not app_state.recorder:
                    print_message("❌ Recorder не инициализирован.", level="error")
                    continue
                    
                print_message("ℹ️ Примечание: ошибки 'Could not find the input entity' для каналов являются нормальными", level="warning")
                await app_state.recorder.recording_menu()
                
            elif choice == "4":
                if not app_state.client_initialized:
                    print_message("❌ Сначала запустите клиент (опция 2).", level="error")
                    continue
                    
                await manage_sources_menu(
                    app_state.client, 
                    app_state.recorder, 
                    app_state.db_pool
                )
                
            elif choice == "5":
                if not app_state.client_initialized:
                    print_message("❌ Сначала запустите клиент (опция 2).", level="error")
                    continue
                    
                await import_to_db_menu(app_state.client, app_state.db_pool)
                
            elif choice == "6":
                await search_and_view_menu()
                
            elif choice == "7":
                try:
                    from visualization import visualization_menu
                    await visualization_menu(app_state.client)
                except ImportError:
                    print_message("❌ Модуль визуализации не найден.", level="error")
                    
            elif choice == "8":
                if app_state.db_pool and not app_state.db_pool._closed:
                    await execute_sql_query(app_state.db_pool)
                else:
                    print_message("❌ Нет соединения с БД.", level="error")
                
            elif choice == "9":
                if app_state.db_pool and not app_state.db_pool._closed:
                    await manage_lists_menu(app_state.db_pool)
                else:
                    print_message("❌ Нет соединения с БД.", level="error")
                
            elif choice == "10":
                if app_state.db_pool and not app_state.db_pool._closed:
                    await manage_settings_menu(app_state.db_pool)
                else:
                    print_message("❌ Нет соединения с БД.", level="error")
                
            elif choice == "11":
                if app_state.db_pool and not app_state.db_pool._closed:
                    await show_s3_stats()
                else:
                    print_message("❌ Нет соединения с БД.", level="error")
                
            elif choice == "12":
                exit_confirm = await confirm("Свернуть программу в фоновый режим? (запись продолжится)").ask_async()
                if exit_confirm:
                    print_message("📱 Программа продолжает работу в фоновом режиме.", level="info")
                    print_message("💡 Используйте 'screen -r kaliapp' чтобы вернуться.", level="info")
                    print_message("💡 Для остановки: pkill -f 'python3.9.*main.py'", level="info")
                    
                    await app_state.save()
                    
                    app_state.is_running = False
                    break
                else:
                    print_message("Возврат в меню.", level="info")          
                              
            elif choice == "13":
                exit_confirm = await confirm("Полное завершение программы?").ask_async()
                if exit_confirm:
                    print_message("🛑 Завершение работы...", level="warning")
                    await app_state.cleanup()
                    print_message("👋 Программа завершена.", level="info")
                    sys.exit(0)
                else:
                    print_message("Возврат в меню.", level="info")
            else:
                print_message("❌ Неверный выбор.", level="error")

        except KeyboardInterrupt:
            print_message("\n⚠️ Операция прервана (Ctrl+C).", level="warning")
            exit_now = await confirm("Выйти из программы?").ask_async()
            if exit_now:
                await app_state.cleanup()
                sys.exit(0)
            else:
                print_message("Продолжаем работу...", level="info")
                
        except Exception as e:
            logging.error(f"Ошибка в главном меню: {e}", exc_info=True)
            print_message(f"❌ Ошибка: {e}", level="error")
            await asyncio.sleep(1)


# ==================== Запуск фоновых задач ====================

async def start_background_tasks():
    """Запуск фоновых обработчиков очередей."""
    try:
        if not app_state.db_pool or app_state.db_pool._closed:
            logging.error("Cannot start background tasks: no DB pool")
            return False
            
        app_state.queue_task = asyncio.create_task(
            process_queue(app_state.db_pool)
        )
        
        app_state.http_queue_task = asyncio.create_task(
            process_http_queue(app_state.db_pool)
        )
        
        logging.info("Background tasks started")
        print_message("✅ Фоновые обработчики очередей запущены.", level="info")
        return True
        
    except Exception as e:
        logging.error(f"Error starting background tasks: {e}")
        print_message(f"❌ Ошибка запуска фоновых задач: {e}", level="error")
        return False


# ==================== Точка входа ====================

async def async_main():
    """Основная асинхронная функция."""
    parser = argparse.ArgumentParser(description="KaliApp Telegram Monitor")
    parser.add_argument("--auto", action="store_true", help="Автоматический режим (не используется)")
    parser.add_argument("--no-s3", action="store_true", help="Отключить загрузку в S3")
    parser.add_argument("--reset-state", action="store_true", help="Сбросить сохраненное состояние")
    args = parser.parse_args()

    setup_logging()
    
    print(f"{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}KaliApp Telegram Monitor v1.0")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

    if args.reset_state:
        state_file = Path("state.json")
        if state_file.exists():
            state_file.unlink()
            print_message("🧹 Состояние сброшено.", level="info")

    await app_state.load()

    try:
        app_state.db_pool = await create_db_pool()
        print_message(f"📊 Пул соединений создан: min={app_state.db_pool._minsize}, max={app_state.db_pool._maxsize}", level="info")
    except Exception as e:
        print_message(f"❌ Ошибка создания пула: {e}", level="error")
        return

    try:
        await init_db()
        if app_state.db_pool:
            await migrate_users_to_gray_list(app_state.db_pool)
        print_message("✅ База данных инициализирована.", level="info")
    except Exception as e:
        print_message(f"❌ Ошибка инициализации БД: {e}", level="error")

    await start_background_tasks()

    if not args.no_s3 and os.getenv('S3_BUCKET') and os.getenv('S3_PUBLIC_URL'):
        try:
            app_state.s3_uploader = S3Uploader(
                pool=app_state.db_pool,
                bucket_name=os.getenv('S3_BUCKET'),
                endpoint_url=os.getenv('S3_ENDPOINT', 'https://s3.yandexcloud.net'),
                public_url_base=os.getenv('S3_PUBLIC_URL'),
                region=os.getenv('S3_REGION', 'ru-central-1'),
                upload_on_finish=os.getenv('S3_UPLOAD_ON_FINISH', 'true').lower() == 'true',
                max_queue_size=int(os.getenv('S3_MAX_QUEUE_SIZE', '1000')),
                dedup_ttl=int(os.getenv('S3_DEDUP_TTL', '300'))
            )
            await app_state.s3_uploader.start()
            print_message("✅ S3 uploader started", level="info")
            
            import db_module
            db_module._global_s3_uploader = app_state.s3_uploader
            
            app_state.s3_recovery = S3Recovery(
                pool=app_state.db_pool,
                s3_uploader=app_state.s3_uploader,
                batch_size=int(os.getenv('S3_RECOVERY_BATCH_SIZE', '50'))
            )
            await app_state.s3_recovery.start()
            print_message("✅ S3 recovery worker started", level="info")
            
        except Exception as e:
            print_message(f"❌ Error starting S3 uploader: {e}", level="error")
            logging.error(f"Failed to start S3 uploader: {e}")
    else:
        if args.no_s3:
            print_message("⚠️ S3 upload disabled by --no-s3 flag", level="warning")
        else:
            print_message("⚠️ S3 not configured (set S3_BUCKET and S3_PUBLIC_URL in .env)", level="warning")
        
        import db_module
        db_module._global_s3_uploader = None

    if app_state.client_initialized and not app_state.client:
        print_message("🔄 Восстановление Telegram клиента...", level="info")
        client = await start_telegram_client()
        if client:
            app_state.client = client
            app_state.recorder = RealtimeRecorder(
                client, 
                app_state.db_pool, 
                s3_uploader=app_state.s3_uploader
            )
            print_message("✅ Клиент восстановлен.", level="info")
        else:
            app_state.client_initialized = False
            print_message("⚠️ Не удалось восстановить клиент.", level="warning")

    try:
        await main_menu()
    except Exception as e:
        logging.error(f"Critical error in main menu: {e}", exc_info=True)
        print_message(f"❌ Критическая ошибка: {e}", level="error")
    finally:
        if app_state.is_running:
            await app_state.cleanup()


def main():
    """Синхронная точка входа."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if sys.platform.startswith('freebsd'):
        import asyncio
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(async_main())
    except KeyboardInterrupt:
        pass
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        
        loop.close()
        print(f"{Fore.GREEN}👋 Программа завершена.{Style.RESET_ALL}")


if __name__ == '__main__':
    main()