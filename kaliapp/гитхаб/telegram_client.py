from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, RPCError
from telethon.tl.types import PeerChannel, PeerChat, PeerUser, Chat, User, Channel, ChannelForbidden
from tabulate import tabulate
from colorama import Fore, Style
from dotenv import load_dotenv
from questionary import select
import os
import logging
import asyncpg
import questionary
from datetime import datetime, timedelta
import pytz
from prompt_toolkit import PromptSession
from pathlib import Path  # ДОБАВЛЕНО: для работы с путями
from db_module import (
    db_config, add_chat_to_db, add_user_to_db, add_message_to_db, 
    ensure_partition_for_date, normalize_chat_id, add_chat_to_list,
    print_message, get_chat_list
)

load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# ==================== Управление источниками ====================

async def manage_sources_menu(client, recorder=None, pool=None):
    """
    Подменю управления источниками.
    
    Args:
        client: Telegram client
        recorder: экземпляр RealtimeRecorder (опционально)
        pool: пул соединений с БД (обязательный параметр)
    """
    if pool is None:
        raise ValueError("manage_sources_menu requires a connection pool")
    
    menu_options = [
        {"name": "1. View and manage list", "value": "1"},
        {"name": "2. Add by ID", "value": "2"},
        {"name": "3. Sync with Telegram", "value": "3"},
        {"name": "4. Return to main menu", "value": "4"}
    ]

    while True:
        try:
            # Получаем время последней синхронизации
            async with pool.acquire() as conn:
                last_sync = await conn.fetchval("SELECT MAX(updated_at) FROM chats")
            print(f"{Fore.CYAN}Last sync: {last_sync or 'Never'}")

            choice = await select("Source management:", choices=menu_options).ask_async()

            if choice == "1":
                await view_and_manage_chats(client, pool, recorder)
                
            elif choice == "2":
                await add_chat_by_id(client, pool)
                
            elif choice == "3":
                async with pool.acquire() as conn:
                    await sync_chats_with_db(client, conn, pool)
                print(f"{Fore.GREEN}Chat synchronization completed.")
                
            elif choice == "4":
                print(f"{Fore.CYAN}Returning to main menu...")
                break
            else:
                print(f"{Fore.RED}Invalid choice.")

        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}Operation interrupted (Ctrl+C). Returning to submenu.")
        except Exception as e:
            logging.error(f"Error in source management submenu: {e}")
            print(f"{Fore.RED}Error: {e}")

async def view_and_manage_chats(client, pool, recorder=None):
    """Просмотр и управление списком чатов."""
    try:
        async with pool.acquire() as conn:
            # Получаем активные чаты
            chats = await conn.fetch(
                """
                SELECT c.chat_id, c.title, c.type, c.username, c.access_type, 
                       c.is_active, c.participants_count, c.updated_at, cl.list_type,
                       c.slow_mode_seconds, c.ttl_period, c.folder_name,
                       c.join_request, c.signatures, c.has_geo
                FROM chats c
                LEFT JOIN chat_lists cl ON c.chat_id = cl.chat_id
                WHERE c.is_active = TRUE
                ORDER BY c.title
                """
            )

            # Получаем неактивные чаты
            inactive_chats = await conn.fetch(
                """
                SELECT c.chat_id, c.title, c.type, cl.list_type
                FROM chats c
                LEFT JOIN chat_lists cl ON c.chat_id = cl.chat_id
                WHERE c.is_active = FALSE
                ORDER BY c.title
                """
            )

        if not chats and not inactive_chats:
            print(f"{Fore.YELLOW}No sources in database. Add via 'Add by ID' option.")
            return

        # Формируем данные для таблицы
        chat_data = []
        for row in chats:
            flags = []
            if row['slow_mode_seconds']:
                flags.append(f"slow:{row['slow_mode_seconds']}")
            if row['ttl_period']:
                flags.append(f"ttl:{row['ttl_period']}")
            if row['join_request']:
                flags.append("join_req")
            if row['signatures']:
                flags.append("sign")
            if row['has_geo']:
                flags.append("geo")
            if row['folder_name']:
                flags.append(f"folder:{row['folder_name']}")
            flags_str = ", ".join(flags) if flags else "-"
            
            chat_data.append([
                f"{Fore.GREEN}[ACTIVE]{Style.RESET_ALL}",
                f"{Fore.LIGHTBLACK_EX}{row['chat_id']}{Style.RESET_ALL}",
                row['title'][:30] + ('...' if len(row['title'] or '') > 30 else ''),
                row['type'],
                f"{Fore.RED}@{row['username']}{Style.RESET_ALL}" if row['username'] else 'None',
                f"{Fore.YELLOW}private{Style.RESET_ALL}" if row['access_type'] == 'private' else f"{Fore.CYAN}public{Style.RESET_ALL}",
                str(row['participants_count'] or 0),
                row['list_type'] or 'gray',
                flags_str,
                row['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if row['updated_at'] else 'Never'
            ])
        
        print(f"{Fore.CYAN}Active chats list:")
        print(tabulate(chat_data, headers=[
            f"{Fore.CYAN}Status{Style.RESET_ALL}",
            f"{Fore.CYAN}ID{Style.RESET_ALL}",
            f"{Fore.CYAN}Title{Style.RESET_ALL}",
            f"{Fore.CYAN}Type{Style.RESET_ALL}",
            f"{Fore.CYAN}Username{Style.RESET_ALL}",
            f"{Fore.CYAN}Access{Style.RESET_ALL}",
            f"{Fore.CYAN}Members{Style.RESET_ALL}",
            f"{Fore.CYAN}List{Style.RESET_ALL}",
            f"{Fore.CYAN}Flags{Style.RESET_ALL}",
            f"{Fore.CYAN}Last Update{Style.RESET_ALL}"
        ], tablefmt="grid"))

        # Формируем список для выбора
        choices = []
        for chat in chats:
            title = chat['title'][:30] + ('...' if len(chat['title'] or '') > 30 else '')
            list_type = chat['list_type'] or 'gray'
            choices.append({
                "name": f"[ACTIVE] {title} (ID: {chat['chat_id']}, Type: {chat['type']}, List: {list_type})",
                "value": chat['chat_id']
            })
        
        for chat in inactive_chats:
            title = chat['title'][:30] + ('...' if len(chat['title'] or '') > 30 else '')
            list_type = chat['list_type'] or 'gray'
            choices.append({
                "name": f"[INACTIVE] {title} (ID: {chat['chat_id']}, Type: {chat['type']}, List: {list_type})",
                "value": chat['chat_id']
            })
        choices.append({"name": "Back", "value": None})

        selected_chat_id = await questionary.select(
            "Select source to manage or go back:", 
            choices=choices
        ).ask_async()
        
        if selected_chat_id is None:
            print(f"{Fore.YELLOW}Returning to source management submenu.")
            return

        # Показываем детальную информацию о выбранном чате
        await show_chat_details(client, pool, selected_chat_id, recorder)

    except Exception as e:
        logging.error(f"Error viewing chats: {e}")
        print(f"{Fore.RED}Error: {e}")

async def show_chat_details(client, pool, chat_id, recorder=None):
    """Показ детальной информации о чате и меню действий."""
    try:
        async with pool.acquire() as conn:
            chat_info = await conn.fetchrow(
                """
                SELECT c.is_active, c.title, c.type, c.username, c.participants_count,
                       cl.list_type, c.slow_mode_seconds, c.ttl_period, c.folder_name,
                       c.join_request, c.signatures, c.has_geo, c.restrictions,
                       c.description, c.photo_id, c.linked_chat_id, c.migrated_to,
                       c.migrated_from, c.sticker_set_name, c.can_set_sticker_set,
                       c.min_rank, c.banned_rights, c.default_banned_rights,
                       c.join_to_send, c.geo_point, c.address, c.folder_id,
                       c.folder_order, c.folder_included, c.folder_pinned,
                       c.created_at, c.updated_at
                FROM chats c
                LEFT JOIN chat_lists cl ON c.chat_id = cl.chat_id
                WHERE c.chat_id = $1
                """,
                chat_id
            )

        if not chat_info:
            print(f"{Fore.RED}Chat with ID {chat_id} not found.")
            return

        # Форматируем информацию для отображения
        print(f"{Fore.CYAN}Chat details for ID {chat_id}:")
        details = [
            ["Title", chat_info['title']],
            ["Type", chat_info['type']],
            ["Username", chat_info['username'] or 'None'],
            ["Members", str(chat_info['participants_count'] or 0)],
            ["Current list", chat_info['list_type'] or 'gray'],
            ["Active", "Yes" if chat_info['is_active'] else "No"],
            ["Slow mode", f"{chat_info['slow_mode_seconds']}s" if chat_info['slow_mode_seconds'] else 'Disabled'],
            ["Auto-delete TTL", f"{chat_info['ttl_period']}s" if chat_info['ttl_period'] else 'Disabled'],
            ["Folder", chat_info['folder_name'] or 'None'],
            ["Join request", "Yes" if chat_info['join_request'] else "No"],
            ["Signatures", "Yes" if chat_info['signatures'] else "No"],
            ["Has geo", "Yes" if chat_info['has_geo'] else "No"],
            ["Description", (chat_info['description'][:50] + '...') if chat_info['description'] and len(chat_info['description']) > 50 else chat_info['description'] or 'None'],
            ["Created at", chat_info['created_at'].strftime('%Y-%m-%d %H:%M:%S') if chat_info['created_at'] else 'Unknown'],
            ["Updated at", chat_info['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if chat_info['updated_at'] else 'Unknown']
        ]
        print(tabulate(details, tablefmt="grid"))

        # Меню действий
        is_active = chat_info['is_active']
        current_list_type = chat_info['list_type'] or 'gray'

        chat_actions = [
            {"name": "Activate for recording" if not is_active else "Deactivate for recording", "value": "toggle"},
            {"name": f"Change list (current: {current_list_type})", "value": "change_list"},
            {"name": "Delete source", "value": "delete"},
            {"name": "Import messages", "value": "import"},
            {"name": "Back", "value": "back"}
        ]
        
        action = await questionary.select(
            f"Actions for ID {chat_id}:", 
            choices=chat_actions
        ).ask_async()

        if action == "toggle":
            await toggle_chat_active_status(client, chat_id, pool, recorder)
        elif action == "change_list":
            await change_chat_list(pool, chat_id)
        elif action == "delete":
            await delete_chat_source(client, chat_id, pool, recorder)
        elif action == "import":
            await import_chat_messages(client, chat_id, pool)
        elif action == "back":
            print(f"{Fore.YELLOW}Returning to chat list.")
            return

    except Exception as e:
        logging.error(f"Error showing chat details: {e}")
        print(f"{Fore.RED}Error: {e}")

async def change_chat_list(pool, chat_id):
    """Изменение списка чата."""
    try:
        new_list_type = await questionary.select(
            "Select list type:",
            choices=[
                {"name": "White", "value": "white"},
                {"name": "Black", "value": "black"},
                {"name": "Gray", "value": "gray"}
            ]
        ).ask_async()

        async with pool.acquire() as conn:
            from db_module import add_chat_to_list
            await add_chat_to_list(conn, chat_id, new_list_type)
            
        print(f"{Fore.GREEN}Chat {chat_id} moved to {new_list_type} list.")
        
    except Exception as e:
        logging.error(f"Error changing chat list: {e}")
        print(f"{Fore.RED}Error: {e}")

async def delete_chat_source(client, chat_id, pool, recorder=None):
    """Удаление источника (чата)."""
    try:
        async with pool.acquire() as conn:
            # Проверяем, есть ли сообщения
            messages_exist = await conn.fetchval(
                "SELECT 1 FROM messages WHERE chat_id = $1 LIMIT 1", 
                chat_id
            )
            
            if messages_exist:
                # Если есть сообщения, только деактивируем
                await conn.execute(
                    "UPDATE chats SET is_active = FALSE WHERE chat_id = $1", 
                    chat_id
                )
                print(f"{Fore.YELLOW}Chat {chat_id} contains messages and marked as inactive.")
                logging.info(f"Chat {chat_id} marked as inactive due to existing messages.")
                
                if recorder and recorder.is_recording:
                    recorder.recorded_chats = await recorder.fetch_chats()
                    print(f"{Fore.CYAN}Active chats list updated in RealtimeRecorder.")
            else:
                # Если сообщений нет, удаляем полностью
                await conn.execute("DELETE FROM chats WHERE chat_id = $1", chat_id)
                print(f"{Fore.GREEN}Chat {chat_id} deleted.")
                logging.info(f"Deleted chat: {chat_id}")
                
    except Exception as e:
        logging.error(f"Error deleting chat: {e}")
        print(f"{Fore.RED}Error: {e}")

async def import_chat_messages(client, chat_id, pool):
    """Импорт сообщений из чата."""
    try:
        limit_input = await PromptSession(
            "Enter number of messages to import (default 10): "
        ).prompt_async()
        limit = int(limit_input.strip()) if limit_input.strip().isdigit() else 10
        
        from db_module import import_messages_to_db
        await import_messages_to_db(client, chat_id, limit, pool)
        
    except Exception as e:
        logging.error(f"Error importing messages: {e}")
        print(f"{Fore.RED}Error: {e}")

async def add_chat_by_id(client, pool):
    """Добавление чата по ID."""
    try:
        chat_id_str = await PromptSession(
            "Enter chat ID (integer, e.g., -100123456789): "
        ).prompt_async()
        
        try:
            chat_id = int(chat_id_str)
        except ValueError:
            print(f"{Fore.RED}Chat ID must be a number.")
            return

        try:
            # Получаем сущность из Telegram
            entity = await client.get_entity(chat_id)
            
            # Проверяем доступ
            try:
                await client.get_messages(entity, limit=1)
            except Exception as e:
                print(f"{Fore.RED}No access to chat {chat_id}: {e}")
                logging.error(f"No access to chat {chat_id}: {e}")
                return
            
            # Определяем тип чата
            chat_type = None
            if isinstance(entity, Channel):
                chat_type = 'supergroup' if getattr(entity, 'megagroup', False) else 'channel'
            elif isinstance(entity, Chat):
                chat_type = 'group'
            else:
                print(f"{Fore.RED}Unsupported chat type: {type(entity).__name__}")
                return

            # Получаем информацию о папке
            folder_info = await get_folder_info(client, chat_id)

            # Подготавливаем данные чата
            chat_data = {
                'chat_id': normalize_chat_id(entity.id),
                'type': chat_type,
                'access_type': 'public' if entity.username else 'private',
                'username': entity.username,
                'title': entity.title,
                'description': getattr(entity, 'about', None),
                'participants_count': getattr(entity, 'participants_count', None),
                'photo_id': getattr(entity.photo, 'photo_id', None) if hasattr(entity, 'photo') else None,
                'linked_chat_id': getattr(entity, 'linked_chat_id', None),
                'migrated_to': getattr(entity, 'migrated_to', None),
                'migrated_from': getattr(entity, 'migrated_from', None),
                'sticker_set_name': getattr(entity, 'sticker_set', None),
                'can_set_sticker_set': getattr(entity, 'can_set_stickers', None),
                'slow_mode_seconds': getattr(entity, 'slow_mode_seconds', None),
                'ttl_period': getattr(entity, 'ttl_period', None),
                'join_request': getattr(entity, 'join_request', False),
                'join_to_send': getattr(entity, 'join_to_send', False),
                'signatures': getattr(entity, 'signatures', False),
                'has_geo': getattr(entity, 'has_geo', False),
                'geo_point': getattr(entity, 'geo_point', None),
                'address': getattr(entity, 'address', None),
                'restrictions': getattr(entity, 'restriction_reason', None),
                'folder_id': folder_info.get('folder_id') if folder_info else None,
                'folder_name': folder_info.get('folder_name') if folder_info else None,
                'folder_order': folder_info.get('folder_order') if folder_info else None,
                'folder_included': True if folder_info else False,
                'folder_pinned': folder_info.get('folder_pinned') if folder_info else False,
                'is_active': False
            }

            # Добавляем в БД
            async with pool.acquire() as conn:
                await add_chat_to_db(conn, chat_data)
                
                # Спрашиваем, добавить ли в список
                add_to_list = await questionary.confirm(
                    f"Add chat {chat_data['title']} to list (white/black/gray)?"
                ).ask_async()
                
                if add_to_list:
                    list_type = await questionary.select(
                        "Select list type:",
                        choices=[
                            {"name": "White", "value": "white"},
                            {"name": "Black", "value": "black"},
                            {"name": "Gray", "value": "gray"}
                        ]
                    ).ask_async()
                    await add_chat_to_list(conn, normalize_chat_id(chat_id), list_type)

            print(f"{Fore.GREEN}Chat {chat_data['title']} (ID: {chat_id}) added to DB.")

            # Спрашиваем, активировать ли для записи
            activate = await questionary.confirm(
                f"Activate chat {chat_data['title']} for recording?"
            ).ask_async()
            
            if activate:
                await toggle_chat_active_status(client, normalize_chat_id(chat_id), pool)
                
        except Exception as e:
            print(f"{Fore.RED}Error: Chat with ID {chat_id_str} not found or inaccessible. {e}")
            logging.error(f"Error adding chat ID {chat_id_str}: {e}")
            
    except Exception as e:
        logging.error(f"Error in add_chat_by_id: {e}")
        print(f"{Fore.RED}Error: {e}")

async def get_folder_info(client, chat_id):
    """Получение информации о папке чата."""
    try:
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if dialog.entity.id == chat_id:
                return {
                    'folder_id': getattr(dialog, 'folder_id', None),
                    'folder_name': getattr(dialog, 'folder_name', None),
                    'folder_order': getattr(dialog, 'order', None),
                    'folder_pinned': getattr(dialog, 'pinned', False)
                }
    except Exception as e:
        logging.warning(f"Could not get folder info for {chat_id}: {e}")
    return None

# ==================== Запуск клиента ====================

async def start_telegram_client():
    """Запуск Telegram клиента."""
    client = TelegramClient(
        'session_name',
        api_id,
        api_hash,
        system_version='4.16.30-vxCUSTOM'
    )
    
    try:
        await client.start(phone=phone)
        
        if not await client.is_user_authorized():
            print(f"{Fore.RED}Authorization required. Check SMS or Telegram for code.")
            return None
        
        # Получаем информацию о текущем пользователе
        me = await client.get_me()
        
        # Формируем флаги
        flags = []
        if me.verified:
            flags.append("Verified")
        if me.scam:
            flags.append("Scam")
        if me.fake:
            flags.append("Fake")
        if me.premium:
            flags.append("Premium")
        if me.bot:
            flags.append("Bot")
        flags_str = f" [{', '.join(flags)}]" if flags else ""
        
        # Отображаем информацию
        user_info = [
            ["User ID", f"{Fore.LIGHTBLACK_EX}{me.id}{Style.RESET_ALL}"],
            ["First Name", f"{Fore.CYAN}{me.first_name or 'Not specified'}{Style.RESET_ALL}"],
            ["Last Name", f"{Fore.CYAN}{me.last_name or 'Not specified'}{Style.RESET_ALL}"],
            ["Username", f"{Fore.RED}{me.username or 'Not specified'}{Style.RESET_ALL}"],
            ["Phone", f"{Fore.WHITE}{me.phone or 'Not specified'}{Style.RESET_ALL}"],
            ["Language", f"{Fore.YELLOW}{getattr(me, 'lang_code', 'Not specified')}{Style.RESET_ALL}"],
            ["DC ID", f"{Fore.MAGENTA}{getattr(me, 'dc_id', 'Unknown')}{Style.RESET_ALL}"],
            ["Flags", f"{Fore.GREEN}{flags_str}{Style.RESET_ALL}"],
            ["Status", f"{Fore.BLUE}{me.status.__class__.__name__ if me.status else 'Unknown'}{Style.RESET_ALL}"]
        ]
        
        print(f"{Fore.CYAN}Telegram user information:")
        print(tabulate(user_info, headers=["Parameter", "Value"], tablefmt="grid"))
        print(f"{Fore.GREEN}Telegram client successfully started.")
        logging.info(f"Telegram client started for user ID: {me.id}")
        
        return client
    
    except SessionPasswordNeededError:
        from prompt_toolkit import PromptSession
        password = await PromptSession(
            "Enter two-factor authentication password: ", 
            is_password=True
        ).prompt_async()
        
        try:
            await client.sign_in(password=password)
            print(f"{Fore.GREEN}Two-factor authentication successful.")
            return await start_telegram_client()
        except Exception as e:
            logging.error(f"Two-factor authentication error: {e}")
            print(f"{Fore.RED}Two-factor authentication error: {e}")
            return None
            
    except RPCError as e:
        logging.error(f"RPC Error: {e}")
        print(f"{Fore.RED}RPC Error: {e}")
        if "UPDATE_APP_TO_LOGIN" in str(e):
            print(f"{Fore.YELLOW}1. Check api_id/api_hash at https://my.telegram.org")
            print(f"{Fore.YELLOW}2. Delete session_name.session and try again.")
        return None
        
    except Exception as e:
        logging.error(f"Error starting Telegram client: {e}")
        print(f"{Fore.RED}Error starting Telegram client: {e}")
        return None

# ==================== Синхронизация чатов ====================

async def sync_chats_with_db(client, conn, pool):
    """
    Синхронизация чатов из Telegram с БД.
    
    Args:
        client: Telegram client
        conn: соединение с БД (для текущей транзакции)
        pool: пул соединений (для вложенных операций)
    """
    try:
        print(f"{Fore.CYAN}Starting chat synchronization...")
        
        # Получаем существующие чаты из БД
        existing_chats = await conn.fetch("SELECT chat_id, updated_at FROM chats")
        existing_chat_ids = {normalize_chat_id(row['chat_id']) for row in existing_chats}
        last_updated = {normalize_chat_id(row['chat_id']): row['updated_at'] for row in existing_chats}

        # Получаем чаты из Telegram
        telegram_chats = []
        async for dialog in client.iter_dialogs():
            print(f"{Fore.YELLOW}Processing dialog: ID={dialog.entity.id}, Type={type(dialog.entity).__name__}")
            
            chat_type = None
            if isinstance(dialog.entity, (PeerChannel, Channel)):
                chat_type = 'channel' if dialog.entity.broadcast else 'supergroup'
            elif isinstance(dialog.entity, (PeerChat, Chat)):
                chat_type = 'group'
            elif isinstance(dialog.entity, (PeerUser, User)):
                print(f"{Fore.BLUE}Skipped user: ID={dialog.entity.id}")
                continue

            # Информация о папке
            folder_info = {
                'folder_id': getattr(dialog, 'folder_id', None),
                'folder_name': getattr(dialog, 'folder_name', None),
                'folder_order': getattr(dialog, 'order', None),
                'folder_pinned': getattr(dialog, 'pinned', False)
            }

            chat_data = {
                'chat_id': normalize_chat_id(dialog.entity.id),
                'type': chat_type,
                'access_type': 'private' if dialog.entity.username is None else 'public',
                'username': dialog.entity.username,
                'title': dialog.entity.title,
                'description': getattr(dialog.entity, 'about', None),
                'participants_count': getattr(dialog.entity, 'participants_count', None),
                'photo_id': getattr(dialog.entity.photo, 'photo_id', None) if hasattr(dialog.entity, 'photo') else None,
                'linked_chat_id': getattr(dialog.entity, 'linked_chat_id', None),
                'migrated_to': getattr(dialog.entity, 'migrated_to', None),
                'migrated_from': getattr(dialog.entity, 'migrated_from', None),
                'slow_mode_seconds': getattr(dialog.entity, 'slow_mode_seconds', None),
                'ttl_period': getattr(dialog.entity, 'ttl_period', None),
                'join_request': getattr(dialog.entity, 'join_request', False),
                'signatures': getattr(dialog.entity, 'signatures', False),
                'has_geo': getattr(dialog.entity, 'has_geo', False),
                'restrictions': getattr(dialog.entity, 'restriction_reason', None),
                'folder_id': folder_info['folder_id'],
                'folder_name': folder_info['folder_name'],
                'folder_order': folder_info['folder_order'],
                'folder_included': True,
                'folder_pinned': folder_info['folder_pinned'],
                'is_active': False
            }
            telegram_chats.append(chat_data)

        print(f"{Fore.CYAN}Found {len(telegram_chats)} chats in Telegram.")
        
        # Обновляем БД
        for chat in telegram_chats:
            if chat['chat_id'] not in existing_chat_ids:
                # Новый чат
                await add_chat_to_db(conn, chat)
            elif last_updated.get(chat['chat_id']) < datetime.now().replace(tzinfo=None) - timedelta(days=1):
                # Чат требует обновления
                update_query = """
                    UPDATE chats 
                    SET type = $1, access_type = $2, username = $3, title = $4,
                        description = $5, participants_count = $6, photo_id = $7,
                        linked_chat_id = $8, migrated_to = $9, migrated_from = $10,
                        slow_mode_seconds = $11, ttl_period = $12, join_request = $13,
                        signatures = $14, has_geo = $15, restrictions = $16,
                        folder_id = $17, folder_name = $18, folder_order = $19,
                        folder_included = $20, folder_pinned = $21, updated_at = CURRENT_TIMESTAMP
                    WHERE chat_id = $22
                """
                await conn.execute(
                    update_query,
                    chat['type'], chat['access_type'], chat['username'], chat['title'],
                    chat['description'], chat['participants_count'], chat['photo_id'],
                    chat['linked_chat_id'], chat['migrated_to'], chat['migrated_from'],
                    chat['slow_mode_seconds'], chat['ttl_period'], chat['join_request'],
                    chat['signatures'], chat['has_geo'], chat['restrictions'],
                    chat['folder_id'], chat['folder_name'], chat['folder_order'],
                    chat['folder_included'], chat['folder_pinned'], chat['chat_id']
                )
                logging.info(f"Updated chat: {chat['chat_id']}")
        
        print(f"{Fore.GREEN}Synchronized {len(telegram_chats)} chats with DB.")
        logging.info(f"Synchronized {len(telegram_chats)} chats with DB.")
        
    except Exception as e:
        logging.error(f"Error syncing chats: {e}")
        print(f"{Fore.RED}Error syncing chats: {e}")
        raise

# ==================== Выбор чата ====================

async def select_chat(client, show_all=False, pool=None):
    """
    Выбор чата из списка.
    
    Args:
        client: Telegram client
        show_all: показывать все чаты или только активные
        pool: пул соединений (обязательный параметр)
    """
    if pool is None:
        raise ValueError("select_chat requires a connection pool")
    
    try:
        async with pool.acquire() as conn:
            # Получаем время последней синхронизации
            last_sync = await conn.fetchval("SELECT MAX(updated_at) FROM chats")
            print(f"{Fore.CYAN}Last sync: {last_sync or 'Never'}")
            
            # Получаем список чатов из БД
            query = """
            SELECT c.chat_id, c.title, c.type, c.username, c.access_type, c.is_active, 
                   cl.list_type, c.participants_count, c.slow_mode_seconds, c.ttl_period,
                   c.folder_name
            FROM chats c
            LEFT JOIN chat_lists cl ON c.chat_id = cl.chat_id
            ORDER BY c.is_active DESC, c.title
            """
            chats = await conn.fetch(query)
            
        print(f"{Fore.CYAN}Found {len(chats)} sources in DB.")
        
        # Формируем список для выбора
        choices = []
        for chat in chats:
            title = chat['title'][:30] + ('...' if len(chat['title'] or '') > 30 else '')
            status = "[ACTIVE]" if chat['is_active'] else "[INACTIVE]"
            list_type = chat['list_type'] or 'gray'
            
            flags = []
            if chat['slow_mode_seconds']:
                flags.append(f"slow:{chat['slow_mode_seconds']}")
            if chat['ttl_period']:
                flags.append(f"ttl:{chat['ttl_period']}")
            if chat['folder_name']:
                flags.append(f"📁:{chat['folder_name']}")
            flags_str = f" ({', '.join(flags)})" if flags else ""
            
            choices.append({
                "name": f"{status} {title} (ID: {chat['chat_id']}, Type: {chat['type']}, List: {list_type}){flags_str}",
                "value": {
                    "id": chat['chat_id'],
                    "title": chat['title'],
                    "type": chat['type'],
                    "username": chat['username'],
                    "access_type": chat['access_type']
                }
            })
        
        choices.extend([
            {"name": "Refresh chat list", "value": "refresh"},
            {"name": "Enter chat ID manually", "value": "manual"},
            {"name": "Back", "value": "back"}
        ])

        while True:
            selected = await select("Select chat or enter ID:", choices=choices).ask_async()
            
            if selected == "refresh":
                # Обновляем список чатов
                async with pool.acquire() as conn:
                    await sync_chats_with_db(client, conn, pool)
                    chats = await conn.fetch(query)
                    
                print(f"{Fore.CYAN}Found {len(chats)} chats in DB.")
                
                # Обновляем choices
                choices = []
                for chat in chats:
                    title = chat['title'][:30] + ('...' if len(chat['title'] or '') > 30 else '')
                    status = "[ACTIVE]" if chat['is_active'] else "[INACTIVE]"
                    list_type = chat['list_type'] or 'gray'
                    
                    flags = []
                    if chat['slow_mode_seconds']:
                        flags.append(f"slow:{chat['slow_mode_seconds']}")
                    if chat['ttl_period']:
                        flags.append(f"ttl:{chat['ttl_period']}")
                    if chat['folder_name']:
                        flags.append(f"📁:{chat['folder_name']}")
                    flags_str = f" ({', '.join(flags)})" if flags else ""
                    
                    choices.append({
                        "name": f"{status} {title} (ID: {chat['chat_id']}, Type: {chat['type']}, List: {list_type}){flags_str}",
                        "value": {
                            "id": chat['chat_id'],
                            "title": chat['title'],
                            "type": chat['type'],
                            "username": chat['username'],
                            "access_type": chat['access_type']
                        }
                    })
                choices.extend([
                    {"name": "Refresh chat list", "value": "refresh"},
                    {"name": "Enter chat ID manually", "value": "manual"},
                    {"name": "Back", "value": "back"}
                ])
                continue
            
            if selected == "manual":
                return await handle_manual_chat_input(client, pool)
            
            if selected == "back":
                print(f"{Fore.YELLOW}Returning to source management submenu.")
                return None, None, None
            
            if isinstance(selected, dict):
                chat_id = normalize_chat_id(selected['id'])
                entity = await client.get_entity(chat_id)
                return entity, selected['title'], selected['type']
            else:
                print(f"{Fore.RED}Invalid choice.")
                return None, None, None
                
    except Exception as e:
        logging.error(f"Error selecting source: {e}")
        print(f"{Fore.RED}Error selecting source: {e}")
        return None, None, None

async def handle_manual_chat_input(client, pool):
    """Обработка ручного ввода ID чата."""
    try:
        chat_id_str = await PromptSession(
            "Enter source ID (integer, e.g., -100123456789): "
        ).prompt_async()
        
        try:
            chat_id = int(chat_id_str)
        except ValueError:
            print(f"{Fore.RED}Source ID must be a number.")
            return None, None, None
        
        try:
            entity = await client.get_entity(chat_id)
            
            # Проверяем доступ
            try:
                await client.get_messages(entity, limit=1)
            except Exception as e:
                print(f"{Fore.RED}No access to chat {chat_id}: {e}")
                logging.error(f"No access to chat {chat_id}: {e}")
                return None, None, None
            
            # Определяем тип
            chat_type = None
            if isinstance(entity, Channel):
                chat_type = 'supergroup' if getattr(entity, 'megagroup', False) else 'channel'
            elif isinstance(entity, Chat):
                chat_type = 'group'
            else:
                print(f"{Fore.RED}Unsupported source type: {type(entity).__name__}")
                return None, None, None
            
            # Получаем информацию о папке
            folder_info = await get_folder_info(client, chat_id)
            
            # Добавляем в БД
            chat_data = {
                'chat_id': normalize_chat_id(entity.id),
                'type': chat_type,
                'access_type': 'public' if entity.username else 'private',
                'username': entity.username,
                'title': entity.title,
                'description': getattr(entity, 'about', None),
                'participants_count': getattr(entity, 'participants_count', None),
                'photo_id': getattr(entity.photo, 'photo_id', None) if hasattr(entity, 'photo') else None,
                'linked_chat_id': getattr(entity, 'linked_chat_id', None),
                'migrated_to': getattr(entity, 'migrated_to', None),
                'migrated_from': getattr(entity, 'migrated_from', None),
                'slow_mode_seconds': getattr(entity, 'slow_mode_seconds', None),
                'ttl_period': getattr(entity, 'ttl_period', None),
                'join_request': getattr(entity, 'join_request', False),
                'signatures': getattr(entity, 'signatures', False),
                'has_geo': getattr(entity, 'has_geo', False),
                'restrictions': getattr(entity, 'restriction_reason', None),
                'folder_id': folder_info.get('folder_id') if folder_info else None,
                'folder_name': folder_info.get('folder_name') if folder_info else None,
                'folder_order': folder_info.get('folder_order') if folder_info else None,
                'folder_included': True if folder_info else False,
                'folder_pinned': folder_info.get('folder_pinned') if folder_info else False
            }
            
            async with pool.acquire() as conn:
                await add_chat_to_db(conn, chat_data)
                
                # Спрашиваем, добавить ли в список
                add_to_list = await questionary.confirm(
                    f"Add chat {chat_data['title']} to list (white/black/gray)?"
                ).ask_async()
                
                if add_to_list:
                    list_type = await questionary.select(
                        "Select list type:",
                        choices=[
                            {"name": "White", "value": "white"},
                            {"name": "Black", "value": "black"},
                            {"name": "Gray", "value": "gray"}
                        ]
                    ).ask_async()
                    await add_chat_to_list(conn, normalize_chat_id(chat_id), list_type)
            
            print(f"{Fore.GREEN}Source {chat_data['title']} (ID: {chat_id}) added to DB.")
            return entity, chat_data['title'], chat_type
            
        except Exception as e:
            print(f"{Fore.RED}Error: Source with ID {chat_id_str} not found or inaccessible. {e}")
            logging.error(f"Error adding ID {chat_id_str}: {e}")
            return None, None, None
            
    except Exception as e:
        logging.error(f"Error in manual chat input: {e}")
        print(f"{Fore.RED}Error: {e}")
        return None, None, None

# ==================== Управление статусом чата ====================

async def toggle_chat_active_status(client, chat_id, pool, recorder=None):
    """
    Переключение статуса активности чата и генерация HTML для белых чатов.
    """
    try:
        chat_id = normalize_chat_id(chat_id)
        
        async with pool.acquire() as conn:
            is_active = await conn.fetchval(
                "SELECT is_active FROM chats WHERE chat_id = $1", 
                chat_id
            )
            
            if is_active is None:
                print(f"{Fore.RED}Chat with ID {chat_id} not found in database.")
                return False

            new_status = not is_active
            
            await conn.execute(
                "UPDATE chats SET is_active = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                new_status, chat_id
            )

        if new_status:
            # Импортируем последние сообщения при активации
            from db_module import import_messages_to_db
            
            # Проверяем список чата
            async with pool.acquire() as conn:
                chat_list = await get_chat_list(conn, chat_id)
            
            # Для белых чатов загружаем медиа и ждем S3
            import_media = (chat_list == 'white')
            wait_for_s3 = import_media  # Ждем S3 только для белых чатов
            
            await import_messages_to_db(
                client, 
                chat_id, 
                limit=10, 
                pool=pool,
                import_media=import_media,
                wait_for_s3=wait_for_s3
            )
            
            # === ГЕНЕРАЦИЯ HTML ПРИ АКТИВАЦИИ БЕЛОГО ЧАТА ===
            if chat_list == 'white':
                try:
                    async with pool.acquire() as conn:
                        chat = await conn.fetchrow(
                            "SELECT username, title FROM chats WHERE chat_id = $1", 
                            chat_id
                        )
                        
                        if chat and chat['username']:
                            username = chat['username'].lstrip('@')
                            title = chat['title'] or username
                            
                            # Создаем директорию
                            base_path = Path('/usr/local/kaliapp/channels/RU')
                            chat_path = base_path / username
                            chat_path.mkdir(parents=True, exist_ok=True)
                            
                            # Простой HTML шаблон
                            html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <meta name="mirror:channel-id" content="{chat_id}">
    <meta name="mirror:channel-title" content="{title}">
    <meta name="mirror:channel-username" content="{username}">
    <link rel="stylesheet" href="/channels/core/core.css">
</head>
<body>
  
    <div id="app">
        <div class="tg-header">
            <div class="channel-profile">
                <div class="channel-avatar" id="channelAvatar" title="Click to toggle theme"></div>
                <div class="channel-info">
                    <div class="channel-title-row">
                        <span class="channel-title" id="channelTitle">Loading...</span>
                        <span class="status-dot" id="statusDot"></span>
                    </div>
                    <div class="channel-meta">
                        <span id="channelUsername">@{username}</span>
                        <span class="channel-badge">realtime</span>
                    </div>
                </div>
                <div class="new-posts-badge hidden" id="newPostsBadge">
                    <span class="new-dot"></span>
                    <span id="newPostsCount">0</span> новых
                </div>
            </div>
        </div>

        <div class="feed" id="feed"></div>

        <div id="infiniteScrollTrigger" class="infinite-scroll-trigger">
            ↓ Загрузить ещё
        </div>

        <button class="scroll-top" id="scrollTopBtn" title="Наверх">
            ↑
        </button>

        <div class="lightbox" id="lightbox">
            <button class="lightbox-close" id="lightboxClose">✕</button>
            <div class="lightbox-content" id="lightboxContent"></div>
        </div>

        <div id="toastContainer"></div>
    </div>

    <script src="/channels/core/core.js"></script>
</body>
</html>'''
                            
                            index_path = chat_path / 'index.html'
                            with open(index_path, 'w', encoding='utf-8') as f:
                                f.write(html)
                            
                            print_message(f"✅ HTML page generated for @{username}")
                            logging.info(f"HTML page generated for chat {chat_id} at {index_path}")
                        else:
                            logging.warning(f"Chat {chat_id} has no username, skipping HTML generation")
                            
                except Exception as e:
                    # Логируем ошибку, но не прерываем активацию
                    logging.error(f"HTML generation failed for chat {chat_id}: {e}")
                    print_message(f"Chat activated but HTML generation failed: {e}", level="warning")
            
            print(f"{Fore.GREEN}Chat {chat_id} activated for recording.")
            logging.info(f"Chat {chat_id} activated for recording.")
        else:
            print(f"{Fore.GREEN}Chat {chat_id} deactivated for recording.")
            logging.info(f"Chat {chat_id} deactivated for recording.")

        if recorder and recorder.is_recording:
            recorder.recorded_chats = await recorder.fetch_chats()
            print(f"{Fore.CYAN}Active chats list updated in RealtimeRecorder.")

        return True
        
    except Exception as e:
        logging.error(f"Error changing chat status {chat_id}: {e}")
        print(f"{Fore.RED}Error changing chat status: {e}")
        return False
        
# ==================== Устаревшие функции (заглушки) ====================

async def add_source(client):
    """Устаревшая функция. Используйте add_chat_by_id."""
    print_message("This function is deprecated. Use 'Add by ID' in manage_sources_menu.", level="warning")
    return

async def list_sources(client):
    """Устаревшая функция. Используйте view_and_manage_chats."""
    print_message("This function is deprecated. Use 'View and manage list' in manage_sources_menu.", level="warning")
    return

async def delete_source(client):
    """Устаревшая функция. Используйте управление через view_and_manage_chats."""
    print_message("This function is deprecated. Use chat management in 'View and manage list'.", level="warning")
    return

async def import_source_messages(client):
    """Устаревшая функция. Используйте импорт через manage_sources_menu."""
    print_message("This function is deprecated. Use 'Import messages' in chat management.", level="warning")
    return