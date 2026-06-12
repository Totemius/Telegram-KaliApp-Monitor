# import_module.py
import asyncio
import logging
from colorama import Fore, Style
from questionary import select, text
from prompt_toolkit import PromptSession
from tabulate import tabulate
from telegram_client import select_chat
from db_module import import_messages_to_db, add_chat_to_db, db_config, print_message
from telethon.tl.types import Channel, Chat
import asyncpg

async def import_to_db_menu(client, pool):
    """
    Подменю для управления импортом сообщений в БД.
    
    Args:
        client: Telegram client
        pool: пул соединений с БД (обязательный параметр)
    """
    if pool is None:
        raise ValueError("import_to_db_menu requires a connection pool")
    
    while True:
        try:
            menu_options = [
                {"name": "1. Select source for import", "value": "1"},
                {"name": "2. Enter ID manually", "value": "2"},
                {"name": "3. Batch import from multiple sources", "value": "3"},
                {"name": "4. Back", "value": "4"}
            ]
            
            choice = await select("Import to Database Management:", choices=menu_options).ask_async()

            if choice == "1":
                await import_from_selected_chat(client, pool)
                
            elif choice == "2":
                await import_from_manual_id(client, pool)
                
            elif choice == "3":
                await batch_import_menu(client, pool)
                
            elif choice == "4":
                print_message("Returning to main menu.")
                break
                
            else:
                print_message("Invalid choice.", level="error")

        except KeyboardInterrupt:
            print_message("Operation interrupted (Ctrl+C). Returning to import submenu.", level="warning")
            logging.info("User interrupted operation in import submenu")
        except Exception as e:
            logging.error(f"Error in import submenu: {e}")
            print_message(f"Error: {e}", level="error")

async def import_from_selected_chat(client, pool):
    """
    Импорт из выбранного чата.
    
    Args:
        client: Telegram client
        pool: пул соединений с БД
    """
    try:
        # Выбор чата с использованием select_chat
        entity, title, chat_type = await select_chat(client, show_all=True, pool=pool)
        
        if not entity:
            print_message("No chat selected. Returning to import submenu.", level="warning")
            return

        # Показываем информацию о выбранном чате
        await display_chat_info(entity, title, chat_type)

        # Проверяем, существует ли чат в БД
        async with pool.acquire() as conn:
            chat_exists = await conn.fetchval(
                "SELECT 1 FROM chats WHERE chat_id = $1",
                entity.id
            )
            
            if not chat_exists:
                print_message(f"Chat {title} not found in database. Adding...", level="warning")
                await add_chat_to_database(client, entity, chat_type, pool)

        # Запрашиваем параметры импорта
        limit = await get_import_limit()
        import_media = await get_import_media_option()
        import_reactions = await get_import_reactions_option()

        # Запуск импорта
        print_message(f"Starting import of {limit} messages from {title} (ID: {entity.id})...")
        
        count = await import_messages_to_db(
            client, 
            entity.id, 
            limit, 
            pool=pool,
            import_media=import_media,
            import_reactions=import_reactions
        )
        
        print_message(f"Imported {count} new messages from {title} (ID: {entity.id}).")
        logging.info(f"User imported {count} messages from {entity.id}")
        
    except Exception as e:
        logging.error(f"Error in import_from_selected_chat: {e}")
        print_message(f"Import error: {e}", level="error")

async def import_from_manual_id(client, pool):
    """
    Импорт из чата по ручному вводу ID.
    
    Args:
        client: Telegram client
        pool: пул соединений с БД
    """
    try:
        # Ввод ID чата
        chat_id_str = await PromptSession(
            "Enter chat ID (integer, e.g., -100123456789): "
        ).prompt_async()
        
        try:
            chat_id = int(chat_id_str)
        except ValueError:
            print_message("Chat ID must be a number.", level="error")
            logging.error(f"Invalid chat ID input: {chat_id_str}")
            return

        # Получаем информацию о чате
        try:
            entity = await client.get_entity(chat_id)
            
            # Проверка доступа
            try:
                await client.get_messages(entity, limit=1)
            except Exception as e:
                print_message(f"No access to chat {chat_id}: {e}", level="error")
                logging.error(f"No access to chat {chat_id}: {e}")
                return

            # Определяем тип чата
            chat_type = get_chat_type(entity)
            if not chat_type:
                print_message(f"Unsupported chat type: {type(entity).__name__}", level="error")
                return

            # Получаем название
            title = get_chat_title(entity)

            # Показываем информацию
            await display_chat_info(entity, title, chat_type)

            # Проверяем и добавляем в БД при необходимости
            async with pool.acquire() as conn:
                chat_exists = await conn.fetchval(
                    "SELECT 1 FROM chats WHERE chat_id = $1",
                    chat_id
                )
                
                if not chat_exists:
                    print_message(f"Chat {title} not found in database. Adding...", level="warning")
                    await add_chat_to_database(client, entity, chat_type, pool, conn)

            # Запрашиваем параметры импорта
            limit = await get_import_limit()
            import_media = await get_import_media_option()
            import_reactions = await get_import_reactions_option()

            # Запуск импорта
            print_message(f"Starting import of {limit} messages from {title} (ID: {chat_id})...")
            
            count = await import_messages_to_db(
                client, 
                chat_id, 
                limit, 
                pool=pool,
                import_media=import_media,
                import_reactions=import_reactions
            )
            
            print_message(f"Imported {count} new messages from {title} (ID: {chat_id}).")
            logging.info(f"User imported {count} messages from {chat_id}")

        except Exception as e:
            print_message(f"Error: source with ID {chat_id_str} not found or inaccessible. {e}", level="error")
            logging.error(f"Error importing for chat ID {chat_id_str}: {e}")

    except Exception as e:
        logging.error(f"Error in import_from_manual_id: {e}")
        print_message(f"Import error: {e}", level="error")

async def batch_import_menu(client, pool):
    """
    Меню пакетного импорта из нескольких источников.
    
    Args:
        client: Telegram client
        pool: пул соединений с БД
    """
    try:
        # Получаем список всех активных чатов
        async with pool.acquire() as conn:
            chats = await conn.fetch("""
                SELECT chat_id, title, type 
                FROM chats 
                WHERE is_active = TRUE 
                ORDER BY title
            """)
        
        if not chats:
            print_message("No active chats found in database.", level="warning")
            return

        # Создаем список для множественного выбора
        chat_choices = []
        for chat in chats:
            title = chat['title'][:50] + ('...' if len(chat['title'] or '') > 50 else '')
            chat_choices.append({
                "name": f"{title} (ID: {chat['chat_id']}, Type: {chat['type']})",
                "value": chat['chat_id']
            })
        
        # Добавляем опцию "Select all"
        chat_choices.insert(0, {"name": "Select All Chats", "value": "ALL"})
        chat_choices.append({"name": "Cancel", "value": None})

        # Множественный выбор чатов
        selected_chats = await select(
            "Select chats for batch import (use space to select multiple):",
            choices=chat_choices,
            multiselect=True
        ).ask_async()

        if not selected_chats or None in selected_chats:
            print_message("No chats selected. Returning to import submenu.", level="warning")
            return

        # Обрабатываем "Select All"
        if "ALL" in selected_chats:
            selected_chats = [chat['chat_id'] for chat in chats]

        # Запрашиваем общие параметры для всех чатов
        limit = await get_import_limit(default=50)
        import_media = await get_import_media_option()
        import_reactions = await get_import_reactions_option()
        parallel = await get_parallel_option()

        print_message(f"Starting batch import for {len(selected_chats)} chats...")
        
        if parallel:
            # Параллельный импорт с ограничением
            semaphore = asyncio.Semaphore(3)  # Не больше 3 параллельных импортов
            
            async def import_chat(chat_id):
                async with semaphore:
                    try:
                        count = await import_messages_to_db(
                            client, 
                            chat_id, 
                            limit, 
                            pool=pool,
                            import_media=import_media,
                            import_reactions=import_reactions
                        )
                        return chat_id, count, None
                    except Exception as e:
                        return chat_id, 0, str(e)
            
            tasks = [import_chat(chat_id) for chat_id in selected_chats]
            results = await asyncio.gather(*tasks)
            
            # Вывод результатов
            total = 0
            for chat_id, count, error in results:
                if error:
                    print_message(f"Chat {chat_id}: ERROR - {error}", level="error")
                else:
                    print_message(f"Chat {chat_id}: imported {count} messages")
                    total += count
                    
            print_message(f"Batch import completed. Total messages: {total}")
            
        else:
            # Последовательный импорт
            total = 0
            for chat_id in selected_chats:
                try:
                    count = await import_messages_to_db(
                        client, 
                        chat_id, 
                        limit, 
                        pool=pool,
                        import_media=import_media,
                        import_reactions=import_reactions
                    )
                    print_message(f"Chat {chat_id}: imported {count} messages")
                    total += count
                except Exception as e:
                    print_message(f"Error importing chat {chat_id}: {e}", level="error")
                    
            print_message(f"Batch import completed. Total messages: {total}")

    except Exception as e:
        logging.error(f"Error in batch import: {e}")
        print_message(f"Batch import error: {e}", level="error")

# ==================== Вспомогательные функции ====================

async def display_chat_info(entity, title, chat_type):
    """
    Отображение информации о чате.
    
    Args:
        entity: сущность Telegram
        title: название чата
        chat_type: тип чата
    """
    chat_info = [
        ["Title", title or "Not specified"],
        ["Chat ID", entity.id],
        ["Type", chat_type or "Unknown"],
        ["Username", getattr(entity, 'username', None) or "None"],
        ["Access", "Public" if getattr(entity, 'username', None) else "Private"],
        ["Participants", getattr(entity, 'participants_count', None) or "Unknown"],
        ["Description", (getattr(entity, 'about', None) or "None")[:50] + 
         ('...' if len(getattr(entity, 'about', '') or '') > 50 else '')]
    ]
    print(f"{Fore.CYAN}Selected chat information:")
    print(tabulate(chat_info, headers=["Parameter", "Value"], tablefmt="grid"))

def get_chat_type(entity):
    """
    Определение типа чата.
    
    Args:
        entity: сущность Telegram
        
    Returns:
        str: тип чата или None
    """
    if isinstance(entity, Channel):
        return 'supergroup' if getattr(entity, 'megagroup', False) else 'channel'
    elif isinstance(entity, Chat):
        return 'group'
    return None

def get_chat_title(entity):
    """
    Получение названия чата.
    
    Args:
        entity: сущность Telegram
        
    Returns:
        str: название чата
    """
    return getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or "Unknown"

async def add_chat_to_database(client, entity, chat_type, pool, conn=None):
    """
    Добавление чата в базу данных.
    
    Args:
        client: Telegram client
        entity: сущность Telegram
        chat_type: тип чата
        pool: пул соединений
        conn: существующее соединение (опционально)
    """
    chat_data = {
        'chat_id': entity.id,
        'type': chat_type,
        'access_type': 'public' if entity.username else 'private',
        'username': entity.username,
        'title': get_chat_title(entity),
        'description': getattr(entity, 'about', None),
        'participants_count': getattr(entity, 'participants_count', None)
    }
    
    if conn:
        await add_chat_to_db(conn, chat_data)
    else:
        async with pool.acquire() as new_conn:
            await add_chat_to_db(new_conn, chat_data)
    
    print_message(f"Chat {chat_data['title']} (ID: {entity.id}) added to database.")
    logging.info(f"Added chat {entity.id} to chats table before import.")

async def get_import_limit(default=100):
    """
    Получение лимита сообщений для импорта.
    
    Args:
        default: значение по умолчанию
        
    Returns:
        int: лимит сообщений
    """
    limit_input = await PromptSession(
        f"Enter number of messages to import (default {default}): "
    ).prompt_async()
    
    if limit_input.strip() and limit_input.strip().isdigit():
        return int(limit_input.strip())
    return default

async def get_import_media_option():
    """
    Получение опции импорта медиа.
    
    Returns:
        bool: True если нужно импортировать медиа
    """
    from questionary import confirm
    return await confirm("Download media files?", default=False).ask_async()

async def get_import_reactions_option():
    """
    Получение опции импорта реакций.
    
    Returns:
        bool: True если нужно импортировать реакции
    """
    from questionary import confirm
    return await confirm("Import detailed reactions?", default=False).ask_async()

async def get_parallel_option():
    """
    Получение опции параллельного импорта.
    
    Returns:
        bool: True если нужен параллельный импорт
    """
    from questionary import confirm
    return await confirm("Run imports in parallel? (faster but more resource intensive)", default=False).ask_async()

# ==================== Функции для обратной совместимости ====================

async def import_to_db_menu_legacy(client):
    """
    Устаревшая функция для обратной совместимости.
    Используйте import_to_db_menu(client, pool) вместо этой.
    """
    from db_module import create_db_pool
    
    print_message(
        "Warning: import_to_db_menu_legacy() is deprecated. "
        "Use import_to_db_menu(client, pool) instead.",
        level="warning"
    )
    
    # Создаем временный пул (не рекомендуется, но для совместимости)
    pool = await create_db_pool()
    try:
        await import_to_db_menu(client, pool)
    finally:
        await pool.close()