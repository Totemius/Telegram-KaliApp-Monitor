# utils.py
import asyncio
import logging
import json
import re
from colorama import Fore, Style, init as colorama_init  # Добавляем init
from config import db_config
from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
import yake
import asyncpg

# Инициализируем colorama
colorama_init()

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

async def listen_for_new_messages(callback):
    """Подписка на уведомления о новых сообщениях."""
    try:
        conn = await asyncpg.connect(**db_config)
        await conn.execute("LISTEN new_message")
        
        async def notification_handler(c, pid, channel, payload):
            try:
                message_data = json.loads(payload)
                await callback(message_data)
            except Exception as e:
                logging.error(f"Ошибка при обработке уведомления: {e}")
        
        conn.add_listener("new_message", notification_handler)
        
        while True:
            await asyncio.sleep(3600)
        
    except Exception as e:
        logging.error(f"Ошибка при подписке на уведомления: {e}")
        print_message(f"Ошибка при подписке на уведомления: {e}", level="error")
    finally:
        if conn:
            await conn.close()

async def check_words_against_dictionary(conn, text):
    """Проверка текста на новые слова и возврат списка новых слов."""
    try:
        if not text or not isinstance(text, str):
            return set()

        # Токенизация текста: разбиваем на слова, удаляем знаки препинания
        words = re.findall(r'\b[\w-]+\b', text.lower())
        
        # Очищаем слова от нежелательных символов
        cleaned_words = []
        for word in words:
            cleaned_word = re.sub(r'^[.,!?;:-]+|[.,!?;:-]+$', '', word)
            if not cleaned_word or re.match(r'^[-.,!?;:]+$', cleaned_word):
                continue
            cleaned_words.append(cleaned_word)

        if not cleaned_words:
            return set()

        # Получаем существующие слова из словаря
        existing_words = await conn.fetch(
            "SELECT word FROM dictionary WHERE word = ANY($1::text[])",
            cleaned_words
        )
        existing_words_set = {row['word'] for row in existing_words}

        # Все слова, отсутствующие в словаре, считаются новыми
        new_words = set(cleaned_words) - existing_words_set
        return new_words

    except Exception as e:
        logging.error(f"Ошибка при проверке слов: {e}")
        print_message(f"Ошибка при проверке слов: {e}", level="error")
        return set()

def extract_keywords(text):
    """Извлечение ключевых слов с использованием yake."""
    try:
        logging.debug(f"Извлечение ключевых слов для текста: {text[:100]}{'...' if len(text) > 100 else ''}")
        kw_extractor = yake.KeywordExtractor(lan='ru', n=3, dedupLim=0.9, top=5)
        keywords = kw_extractor.extract_keywords(text or '')
        keyword_list = [kw[0] for kw in keywords]
        logging.debug(f"Результат ключевых слов: {keyword_list}")
        return keyword_list
    except Exception as e:
        logging.error(f"Ошибка при извлечении ключевых слов: {e}")
        return []

def extract_ner(text):
    """Извлечение именованных сущностей (NER) с использованием natasha."""
    try:
        logging.debug(f"Извлечение NER для текста: {text[:100]}{'...' if len(text) > 100 else ''}")
        segmenter = Segmenter()
        doc = Doc(text or '')
        doc.segment(segmenter)
        emb = NewsEmbedding()
        ner_tagger = NewsNERTagger(emb)
        doc.tag_ner(ner_tagger)
        ner_results = {'person': [], 'organization': [], 'location': []}
        for span in doc.spans:
            if span.type == 'PER':
                ner_results['person'].append(span.text)
            elif span.type == 'ORG':
                ner_results['organization'].append(span.text)
            elif span.type == 'LOC':
                ner_results['location'].append(span.text)
        logging.debug(f"Результат NER: {ner_results}")
        return ner_results
    except Exception as e:
        logging.error(f"Ошибка при извлечении NER: {e}")
        return {'person': [], 'organization': [], 'location': []}