import logging
import os
from datetime import datetime
from colorama import init, Fore, Style

# Инициализация colorama для Windows
init(autoreset=True)

def setup_logging():
    """Настраивает логирование в файл и консоль."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"logs_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Настройка обработчиков
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    file_handler = logging.FileHandler(os.path.join(output_dir, 'app.log'), encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[file_handler, console_handler]
    )
    print(f"{Fore.GREEN}Логирование настроено. Логи сохраняются в {output_dir}/app.log")