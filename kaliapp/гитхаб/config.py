# config.py
from dotenv import load_dotenv
import os

load_dotenv()

# Параметры подключения к БД
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}