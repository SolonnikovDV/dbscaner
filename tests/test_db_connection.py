import psycopg2

# Параметры подключения
# Import database configuration from config.py
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.config_test import DB_CONFIG


try:
    # Попытка подключения
    conn = psycopg2.connect(**DB_CONFIG)
    print("Успешное подключение к базе данных!")
    
    # Простой тестовый запрос
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Версия PostgreSQL: {version[0]}")
    
    # Закрытие соединения
    conn.close()
    print("Соединение закрыто")

except Exception as e:
    print(f"Ошибка при подключении к базе данных: {e}")