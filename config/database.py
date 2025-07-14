# config/database.py
from dotenv import get_key

DB_CONFIG = {
    "host": "mysql-2025-order-book.b.aivencloud.com",
    "port": 19191,
    "user": get_key(".env", "user"),
    "password": get_key(".env", "password"),
    "database": get_key(".env", "database"),
    "charset": "utf8mb4",
    "connection_timeout": 10,
}