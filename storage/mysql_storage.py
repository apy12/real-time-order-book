# storage/mysql_storage.py
import mysql.connector
from mysql.connector import Error
from storage.base_storage import BaseStorage
from utils.logger import logger
from datetime import datetime

class MySQLStorage(BaseStorage):
    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                logger.info("Successfully connected to MySQL database.")
                return True
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {e}")
            self.connection = None
        return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL database connection closed.")
            self.connection = None

    def create_tables(self):
        if not self.connection or not self.connection.is_connected():
            logger.error("Cannot create tables: Not connected to database.")
            return False

        try:
            cursor = self.connection.cursor()
            tables_sql = [
                """
                CREATE TABLE IF NOT EXISTS order_book_snapshots (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    exchange_id VARCHAR(50) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    datetime_utc DATETIME(3) NOT NULL,
                    nonce BIGINT,
                    INDEX idx_symbol_timestamp (symbol, timestamp_ms),
                    INDEX idx_exchange_symbol_timestamp (exchange_id, symbol, timestamp_ms)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS bids (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    snapshot_id BIGINT NOT NULL,
                    price DECIMAL(20, 10) NOT NULL,
                    amount DECIMAL(20, 10) NOT NULL,
                    FOREIGN KEY (snapshot_id) REFERENCES order_book_snapshots(id) ON DELETE CASCADE,
                    INDEX idx_snapshot_id_price (snapshot_id, price)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS asks (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    snapshot_id BIGINT NOT NULL,
                    price DECIMAL(20, 10) NOT NULL,
                    amount DECIMAL(20, 10) NOT NULL,
                    FOREIGN KEY (snapshot_id) REFERENCES order_book_snapshots(id) ON DELETE CASCADE,
                    INDEX idx_snapshot_id_price (snapshot_id, price)
                );
                """
            ]
            for sql in tables_sql:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("Tables checked/created successfully.")
            return True
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            return False
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def insert_order_book_snapshot(self, snapshot_data: dict):
        if not self.connection or not self.connection.is_connected():
            logger.error("Cannot insert data: Not connected to database.")
            return False

        try:
            cursor = self.connection.cursor()

            # Insert into order_book_snapshots
            snapshot_sql = """
            INSERT INTO order_book_snapshots
            (symbol, exchange_id, timestamp_ms, datetime_utc, nonce)
            VALUES (%s, %s, %s, %s, %s)
            """
            # Convert ISO 8601 string to datetime object for MySQL DATETIME type
            datetime_obj = datetime.fromisoformat(snapshot_data['datetime'].replace('Z', '+00:00'))

            cursor.execute(snapshot_sql, (
                snapshot_data['symbol'],
                snapshot_data['exchange_id'],
                snapshot_data['timestamp'],
                datetime_obj,
                snapshot_data['nonce']
            ))
            snapshot_id = cursor.lastrowid

            # Insert bids
            bids_sql = "INSERT INTO bids (snapshot_id, price, amount) VALUES (%s, %s, %s)"
            bids_values = [(snapshot_id, float(bid[0]), float(bid[1])) for bid in snapshot_data['bids']]
            if bids_values:
                cursor.executemany(bids_sql, bids_values)

            # Insert asks
            asks_sql = "INSERT INTO asks (snapshot_id, price, amount) VALUES (%s, %s, %s)"
            asks_values = [(snapshot_id, float(ask[0]), float(ask[1])) for ask in snapshot_data['asks']]
            if asks_values:
                cursor.executemany(asks_sql, asks_values)

            self.connection.commit()
            logger.info(f"Inserted snapshot {snapshot_id} for {snapshot_data['symbol']} on {snapshot_data['exchange_id']}")
            return True
        except Error as e:
            logger.error(f"Error inserting order book snapshot: {e}")
            self.connection.rollback()
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during data insertion: {e}")
            self.connection.rollback()
            return False
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()