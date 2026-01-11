import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import Any


class SQLLoader:
    def __init__(self):
        self.db_type = os.getenv('DB_TYPE', 'postgresql').lower()
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_name = os.getenv('DB_NAME')
        
        if not all([self.db_host, self.db_port, self.db_user, self.db_password, self.db_name]):
            raise ValueError("Missing required database credentials: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
        
        self.connection_string = self._build_connection_string()
        self.engine = create_engine(self.connection_string)
        self._check_connection()
    
    def _check_connection(self, max_retries=5, retry_delay=2):
        """Check database connection with retry logic."""
        for attempt in range(max_retries):
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return
            except OperationalError as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ConnectionError(f"Failed to connect to database after {max_retries} attempts: {e}")
    
    def _build_connection_string(self):
        if self.db_type == 'postgresql' or self.db_type == 'postgres':
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}. Only PostgreSQL is supported.")

    def load(self, data: Any, target: str):
        if isinstance(data, pd.DataFrame):
            self._create_table(data, target)
            data.to_sql(target, self.engine, if_exists='append', index=False)
        elif isinstance(data, dict):
            for table_name, df in data.items():
                self._create_table(df, table_name)
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
    
    def _create_table(self, df: pd.DataFrame, table_name: str):
        from sqlalchemy import inspect
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            return
        
        with self.engine.connect() as conn:
            if table_name == 'test':
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS test (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255),
                        address TEXT,
                        color VARCHAR(50),
                        created_at TIMESTAMP,
                        last_login TIMESTAMP,
                        is_claimed BOOLEAN,
                        paid_amount NUMERIC(10, 2)
                    )
                """))
            elif table_name == 'users':
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id VARCHAR(255) PRIMARY KEY,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        logged_at TIMESTAMP,
                        name VARCHAR(255),
                        dob DATE,
                        address TEXT,
                        username VARCHAR(255),
                        password VARCHAR(255),
                        national_id VARCHAR(50)
                    )
                """))
            elif table_name == 'telephone_numbers':
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS telephone_numbers (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        telephone_number VARCHAR(50),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """))
            elif table_name == 'jobs_history':
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS jobs_history (
                        job_id VARCHAR(255) PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        occupation VARCHAR(255),
                        is_fulltime BOOLEAN,
                        start DATE,
                        end DATE,
                        employer VARCHAR(255),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """))
            conn.commit()
    
    def close(self):
        self.engine.dispose()


class ObjectStoreLoader:
    def __init__(self, base_path: str):
        from .storage import ObjectStore
        self.object_store = ObjectStore(base_path)
    
    def load(self, data: Any, target: str):
        self.object_store.save(data, target, 'parquet')
    
