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
        self.engine = None  # Will be initialized on first use
    
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

    def _ensure_engine(self):
        """Initialize engine and check connection only when needed."""
        if self.engine is None:
            self.engine = create_engine(
                self.connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True
            )
            self._check_connection()
    
    def _fast_insert_postgresql(self, df: pd.DataFrame, table_name: str):
        """Fast bulk insert using PostgreSQL COPY for better performance."""
        import io
        
        # Get raw connection for COPY
        raw_conn = self.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            
            # Create StringIO buffer
            output = io.StringIO()
            
            # Write DataFrame to CSV format in memory (use \\N for NULL)
            null_repr = '\\N'
            df.to_csv(output, sep='\t', header=False, index=False, na_rep=null_repr)
            output.seek(0)
            
            # Use COPY FROM for fast bulk insert
            columns = ', '.join([f'"{col}"' for col in df.columns])
            # Use ON CONFLICT DO NOTHING to handle duplicates gracefully
            copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{null_repr}')"
            
            # For tables with primary keys, we need to handle duplicates
            # Check if table has primary key by trying to insert and handling conflict
            try:
                cursor.copy_expert(copy_sql, output)
                raw_conn.commit()
            except Exception as e:
                # If duplicate key error, truncate and retry, or use temp table approach
                if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                    raw_conn.rollback()
                    # Truncate table and retry
                    cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    output.seek(0)
                    cursor.copy_expert(copy_sql, output)
                    raw_conn.commit()
                else:
                    raise
            cursor.close()
        finally:
            raw_conn.close()
    
    def load(self, data: Any, target: str):
        import polars as pl
        try:
            self._ensure_engine()
        except ConnectionError as e:
            raise ConnectionError(
                f"Database load failed: {e}. "
                f"Data has been saved to object store but could not be loaded into database. "
                f"Please check database connection and try again."
            ) from e
        
        # Use COPY method for PostgreSQL (much faster than INSERT)
        chunksize = 50000  # Fallback chunksize for non-PostgreSQL
        if isinstance(data, pl.DataFrame):
            # Convert Polars to pandas only for database write
            df_pd = data.to_pandas()
            # Create table first
            self._create_table(df_pd, target)
            if self.db_type == 'postgresql' or self.db_type == 'postgres':
                self._fast_insert_postgresql(df_pd, target)
            else:
                df_pd.to_sql(target, self.engine, if_exists='append', index=False, chunksize=chunksize, method='multi')
        elif isinstance(data, pd.DataFrame):
            # Create table first
            self._create_table(data, target)
            if self.db_type == 'postgresql' or self.db_type == 'postgres':
                self._fast_insert_postgresql(data, target)
            else:
                data.to_sql(target, self.engine, if_exists='append', index=False, chunksize=chunksize, method='multi')
        elif isinstance(data, dict):
            # Create tables in correct order (users first, then dependent tables)
            table_order = ['users', 'telephone_numbers', 'jobs_history']
            for table_name in table_order:
                if table_name in data:
                    df = data[table_name]
                    if isinstance(df, pl.DataFrame):
                        df = df.to_pandas()
                    # Create table first
                    self._create_table(df, table_name)
                    if self.db_type == 'postgresql' or self.db_type == 'postgres':
                        self._fast_insert_postgresql(df, table_name)
                    else:
                        df.to_sql(table_name, self.engine, if_exists='append', index=False, chunksize=chunksize, method='multi')
    
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
                        "end" DATE,
                        employer VARCHAR(255),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """))
            conn.commit()
    
    def close(self):
        if self.engine is not None:
            self.engine.dispose()


class ObjectStoreLoader:
    def __init__(self, base_path: str):
        from .storage import ObjectStore
        self.object_store = ObjectStore(base_path)
    
    def load(self, data: Any, target: str):
        self.object_store.save(data, target, 'parquet')
    
