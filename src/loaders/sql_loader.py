"""
SQL database loader.
"""
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Any
from .base_loader import BaseLoader
from ..utils.logger import setup_logger

logger = setup_logger()


class SQLLoader(BaseLoader):
    """Loader for SQL databases."""
    
    def __init__(self, connection_string: str):
        """
        Initialize SQL loader.
        
        Args:
            connection_string: SQLAlchemy connection string
        """
        self.connection_string = connection_string
        # Enable foreign keys for SQLite
        if 'sqlite' in connection_string.lower():
            self.engine = create_engine(connection_string, connect_args={'check_same_thread': False})
            # Enable foreign key constraints
            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA foreign_keys = ON"))
                conn.commit()
        else:
            self.engine = create_engine(connection_string)
        logger.info("SQL loader initialized")
    
    def load(self, data: Any, target: str) -> None:
        """
        Load data to SQL database.
        
        Args:
            data: DataFrame or dict of DataFrames to load
            target: Table name or dict of table names
        """
        try:
            if isinstance(data, pd.DataFrame):
                # Single table
                logger.info(f"Loading data to table: {target}")
                self._create_table(data, target)
                data.to_sql(target, self.engine, if_exists='append', index=False)
                logger.info(f"Successfully loaded {len(data)} rows to table {target}")
            
            elif isinstance(data, dict):
                # Multiple tables
                for table_name, df in data.items():
                    logger.info(f"Loading data to table: {table_name}")
                    self._create_table(df, table_name)
                    df.to_sql(table_name, self.engine, if_exists='append', index=False)
                    logger.info(f"Successfully loaded {len(df)} rows to table {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading data to SQL: {str(e)}")
            raise
    
    def _create_table(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Create table with appropriate schema based on table name.
        
        Args:
            df: DataFrame with data
            table_name: Name of the table to create
        """
        try:
            inspector = inspect(self.engine)
            
            # Check if table exists
            if inspector.has_table(table_name):
                logger.info(f"Table {table_name} already exists")
                return
            
            logger.info(f"Creating table {table_name}")
            
            if table_name == 'test':
                self._create_test_table(df)
            elif table_name == 'users':
                self._create_users_table(df)
            elif table_name == 'telephone_numbers':
                self._create_telephone_numbers_table(df)
            elif table_name == 'jobs_history':
                self._create_jobs_history_table(df)
            else:
                # Generic table creation
                df.head(0).to_sql(table_name, self.engine, if_exists='replace', index=False)
                logger.info(f"Created generic table {table_name}")
                
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def _create_test_table(self, df: pd.DataFrame) -> None:
        """Create test table from CSV data."""
        with self.engine.connect() as conn:
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
            conn.commit()
            logger.info("Created test table")
    
    def _create_users_table(self, df: pd.DataFrame) -> None:
        """Create users table."""
        with self.engine.connect() as conn:
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
            conn.commit()
            logger.info("Created users table")
    
    def _create_telephone_numbers_table(self, df: pd.DataFrame) -> None:
        """Create telephone_numbers table."""
        # Detect database type
        is_sqlite = 'sqlite' in self.connection_string.lower()
        
        with self.engine.connect() as conn:
            if is_sqlite:
                # SQLite uses INTEGER PRIMARY KEY AUTOINCREMENT
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS telephone_numbers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id VARCHAR(255) NOT NULL,
                        telephone_number VARCHAR(50),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """))
            else:
                # PostgreSQL/MySQL use SERIAL/AUTO_INCREMENT
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS telephone_numbers (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        telephone_number VARCHAR(50),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """))
            conn.commit()
            logger.info("Created telephone_numbers table")
    
    def _create_jobs_history_table(self, df: pd.DataFrame) -> None:
        """Create jobs_history table."""
        with self.engine.connect() as conn:
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
            logger.info("Created jobs_history table")
    
    def close(self) -> None:
        """Close database connection."""
        self.engine.dispose()
        logger.info("SQL connection closed")

