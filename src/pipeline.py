import os
from typing import Any
import pandas as pd
from .extractors import CSVExtractor, JSONExtractor
from .transformers import CSVTransformer, JSONTransformer
from .loaders import SQLLoader
from .storage import ObjectStore
from .utils.logger import setup_logger
import polars as pl

logger = setup_logger()


class Pipeline:
    def __init__(self):
        self.object_store_path = os.getenv('OBJECT_STORE_PATH', './output')
        self.data_path = os.getenv('DATA_PATH', './data')
        
        self.object_store = ObjectStore(self.object_store_path)
        self.csv_extractor = CSVExtractor()
        self.json_extractor = JSONExtractor()
        self.csv_transformer = CSVTransformer()
        self.json_transformer = JSONTransformer()
        self.loader = SQLLoader()
    
    def process_csv(self, filename: str):
        store_key = os.getenv('STORE_KEY')
        
        if not store_key:
            raise ValueError("STORE_KEY must be set via environment variables")
        
        logger.info(f"Processing CSV: {filename}")
        
        file_path = f"{self.data_path}/{filename}"
        raw_data = self.csv_extractor.extract(file_path)
        transformed = self.csv_transformer.transform(raw_data)
        
        self.object_store.save(transformed, store_key, 'parquet')
        logger.info(f"Saved to object store: {store_key}.parquet")
        
        self.load_from_store(store_key)
    
    def process_json(self, filename: str):
        store_key = os.getenv('STORE_KEY')
        
        if not store_key:
            raise ValueError("STORE_KEY must be set via environment variables")
        
        logger.info(f"Processing JSON: {filename}")
        
        file_path = f"{self.data_path}/{filename}"
        raw_data = self.json_extractor.extract(file_path)
        transformed = self.json_transformer.transform(raw_data)
        
        self.object_store.save(transformed, store_key, 'parquet')
        logger.info(f"Saved to object store: {store_key}_*.parquet")
        
        self.load_from_store(store_key)
    
    def load_from_store(self, store_key: str):
        logger.info(f"Loading from object store: {store_key} to destination database")
        data = self.object_store.load(store_key, 'parquet')
        if isinstance(data, pl.DataFrame):
            data = data.to_pandas()
        elif isinstance(data, dict):
            data = {k: (v.to_pandas() if isinstance(v, pl.DataFrame) else v) for k, v in data.items()}
        try:
            # Determine table name (CSV data goes to 'test' table, JSON uses store_key)
            if isinstance(data, pd.DataFrame):
                table_name = 'test' if store_key == 'csv_data' else store_key
            else:
                table_name = store_key  # For dict (JSON data), use store_key
            
            # Clear existing data before loading to avoid duplicates
            self._clear_existing_data(data, store_key)
            self.loader.load(data, table_name)
            logger.info(f"Loaded {store_key} from object store to destination")
        except ConnectionError as e:
            logger.error(f"Database load failed: {e}")
            logger.info(f"Data has been successfully saved to object store: {store_key}")
            raise
    
    def _clear_existing_data(self, data: Any, store_key: str):
        """Clear existing data from tables before loading to avoid duplicates."""
        from sqlalchemy import text
        import polars as pl
        
        try:
            self.loader._ensure_engine()
            with self.loader.engine.connect() as conn:
                if isinstance(data, (pd.DataFrame, pl.DataFrame)):
                    # Single table - determine table name
                    table_name = 'test' if store_key == 'csv_data' else store_key
                    try:
                        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                        conn.commit()
                        logger.info(f"Cleared existing data from table: {table_name}")
                    except Exception:
                        pass  # Table might not exist yet
                elif isinstance(data, dict):
                    # Multiple tables - clear in reverse order (to handle foreign keys)
                    table_order = ['jobs_history', 'telephone_numbers', 'users']
                    for table_name in table_order:
                        if table_name in data:
                            try:
                                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                                logger.info(f"Cleared existing data from table: {table_name}")
                            except Exception:
                                pass  # Table might not exist yet
                    conn.commit()
        except Exception as e:
            logger.warning(f"Could not clear existing data: {e}")
            # Continue anyway - the loader will handle duplicates
    
    def close(self):
        self.loader.close()
