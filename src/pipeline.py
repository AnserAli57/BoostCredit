import os
from .extractors import CSVExtractor, JSONExtractor
from .transformers import CSVTransformer, JSONTransformer
from .loaders import SQLLoader, ObjectStoreLoader
from .storage import ObjectStore
from .utils.logger import setup_logger

logger = setup_logger()


class Pipeline:
    def __init__(self):
        self.object_store_path = os.getenv('OBJECT_STORE_PATH', './output')
        self.data_path = os.getenv('DATA_PATH', './data')
        loader_type = os.getenv('LOADER_TYPE', 'sql').lower()
        
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
        self.loader.load(data, store_key)
        logger.info(f"Loaded {store_key} from object store to destination")
    
    def close(self):
        self.loader.close()
