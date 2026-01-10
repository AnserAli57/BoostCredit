"""
Main ETL Pipeline orchestrator.
"""
from typing import Optional
from .extractors.csv_extractor import CSVExtractor
from .extractors.json_extractor import JSONExtractor
from .transformers.csv_transformer import CSVTransformer
from .transformers.json_transformer import JSONTransformer
from .loaders.sql_loader import SQLLoader
from .utils.logger import setup_logger

logger = setup_logger()


class ETLPipeline:
    """Main ETL Pipeline class."""
    
    def __init__(self, db_connection_string: str):
        """
        Initialize ETL Pipeline.
        
        Args:
            db_connection_string: SQLAlchemy connection string for the database
        """
        self.db_connection_string = db_connection_string
        self.csv_extractor = CSVExtractor()
        self.json_extractor = JSONExtractor()
        self.csv_transformer = CSVTransformer()
        self.json_transformer = JSONTransformer()
        self.sql_loader = SQLLoader(db_connection_string)
        logger.info("ETL Pipeline initialized")
    
    def process_csv(self, csv_file_path: str, table_name: str = 'test') -> None:
        """
        Process CSV file: extract, transform, and load.
        
        Args:
            csv_file_path: Path to the CSV file
            table_name: Target table name (default: 'test')
        """
        try:
            logger.info(f"Starting CSV processing pipeline for {csv_file_path}")
            
            # Extract
            raw_data = self.csv_extractor.extract(csv_file_path)
            
            # Transform
            transformed_data = self.csv_transformer.transform(raw_data)
            
            # Load
            self.sql_loader.load(transformed_data, table_name)
            
            logger.info(f"Successfully completed CSV processing pipeline")
            
        except Exception as e:
            logger.error(f"Error in CSV processing pipeline: {str(e)}")
            raise
    
    def process_json(self, json_file_path: str) -> None:
        """
        Process JSON file: extract, transform, and load.
        
        Args:
            json_file_path: Path to the JSON file
        """
        try:
            logger.info(f"Starting JSON processing pipeline for {json_file_path}")
            
            # Extract
            raw_data = self.json_extractor.extract(json_file_path)
            
            # Transform
            transformed_data = self.json_transformer.transform(raw_data)
            
            # Load (multiple tables)
            self.sql_loader.load(transformed_data, transformed_data)
            
            logger.info(f"Successfully completed JSON processing pipeline")
            
        except Exception as e:
            logger.error(f"Error in JSON processing pipeline: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close database connections."""
        self.sql_loader.close()

