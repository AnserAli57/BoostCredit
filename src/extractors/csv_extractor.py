"""
CSV file extractor.
"""
import pandas as pd
from typing import Any
from .base_extractor import BaseExtractor
from ..utils.logger import setup_logger

logger = setup_logger()


class CSVExtractor(BaseExtractor):
    """Extractor for CSV files."""
    
    def extract(self, source: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.
        
        Args:
            source: Path to the CSV file
            
        Returns:
            DataFrame containing the CSV data
        """
        try:
            logger.info(f"Extracting data from CSV file: {source}")
            df = pd.read_csv(source)
            logger.info(f"Successfully extracted {len(df)} rows from CSV file")
            return df
        except Exception as e:
            logger.error(f"Error extracting CSV file {source}: {str(e)}")
            raise

