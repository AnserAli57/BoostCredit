"""
JSON file extractor.
"""
import json
from typing import Any, List, Dict
from .base_extractor import BaseExtractor
from ..utils.logger import setup_logger

logger = setup_logger()


class JSONExtractor(BaseExtractor):
    """Extractor for JSON files (JSONL format - one JSON object per line)."""
    
    def extract(self, source: str) -> List[Dict[str, Any]]:
        """
        Extract data from a JSON file (JSONL format).
        
        Args:
            source: Path to the JSON file
            
        Returns:
            List of dictionaries containing the JSON data
        """
        try:
            logger.info(f"Extracting data from JSON file: {source}")
            data = []
            with open(source, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Error parsing JSON at line {line_num}: {str(e)}")
                            continue
            
            logger.info(f"Successfully extracted {len(data)} records from JSON file")
            return data
        except Exception as e:
            logger.error(f"Error extracting JSON file {source}: {str(e)}")
            raise

