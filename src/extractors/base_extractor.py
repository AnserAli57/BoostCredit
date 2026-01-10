"""
Base extractor class for extensibility.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    @abstractmethod
    def extract(self, source: str) -> Any:
        """
        Extract data from a source.
        
        Args:
            source: Path to the data source
            
        Returns:
            Extracted data
        """
        pass

