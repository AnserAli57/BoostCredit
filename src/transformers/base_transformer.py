"""
Base transformer class for extensibility.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Transform data.
        
        Args:
            data: Raw data to transform
            
        Returns:
            Transformed data
        """
        pass

