"""
Base loader class for extensibility.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseLoader(ABC):
    """Base class for all data loaders."""
    
    @abstractmethod
    def load(self, data: Any, target: str) -> None:
        """
        Load data to a target destination.
        
        Args:
            data: Transformed data to load
            target: Target destination identifier
        """
        pass

