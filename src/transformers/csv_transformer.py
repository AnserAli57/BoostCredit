"""
CSV data transformer with cleaning and validation.
"""
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from .base_transformer import BaseTransformer
from ..utils.logger import setup_logger
from ..utils.pii_masking import mask_pii_in_dict

logger = setup_logger()


class CSVTransformer(BaseTransformer):
    """Transformer for CSV data."""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform CSV data according to requirements:
        - Add unique identifier (id column)
        - Convert created_at and last_login to timestamp
        - Convert is_claimed to boolean
        - Convert paid_amount to numeric with 2 decimal places
        - Mask PII data
        
        Args:
            data: Raw DataFrame from CSV
            
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info("Starting CSV data transformation")
            df = data.copy()
            
            # Ensure id column exists and is unique
            if 'id' not in df.columns:
                logger.warning("No 'id' column found, creating one")
                df['id'] = range(1, len(df) + 1)
            
            # Ensure id is unique
            if df['id'].duplicated().any():
                logger.warning("Duplicate IDs found, reassigning unique IDs")
                df['id'] = range(1, len(df) + 1)
            
            # Convert created_at to timestamp
            if 'created_at' in df.columns:
                df['created_at'] = df['created_at'].apply(self._parse_timestamp)
            
            # Convert last_login to timestamp
            if 'last_login' in df.columns:
                df['last_login'] = df['last_login'].apply(self._parse_timestamp_or_unix)
            
            # Convert is_claimed to boolean
            if 'is_claimed' in df.columns:
                df['is_claimed'] = df['is_claimed'].apply(self._to_boolean)
            
            # Convert paid_amount to numeric with 2 decimal places
            if 'paid_amount' in df.columns:
                df['paid_amount'] = pd.to_numeric(df['paid_amount'], errors='coerce')
                df['paid_amount'] = df['paid_amount'].round(2)
            
            # Mask PII data (name, address)
            if 'name' in df.columns:
                df['name'] = df['name'].apply(lambda x: self._mask_name(x) if pd.notna(x) else x)
            if 'address' in df.columns:
                df['address'] = df['address'].apply(lambda x: self._mask_address(x) if pd.notna(x) else x)
            
            logger.info(f"Successfully transformed {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming CSV data: {str(e)}")
            raise
    
    def _parse_timestamp(self, value: Any) -> Any:
        """Parse timestamp from various formats."""
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            # Unix timestamp
            try:
                return pd.Timestamp.fromtimestamp(value)
            except (ValueError, OSError):
                return None
        
        if isinstance(value, str):
            # Try parsing common date formats
            formats = [
                '%A, %B %dth, %Y',
                '%A, %B %d, %Y',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d',
            ]
            
            for fmt in formats:
                try:
                    return pd.Timestamp(datetime.strptime(value, fmt))
                except ValueError:
                    continue
            
            # Try pandas parsing
            try:
                return pd.Timestamp(value)
            except:
                return None
        
        return value
    
    def _parse_timestamp_or_unix(self, value: Any) -> Any:
        """Parse timestamp, handling both string formats and Unix timestamps."""
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            # Unix timestamp
            try:
                return pd.Timestamp.fromtimestamp(value)
            except (ValueError, OSError):
                return None
        
        return self._parse_timestamp(value)
    
    def _to_boolean(self, value: Any) -> bool:
        """Convert value to boolean."""
        if pd.isna(value):
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 't')
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def _mask_name(self, name: str) -> str:
        """Mask name, keeping first letter."""
        if not name or not isinstance(name, str):
            return name
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[0][0]}*** {parts[-1][0]}***"
        elif len(parts) == 1:
            return f"{parts[0][0]}***"
        return "***"
    
    def _mask_address(self, address: str) -> str:
        """Mask address, keeping city and state."""
        if not address or not isinstance(address, str):
            return address
        # Try to extract city and state (usually at the end)
        parts = address.split('\n')
        if len(parts) >= 2:
            street = parts[0]
            city_state = parts[1]
            masked_street = '*' * len(street)
            return f"{masked_street}\n{city_state}"
        return '*' * len(address)

