"""
JSON data transformer with normalization to 3 tables.
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from .base_transformer import BaseTransformer
from ..utils.logger import setup_logger
from ..utils.pii_masking import mask_email, mask_phone, mask_national_id, mask_password, mask_address

logger = setup_logger()


class JSONTransformer(BaseTransformer):
    """Transformer for JSON data, normalizing into users, telephone_numbers, and jobs_history tables."""
    
    def transform(self, data: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Transform JSON data into three normalized tables:
        - users: Main user information
        - telephone_numbers: User telephone numbers (one-to-many)
        - jobs_history: User job history (one-to-many)
        
        Args:
            data: List of JSON records
            
        Returns:
            Dictionary with keys 'users', 'telephone_numbers', 'jobs_history'
        """
        try:
            logger.info("Starting JSON data transformation")
            
            users_data = []
            telephone_numbers_data = []
            jobs_history_data = []
            
            for record in data:
                user_id = record.get('user_id')
                if not user_id:
                    logger.warning("Skipping record without user_id")
                    continue
                
                # Extract user details
                user_details = record.get('user_details', {})
                
                # Create user record
                user_record = {
                    'user_id': user_id,
                    'created_at': self._parse_timestamp(record.get('created_at')),
                    'updated_at': self._parse_timestamp(record.get('updated_at')),
                    'logged_at': self._parse_timestamp_unix(record.get('logged_at')),
                    'name': self._mask_name(user_details.get('name')) if user_details.get('name') else None,
                    'dob': self._parse_date(user_details.get('dob')),
                    'address': mask_address(user_details.get('address')) if user_details.get('address') else None,
                    'username': mask_email(user_details.get('username')) if user_details.get('username') else None,
                    'password': mask_password(user_details.get('password')) if user_details.get('password') else None,
                    'national_id': mask_national_id(user_details.get('national_id')) if user_details.get('national_id') else None,
                }
                users_data.append(user_record)
                
                # Extract telephone numbers
                telephone_numbers = user_details.get('telephone_numbers', [])
                if isinstance(telephone_numbers, list):
                    for tel_num in telephone_numbers:
                        telephone_numbers_data.append({
                            'user_id': user_id,
                            'telephone_number': mask_phone(tel_num) if tel_num else None
                        })
                
                # Extract jobs history
                jobs_history = record.get('jobs_history', [])
                if isinstance(jobs_history, list):
                    for job in jobs_history:
                        jobs_history_data.append({
                            'job_id': job.get('id'),
                            'user_id': user_id,
                            'occupation': job.get('occupation'),
                            'is_fulltime': self._to_boolean(job.get('is_fulltime')),
                            'start': self._parse_date(job.get('start')),
                            'end': self._parse_date(job.get('end')),
                            'employer': job.get('employer')
                        })
            
            # Create DataFrames
            users_df = pd.DataFrame(users_data)
            telephone_numbers_df = pd.DataFrame(telephone_numbers_data)
            jobs_history_df = pd.DataFrame(jobs_history_data)
            
            logger.info(f"Successfully transformed data into {len(users_df)} users, "
                       f"{len(telephone_numbers_df)} telephone numbers, "
                       f"{len(jobs_history_df)} job history records")
            
            return {
                'users': users_df,
                'telephone_numbers': telephone_numbers_df,
                'jobs_history': jobs_history_df
            }
            
        except Exception as e:
            logger.error(f"Error transforming JSON data: {str(e)}")
            raise
    
    def _parse_timestamp(self, value: Any) -> Any:
        """Parse timestamp from various formats."""
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            # Try as Unix timestamp
            try:
                return pd.Timestamp.fromtimestamp(value)
            except (ValueError, OSError):
                return None
        
        if isinstance(value, str):
            # Try parsing common date formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
            ]
            
            for fmt in formats:
                try:
                    return pd.Timestamp(datetime.strptime(value[:19], fmt))
                except (ValueError, IndexError):
                    continue
            
            # Try pandas parsing
            try:
                return pd.Timestamp(value)
            except:
                return None
        
        return None
    
    def _parse_timestamp_unix(self, value: Any) -> Any:
        """Parse Unix timestamp."""
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            try:
                return pd.Timestamp.fromtimestamp(value)
            except (ValueError, OSError):
                return None
        
        return None
    
    def _parse_date(self, value: Any) -> Any:
        """Parse date from string."""
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).date()
            except:
                return None
        
        return None
    
    def _to_boolean(self, value: Any) -> bool:
        """Convert value to boolean."""
        if value is None or pd.isna(value):
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

