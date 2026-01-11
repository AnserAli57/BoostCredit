import pandas as pd
import re


def _clean_timestamp_string(value):

    if not isinstance(value, str):
        return value
    
    value = value.strip()
    

    # This handles: '1986-06-23TEST', '1998-07-17TEMP123', '2020-01-15OLD', etc.
    value = re.sub(r'([0-9]{4}-[0-9]{2}-[0-9]{2})(?![0-9\s:-])(\S+)$', r'\1', value)
    
    # This handles: '2021-12-25 10:30:45EXTRA', '2022-03-10 10:30:45TEMP', etc.
    value = re.sub(r'([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})(?![0-9\s:-])(\S+)$', r'\1', value)
    
    # Remove trailing whitespace
    value = value.strip()
    
    return value


def parse_timestamp(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        try:
            return pd.Timestamp.fromtimestamp(value)
        except:
            return None
    if isinstance(value, str):
        value = _clean_timestamp_string(value)
        
        # pandas handles most formats including "Monday, June 30th, 2013" and standard datetime formats
        try:
            return pd.to_datetime(value)
        except:
            return None
    return None


def parse_date(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        value = _clean_timestamp_string(value)
        try:
            return pd.to_datetime(value).date()
        except:
            return None
    return None


def to_boolean(value):
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value_lower = value.lower().strip()
        
        # Exact matches for common true values
        if value_lower in ('true', '1', 'yes', 't', 'y', 'on'):
            return True
        
        # Exact matches for common false values
        if value_lower in ('false', '0', 'no', 'f', 'n', 'off', ''):
            return False
        
        # Handle typos: if string starts with 'tru' (like 'truee', 'tru', 'ture'), treat as True
        if value_lower.startswith('tru'):
            return True
        
        # Handle typos: if string starts with 'fals' (like 'falsee', 'fals'), treat as False
        if value_lower.startswith('fals'):
            return False
        
        return bool(value)
    return bool(value)


