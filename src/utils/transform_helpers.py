import re
from datetime import datetime


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
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except:
            return None
    if isinstance(value, str):
        value = value.strip()
        # Check if it's a numeric string (Unix timestamp)
        try:
            numeric_value = float(value)
            # If it's a reasonable Unix timestamp (between 1970 and 2100)
            if 0 <= numeric_value <= 4102444800:  # Jan 1, 1970 to Jan 1, 2100
                return datetime.fromtimestamp(numeric_value)
        except (ValueError, OverflowError, OSError):
            pass
        
        # Clean timestamp string (remove trailing TEST, etc.)
        value = _clean_timestamp_string(value)
        
        # Try common datetime formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%A, %B %d, %Y',
            '%A, %B %dth, %Y',
            '%A, %B %dst, %Y',
            '%A, %B %dnd, %Y',
            '%A, %B %drd, %Y',
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except:
                continue
        # Try parsing with dateutil if available, otherwise return None
        try:
            from dateutil import parser
            return parser.parse(value)
        except:
            return None
    return None


def parse_date(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = _clean_timestamp_string(value)
        timestamp = parse_timestamp(value)
        if timestamp:
            return timestamp.date()
        return None
    return None


def to_boolean(value):
    if value is None:
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


