import pandas as pd


def parse_timestamp(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        try:
            return pd.Timestamp.fromtimestamp(value)
        except:
            return None
    if isinstance(value, str):
        try:
            return pd.to_datetime(value)
        except:
            return None
    return None


def parse_date(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
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
        return value.lower() in ('true', '1', 'yes', 't', 'y')
    return bool(value)


