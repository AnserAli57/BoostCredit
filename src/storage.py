import pandas as pd
from pathlib import Path
from typing import Any


class ObjectStore:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save(self, data: Any, key: str, format: str = 'parquet'):
        if isinstance(data, pd.DataFrame):
            path = self.base_path / f"{key}.{format}"
            data.to_parquet(path, index=False) if format == 'parquet' else data.to_csv(path, index=False)
            return str(path)
        
        if isinstance(data, dict):
            for table_name, df in data.items():
                path = self.base_path / f"{key}_{table_name}.{format}"
                df.to_parquet(path, index=False) if format == 'parquet' else df.to_csv(path, index=False)
    
    def load(self, key: str, format: str = 'parquet'):
        path = self.base_path / f"{key}.{format}"
        if path.exists():
            return pd.read_parquet(path) if format == 'parquet' else pd.read_csv(path)
        
        paths = list(self.base_path.glob(f"{key}_*.{format}"))
        if not paths:
            return None
        
        result = {}
        for p in paths:
            table_name = p.stem.replace(f"{key}_", "")
            result[table_name] = pd.read_parquet(p) if format == 'parquet' else pd.read_csv(p)
        return result
