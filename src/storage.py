import polars as pl
import pandas as pd
from pathlib import Path
from typing import Any


class ObjectStore:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True, mode=0o755)
    
    def save(self, data: Any, key: str, format: str = 'parquet'):
        if isinstance(data, pl.DataFrame):
            path = self.base_path / f"{key}.{format}"
            # Remove existing file if it exists and is not writable
            if path.exists():
                try:
                    path.unlink()
                except PermissionError:
                    # If we can't delete, try to overwrite (may fail if owned by root)
                    pass
            if format == 'parquet':
                data.write_parquet(path)
            else:
                data.write_csv(path)
            return str(path)
        elif isinstance(data, pd.DataFrame):
            # Convert pandas to polars for faster I/O
            df_pl = pl.from_pandas(data)
            path = self.base_path / f"{key}.{format}"
            if format == 'parquet':
                df_pl.write_parquet(path)
            else:
                df_pl.write_csv(path)
            return str(path)
        
        if isinstance(data, dict):
            for table_name, df in data.items():
                path = self.base_path / f"{key}_{table_name}.{format}"
                # Remove existing file if it exists
                if path.exists():
                    try:
                        path.unlink()
                    except PermissionError:
                        pass
                if isinstance(df, pl.DataFrame):
                    if format == 'parquet':
                        df.write_parquet(path)
                    else:
                        df.write_csv(path)
                else:
                    # Convert pandas to polars
                    df_pl = pl.from_pandas(df)
                    if format == 'parquet':
                        df_pl.write_parquet(path)
                    else:
                        df_pl.write_csv(path)
    
    def load(self, key: str, format: str = 'parquet'):
        path = self.base_path / f"{key}.{format}"
        if path.exists():
            return pl.read_parquet(path) if format == 'parquet' else pl.read_csv(path)
        
        paths = list(self.base_path.glob(f"{key}_*.{format}"))
        if not paths:
            return None
        
        result = {}
        for p in paths:
            table_name = p.stem.replace(f"{key}_", "")
            result[table_name] = pl.read_parquet(p) if format == 'parquet' else pl.read_csv(p)
        return result
