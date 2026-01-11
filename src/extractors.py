import pandas as pd
import json
from typing import List, Dict


class CSVExtractor:
    def extract(self, source: str) -> pd.DataFrame:
        return pd.read_csv(source)


class JSONExtractor:
    def extract(self, source: str) -> List[Dict]:
        with open(source, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                f.seek(0)
                content = f.read().strip()
                return json.loads('[' + ','.join(content.split('\n')) + ']')
