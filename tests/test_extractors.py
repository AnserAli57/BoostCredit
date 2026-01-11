import unittest
import pandas as pd
import tempfile
import json
from src.extractors import CSVExtractor, JSONExtractor


class TestCSVExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = CSVExtractor()
    
    def test_extract_csv(self):
        df = pd.DataFrame({'id': [1, 2], 'name': ['John', 'Jane']})
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=True) as f:
            df.to_csv(f.name, index=False)
            result = self.extractor.extract(f.name)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
    
    def test_extract_invalid_file(self):
        with self.assertRaises(Exception):
            self.extractor.extract('nonexistent.csv')


class TestJSONExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = JSONExtractor()
    
    def test_extract_json(self):
        data = [{"user_id": "123"}, {"user_id": "456"}]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=True) as f:
            json.dump(data, f)
            f.flush()
            result = self.extractor.extract(f.name)
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 2)


if __name__ == '__main__':
    unittest.main()
