"""Tests for CSV extractor."""
import unittest
import pandas as pd
import tempfile
import os
from src.extractors.csv_extractor import CSVExtractor


class TestCSVExtractor(unittest.TestCase):
    """Test cases for CSVExtractor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.extractor = CSVExtractor()
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'value': [10, 20, 30]
        })
    
    def test_extract_valid_csv(self):
        """Test extracting valid CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            self.test_data.to_csv(f.name, index=False)
            temp_path = f.name
        
        try:
            result = self.extractor.extract(temp_path)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
        finally:
            os.unlink(temp_path)
    
    def test_extract_invalid_file(self):
        """Test extracting non-existent file."""
        with self.assertRaises(Exception):
            self.extractor.extract('nonexistent_file.csv')


if __name__ == '__main__':
    unittest.main()

