"""Tests for JSON extractor."""
import unittest
import json
import tempfile
import os
from src.extractors.json_extractor import JSONExtractor


class TestJSONExtractor(unittest.TestCase):
    """Test cases for JSONExtractor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.extractor = JSONExtractor()
    
    def test_extract_valid_json(self):
        """Test extracting valid JSON file."""
        test_data = [
            {"user_id": "123", "name": "John"},
            {"user_id": "456", "name": "Jane"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            for record in test_data:
                f.write(json.dumps(record) + '\n')
            temp_path = f.name
        
        try:
            result = self.extractor.extract(temp_path)
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 2)
        finally:
            os.unlink(temp_path)


if __name__ == '__main__':
    unittest.main()

