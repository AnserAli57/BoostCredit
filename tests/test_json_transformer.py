"""Tests for JSON transformer."""
import unittest
from src.transformers.json_transformer import JSONTransformer


class TestJSONTransformer(unittest.TestCase):
    """Test cases for JSONTransformer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.transformer = JSONTransformer()
    
    def test_transform_normalization(self):
        """Test JSON normalization into 3 tables."""
        test_data = [{
            "user_id": "123",
            "created_at": "2020-01-01T00:00:00",
            "updated_at": "2020-01-02T00:00:00",
            "logged_at": 1577836800,
            "user_details": {
                "name": "John Doe",
                "dob": "1990-01-01",
                "telephone_numbers": ["123-456-7890", "098-765-4321"]
            },
            "jobs_history": [{
                "id": "job1",
                "occupation": "Engineer",
                "start": "2020-01-01",
                "end": "2021-01-01"
            }]
        }]
        
        result = self.transformer.transform(test_data)
        
        self.assertIn('users', result)
        self.assertIn('telephone_numbers', result)
        self.assertIn('jobs_history', result)
        
        self.assertEqual(len(result['users']), 1)
        self.assertEqual(len(result['telephone_numbers']), 2)
        self.assertEqual(len(result['jobs_history']), 1)


if __name__ == '__main__':
    unittest.main()

