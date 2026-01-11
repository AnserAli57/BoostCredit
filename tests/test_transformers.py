import unittest
import polars as pl
from src.transformers import CSVTransformer, JSONTransformer


class TestCSVTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = CSVTransformer()
    
    def test_transform_with_timestamps(self):
        df = pl.DataFrame({
            'id': [1, 2],
            'created_at': ['2020-01-01', '2020-01-02'],
            'last_login': ['1577836800', '1577923200'],
            'is_claimed': ['True', 'False'],
            'paid_amount': ['5004.67', '893.40']
        })
        
        result = self.transformer.transform(df)
        
        self.assertIsNotNone(result['created_at'][0])
        self.assertIsNotNone(result['last_login'][0])
        self.assertTrue(result['is_claimed'][0])
        self.assertFalse(result['is_claimed'][1])
        self.assertEqual(result['paid_amount'][0], 5004.67)
    
    def test_transform_unique_ids(self):
        df = pl.DataFrame({
            'id': [1, 1, 2],
            'name': ['A', 'B', 'C']
        })
        
        result = self.transformer.transform(df)
        # Check that all IDs are unique (no duplicates)
        self.assertTrue(result['id'].is_unique().all())


class TestJSONTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = JSONTransformer()
    
    def test_transform_normalization(self):
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
        
        self.assertEqual(result['users'].height, 1)
        self.assertEqual(result['telephone_numbers'].height, 2)
        self.assertEqual(result['jobs_history'].height, 1)


if __name__ == '__main__':
    unittest.main()
