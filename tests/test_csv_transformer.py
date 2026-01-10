"""Tests for CSV transformer."""
import unittest
import pandas as pd
from src.transformers.csv_transformer import CSVTransformer


class TestCSVTransformer(unittest.TestCase):
    """Test cases for CSVTransformer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.transformer = CSVTransformer()
    
    def test_transform_with_timestamps(self):
        """Test timestamp conversion."""
        df = pd.DataFrame({
            'id': [1, 2],
            'created_at': ['Monday, June 30th, 2013', '2020-01-01'],
            'last_login': [1202190735, 1577836800],
            'is_claimed': ['True', 'False'],
            'paid_amount': ['5004.67', '893.40']
        })
        
        result = self.transformer.transform(df)
        
        self.assertIsNotNone(result['created_at'].iloc[0])
        self.assertIsNotNone(result['last_login'].iloc[0])
        self.assertTrue(result['is_claimed'].iloc[0])
        self.assertFalse(result['is_claimed'].iloc[1])
        self.assertEqual(result['paid_amount'].iloc[0], 5004.67)
    
    def test_transform_unique_ids(self):
        """Test unique ID generation."""
        df = pd.DataFrame({
            'id': [1, 1, 2],  # Duplicate IDs
            'name': ['A', 'B', 'C']
        })
        
        result = self.transformer.transform(df)
        self.assertTrue(result['id'].is_unique)


if __name__ == '__main__':
    unittest.main()

