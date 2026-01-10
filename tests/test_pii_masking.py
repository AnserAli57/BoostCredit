"""Tests for PII masking utilities."""
import unittest
from src.utils.pii_masking import (
    mask_email, mask_phone, mask_national_id,
    mask_address, mask_password, mask_pii_in_dict
)


class TestPIIMasking(unittest.TestCase):
    """Test cases for PII masking functions."""
    
    def test_mask_email(self):
        """Test email masking."""
        result = mask_email("john.doe@example.com")
        self.assertIn("@", result)
        self.assertNotEqual(result, "john.doe@example.com")
    
    def test_mask_phone(self):
        """Test phone masking."""
        result = mask_phone("123-456-7890")
        self.assertTrue(result.endswith("7890"))
        self.assertTrue(result.startswith("*"))
    
    def test_mask_national_id(self):
        """Test national ID masking."""
        result = mask_national_id("123-45-6789")
        self.assertTrue(result.endswith("6789"))
    
    def test_mask_password(self):
        """Test password masking."""
        result = mask_password("secret123")
        self.assertEqual(result, "*" * len("secret123"))
    
    def test_mask_pii_in_dict(self):
        """Test PII masking in dictionary."""
        data = {
            "email": "test@example.com",
            "phone": "123-456-7890",
            "name": "John Doe"
        }
        result = mask_pii_in_dict(data)
        self.assertNotEqual(result["email"], data["email"])


if __name__ == '__main__':
    unittest.main()

