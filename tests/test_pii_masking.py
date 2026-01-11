import unittest
from src.utils.pii_masking import (
    mask_email, mask_phone, mask_national_id,
    mask_address, mask_password
)


class TestPIIMasking(unittest.TestCase):
    def test_mask_email(self):
        result = mask_email("john.doe@example.com")
        self.assertIn("@", result)
        self.assertNotEqual(result, "john.doe@example.com")
    
    def test_mask_phone(self):
        result = mask_phone("123-456-7890")
        self.assertTrue(result.endswith("7890"))
        self.assertTrue(result.startswith("*"))
    
    def test_mask_national_id(self):
        result = mask_national_id("123-45-6789")
        self.assertTrue(result.endswith("6789"))
    
    def test_mask_password(self):
        result = mask_password("secret123")
        self.assertEqual(result, "*" * len("secret123"))
    
    def test_mask_address(self):
        result = mask_address("123 Main St\nCity, State 12345")
        self.assertIn("\n", result)
        self.assertTrue(result.startswith("*"))


if __name__ == '__main__':
    unittest.main()
