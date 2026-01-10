"""
PII (Personally Identifiable Information) masking utilities.
"""
import re
from typing import Any, Dict, List


def mask_email(email: str) -> str:
    """Mask email address, keeping domain visible."""
    if not email or not isinstance(email, str):
        return email
    parts = email.split('@')
    if len(parts) == 2:
        username = parts[0]
        domain = parts[1]
        if len(username) > 2:
            masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
        else:
            masked_username = '*' * len(username)
        return f"{masked_username}@{domain}"
    return email


def mask_phone(phone: str) -> str:
    """Mask phone number, keeping last 4 digits."""
    if not phone or not isinstance(phone, str):
        return phone
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 4:
        return '*' * (len(digits) - 4) + digits[-4:]
    return '*' * len(digits)


def mask_national_id(national_id: str) -> str:
    """Mask national ID, keeping last 4 digits."""
    if not national_id or not isinstance(national_id, str):
        return national_id
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', national_id)
    if len(digits) >= 4:
        return '*' * (len(digits) - 4) + digits[-4:]
    return '*' * len(digits)


def mask_address(address: str) -> str:
    """Mask address, keeping city and state."""
    if not address or not isinstance(address, str):
        return address
    # Try to extract city and state (usually at the end)
    # Format: "Street Address\nCity, State ZIP"
    parts = address.split('\n')
    if len(parts) >= 2:
        street = parts[0]
        city_state = parts[1]
        masked_street = '*' * len(street)
        return f"{masked_street}\n{city_state}"
    return '*' * len(address)


def mask_password(password: str) -> str:
    """Mask password completely."""
    if not password or not isinstance(password, str):
        return password
    return '*' * len(password)


def mask_pii_in_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mask PII fields in a dictionary.
    
    Args:
        data: Dictionary containing potentially sensitive data
        
    Returns:
        Dictionary with masked PII fields
    """
    masked_data = data.copy()
    
    # Mask common PII fields
    if 'email' in masked_data:
        masked_data['email'] = mask_email(masked_data['email'])
    if 'username' in masked_data:
        masked_data['username'] = mask_email(masked_data['username'])  # Username might be email
    if 'password' in masked_data:
        masked_data['password'] = mask_password(masked_data['password'])
    if 'national_id' in masked_data:
        masked_data['national_id'] = mask_national_id(masked_data['national_id'])
    if 'address' in masked_data:
        masked_data['address'] = mask_address(masked_data['address'])
    if 'telephone_numbers' in masked_data and isinstance(masked_data['telephone_numbers'], list):
        masked_data['telephone_numbers'] = [mask_phone(phone) for phone in masked_data['telephone_numbers']]
    
    return masked_data

