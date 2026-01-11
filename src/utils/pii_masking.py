import re


def mask_email(email):
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


def mask_phone(phone):
    if not phone or not isinstance(phone, str):
        return phone
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 4:
        return '*' * (len(digits) - 4) + digits[-4:]
    return '*' * len(digits)


def mask_national_id(national_id):
    if not national_id or not isinstance(national_id, str):
        return national_id
    digits = re.sub(r'\D', '', national_id)
    if len(digits) >= 4:
        return '*' * (len(digits) - 4) + digits[-4:]
    return '*' * len(digits)


def mask_address(address):
    if not address or not isinstance(address, str):
        return address
    parts = address.split('\n')
    if len(parts) >= 2:
        street = parts[0]
        city_state = parts[1]
        masked_street = '*' * len(street)
        return f"{masked_street}\n{city_state}"
    return '*' * len(address)


def mask_password(password):
    if not password or not isinstance(password, str):
        return password
    return '*' * len(password)


def mask_name(name):
    if not name or not isinstance(name, str):
        return name
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}*** {parts[-1][0]}***"
    elif len(parts) == 1:
        return f"{parts[0][0]}***"
    return "***"
