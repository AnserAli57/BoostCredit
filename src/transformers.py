import pandas as pd
from typing import List, Dict
from .utils.pii_masking import mask_email, mask_phone, mask_national_id, mask_password, mask_address, mask_name
from .utils.transform_helpers import parse_timestamp, parse_date, to_boolean


class CSVTransformer:
    def transform(self, data: pd.DataFrame):
        df = data.copy()
        
        if 'id' not in df.columns:
            df['id'] = range(1, len(df) + 1)
        
        if df['id'].duplicated().any():
            df['id'] = range(1, len(df) + 1)
        
        if 'created_at' in df.columns:
            df['created_at'] = df['created_at'].apply(parse_timestamp)
        
        if 'last_login' in df.columns:
            df['last_login'] = df['last_login'].apply(parse_timestamp)
        
        if 'is_claimed' in df.columns:
            df['is_claimed'] = df['is_claimed'].apply(to_boolean)
        
        if 'paid_amount' in df.columns:
            df['paid_amount'] = pd.to_numeric(df['paid_amount'], errors='coerce').round(2)
        
        if 'name' in df.columns:
            df['name'] = df['name'].apply(lambda x: mask_name(x) if pd.notna(x) else x)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(lambda x: mask_address(x) if pd.notna(x) else x)
        
        return df
    
class JSONTransformer:
    def transform(self, data: List[Dict]):
        users_data = []
        telephone_numbers_data = []
        jobs_history_data = []
        
        for record in data:
            user_id = record.get('user_id')
            if not user_id:
                continue
            
            user_details = record.get('user_details', {})
            
            user_record = {
                'user_id': user_id,
                'created_at': parse_timestamp(record.get('created_at', None)),
                'updated_at': parse_timestamp(record.get('updated_at', None)),
                'logged_at': parse_timestamp(record.get('logged_at', None)),
                'name': mask_name(user_details.get('name', None)),
                'dob': parse_date(user_details.get('dob', None)),
                'address': mask_address(user_details.get('address', None)),
                'username': mask_email(user_details.get('username', None)),
                'password': mask_password(user_details.get('password', None)),
                'national_id': mask_national_id(user_details.get('national_id', None)),
            }
            users_data.append(user_record)
            
            telephone_numbers = user_details.get('telephone_numbers', [])
            if isinstance(telephone_numbers, list):
                for tel_num in telephone_numbers:
                    telephone_numbers_data.append({
                        'user_id': user_id,
                        'telephone_number': mask_phone(tel_num) if tel_num else None
                    })
            
            jobs_history = record.get('jobs_history', [])
            if isinstance(jobs_history, list):
                for job in jobs_history:
                    jobs_history_data.append({
                        'job_id': job.get('id', None),
                        'user_id': user_id,
                        'occupation': job.get('occupation', None),
                        'is_fulltime': to_boolean(job.get('is_fulltime', None)),
                        'start': parse_date(job.get('start', None)),
                        'end': parse_date(job.get('end', None)),
                        'employer': job.get('employer', None)
                    })
        
        return {
            'users': pd.DataFrame(users_data),
            'telephone_numbers': pd.DataFrame(telephone_numbers_data),
            'jobs_history': pd.DataFrame(jobs_history_data)
        }
