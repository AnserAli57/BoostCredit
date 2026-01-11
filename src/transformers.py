import polars as pl
from typing import List, Dict
from .utils.pii_masking import mask_email, mask_phone, mask_national_id, mask_password, mask_address, mask_name
from .utils.transform_helpers import parse_timestamp, parse_date, to_boolean


class CSVTransformer:
    def transform(self, data: pl.DataFrame):
        df = data.clone()
        
        # Add ID column if missing
        if 'id' not in df.columns:
            df = df.with_columns(pl.int_range(1, df.height + 1).alias('id'))
        
        # Check and fix duplicates
        if df['id'].is_duplicated().any():
            df = df.with_columns(pl.int_range(1, df.height + 1).alias('id'))
        
        # Timestamp parsing with cleaning
        if 'created_at' in df.columns:
            df = df.with_columns(
                pl.col('created_at')
                .map_elements(parse_timestamp, return_dtype=pl.Datetime)
                .alias('created_at')
            )
        
        if 'last_login' in df.columns:
            # Handle Unix timestamps and string dates using helper function
            df = df.with_columns(
                pl.col('last_login')
                .map_elements(parse_timestamp, return_dtype=pl.Datetime)
                .alias('last_login')
            )
        
        # Boolean conversion
        if 'is_claimed' in df.columns:
            df = df.with_columns(
                pl.col('is_claimed')
                .map_elements(to_boolean, return_dtype=pl.Boolean)
                .alias('is_claimed')
            )
        
        if 'paid_amount' in df.columns:
            df = df.with_columns(
                pl.col('paid_amount').cast(pl.Float64, strict=False).round(2).alias('paid_amount')
            )
        
        # PII masking
        if 'name' in df.columns:
            df = df.with_columns(
                pl.col('name')
                .map_elements(mask_name, return_dtype=pl.Utf8)
                .alias('name')
            )
        
        if 'address' in df.columns:
            df = df.with_columns(
                pl.col('address')
                .map_elements(mask_address, return_dtype=pl.Utf8)
                .alias('address')
            )
        
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
        
        # Create DataFrames
        users_df = pl.DataFrame(users_data)
        telephone_numbers_df = pl.DataFrame(telephone_numbers_data)
        jobs_history_df = pl.DataFrame(jobs_history_data)
        
        # Remove duplicates based on user_id (keep the one with latest created_at)
        if not users_df.is_empty():
            # Sort by created_at descending (latest first), then remove duplicates keeping first (latest)
            if 'created_at' in users_df.columns:
                users_df = users_df.sort('created_at', descending=True, nulls_last=True)
            users_df = users_df.unique(subset=['user_id'], keep='first')
        
        # Filter telephone_numbers and jobs_history to only include valid user_ids
        if not users_df.is_empty() and not telephone_numbers_df.is_empty():
            valid_user_ids = users_df['user_id'].to_list()
            telephone_numbers_df = telephone_numbers_df.filter(
                pl.col('user_id').is_in(valid_user_ids)
            )
        
        if not users_df.is_empty() and not jobs_history_df.is_empty():
            valid_user_ids = users_df['user_id'].to_list()
            jobs_history_df = jobs_history_df.filter(
                pl.col('user_id').is_in(valid_user_ids)
            )
        
        return {
            'users': users_df,
            'telephone_numbers': telephone_numbers_df,
            'jobs_history': jobs_history_df
        }
