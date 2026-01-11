import os
import sys
import argparse
from src.pipeline import Pipeline
from src.utils.logger import setup_logger

logger = setup_logger()


def parse_args():
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument('--mode', required=True, choices=['csv', 'json'], help='Processing mode: csv or json')
    parser.add_argument('--store-key', help='Object store key')
    parser.add_argument('--db-type', help='Destination database type (e.g., postgresql, sqlite)', default='postgresql')
    parser.add_argument('--file', help='Input file path (CSV or JSON based on mode)')
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Set configs from args if provided, otherwise use ENV vars
    if args.store_key:
        os.environ['STORE_KEY'] = args.store_key
    
    if args.db_type:
        os.environ['DB_TYPE'] = args.db_type
    
    # Get file path from args or ENV
    if args.file:
        file_path = args.file
    elif args.mode == 'csv':
        file_path = os.getenv('CSV_FILE')
    elif args.mode == 'json':
        file_path = os.getenv('JSON_FILE')
    else:
        file_path = None
    
    if not file_path:
        raise ValueError(f"File path must be provided via --file argument or {args.mode.upper()}_FILE environment variable")
    
    pipeline = Pipeline()
    
    try:
        if args.mode == 'csv':
            pipeline.process_csv(file_path)
        elif args.mode == 'json':
            pipeline.process_json(file_path)
        
        logger.info("Pipeline completed")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        pipeline.close()


if __name__ == '__main__':
    main()
