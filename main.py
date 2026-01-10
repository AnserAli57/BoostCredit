"""
Main entry point for the ETL pipeline.
"""
import os
import sys
from pathlib import Path
from src.pipeline import ETLPipeline
from src.utils.logger import setup_logger

logger = setup_logger()


def main():
    """Main function to run the ETL pipeline."""
    # Database connection string
    # Default to SQLite for simplicity, can be overridden with environment variable
    db_connection_string = os.getenv(
        'DATABASE_URL',
        'sqlite:///etl_database.db'
    )
    
    # File paths
    base_path = Path(__file__).parent
    csv_file = base_path / 'data' / 'test.csv'
    json_file = base_path / 'data' / 'test.json'
    
    # Initialize pipeline
    pipeline = ETLPipeline(db_connection_string)
    
    try:
        # Process CSV file
        if csv_file.exists():
            logger.info("=" * 60)
            logger.info("Processing CSV file")
            logger.info("=" * 60)
            pipeline.process_csv(str(csv_file), 'test')
        else:
            logger.warning(f"CSV file not found: {csv_file}")
        
        # Process JSON file
        if json_file.exists():
            logger.info("=" * 60)
            logger.info("Processing JSON file")
            logger.info("=" * 60)
            pipeline.process_json(str(json_file))
        else:
            logger.warning(f"JSON file not found: {json_file}")
        
        logger.info("=" * 60)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)
    
    finally:
        pipeline.close()


if __name__ == '__main__':
    main()

