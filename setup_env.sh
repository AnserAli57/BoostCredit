#!/bin/bash

# Database Configuration
export DB_TYPE=${DB_TYPE:-postgresql}
export DB_HOST=${DB_HOST:-localhost}
export DB_PORT=${DB_PORT:-5432}
export DB_USER=${DB_USER:-etl_user}
export DB_PASSWORD=${DB_PASSWORD:-etl_password}
export DB_NAME=${DB_NAME:-etl_database}

# PostgreSQL Configuration (for docker-compose)
export POSTGRES_USER=${POSTGRES_USER:-etl_user}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-etl_password}
export POSTGRES_DB=${POSTGRES_DB:-etl_database}
export POSTGRES_PORT=${POSTGRES_PORT:-5432}

# Pipeline Configuration
export DATA_PATH=${DATA_PATH:-./data}
export OBJECT_STORE_PATH=${OBJECT_STORE_PATH:-./output}
export LOADER_TYPE=${LOADER_TYPE:-sql}

# File Configuration (set these based on your needs)
# export CSV_FILE=test.csv
# export JSON_FILE=test.json
# export STORE_KEY=csv_data

echo "Environment variables set:"
echo "  DB_TYPE: $DB_TYPE"
echo "  DB_HOST: $DB_HOST"
echo "  DB_PORT: $DB_PORT"
echo "  DB_USER: $DB_USER"
echo "  DB_NAME: $DB_NAME"
echo "  DATA_PATH: $DATA_PATH"
echo "  OBJECT_STORE_PATH: $OBJECT_STORE_PATH"
