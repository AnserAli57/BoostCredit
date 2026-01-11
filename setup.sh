#!/bin/bash

# Install Python dependencies
if [ -f "requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip3 install -q -r requirements.txt
    echo "✓ Dependencies installed"
fi

# Database Configuration (used by ETL pipeline)
export DB_TYPE=${DB_TYPE:-postgresql}
export DB_HOST=${DB_HOST:-localhost}
export DB_PORT=${DB_PORT:-5432}
export DB_USER=${DB_USER:-etl_user}
export DB_PASSWORD=${DB_PASSWORD:-etl_password}
export DB_NAME=${DB_NAME:-etl_database}

# PostgreSQL Container Configuration (must match DB_* variables)
export POSTGRES_USER=${POSTGRES_USER:-${DB_USER}}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-${DB_PASSWORD}}
export POSTGRES_DB=${POSTGRES_DB:-${DB_NAME}}
export POSTGRES_PORT=${POSTGRES_PORT:-${DB_PORT}}

# Pipeline Configuration
export DATA_PATH=${DATA_PATH:-./data}
export OBJECT_STORE_PATH=${OBJECT_STORE_PATH:-./output}

echo "Environment variables set:"
echo "  DB_HOST: ${DB_HOST}"
echo "  DB_PORT: ${DB_PORT}"
echo "  DB_USER: ${DB_USER}"
echo "  DB_NAME: ${DB_NAME}"
