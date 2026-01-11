# BoostCredit ETL Pipeline

## Setup

1. Start PostgreSQL database:
```bash
make setup
```

Or manually:
```bash
docker-compose up -d postgres-db
```

## Running the Pipeline

### Local Execution (Python directly)

#### CSV Mode
```bash
make run-csv
```

With custom file:
```bash
make run-csv FILE=myfile.csv STORE_KEY=my_data
```

#### JSON Mode
```bash
make run-json
```

With custom file:
```bash
make run-json FILE=myfile.json STORE_KEY=my_data
```

### Docker Execution (Container)

The ETL pipeline container automatically waits for PostgreSQL to be ready before starting.

#### CSV Mode
```bash
make run-csv-docker
```

#### JSON Mode
```bash
make run-json-docker
```

Build the container:
```bash
make build
```

## Database Management

Start database:
```bash
make db-up
```

Stop database:
```bash
make db-down
```

View logs:
```bash
make logs
```

## Testing

Run tests:
```bash
make test
```


## Project Structure

```
BoostCredit/
├── data/              # Input data files (CSV/JSON)
├── output/            # Object store (Parquet files)
├── logs/              # Pipeline logs
├── src/
│   ├── extractors.py  # CSV/JSON extractors
│   ├── transformers.py # Data transformers
│   ├── loaders.py      # SQL loader
│   ├── storage.py      # Object store
│   ├── pipeline.py     # ETL pipeline
│   └── utils/
│       ├── logger.py
│       ├── pii_masking.py
│       └── transform_helpers.py
├── tests/              # Unit tests
├── main.py            # Entry point
├── docker-compose.yml # PostgreSQL and ETL pipeline services
├── Dockerfile         # ETL pipeline container
├── setup.sh           # Environment variables
└── Makefile           # Commands

```

## Environment Variables

Set via `setup.sh`:
- `DB_HOST` - Database host (default: localhost)
- `DB_PORT` - Database port (default: 5432)
- `DB_USER` - Database user (default: etl_user)
- `DB_PASSWORD` - Database password (default: etl_password)
- `DB_NAME` - Database name (default: etl_database)
