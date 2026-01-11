setup:
	@echo "Setting up database..."
	docker-compose up -d postgres-db
	@echo "Waiting for database to be ready..."
	@sleep 3

# Local execution (runs Python directly)
run-csv: setup
	@. ./setup.sh; FILE=$${FILE:-test.csv}; STORE_KEY=$${STORE_KEY:-csv_data}; python3 main.py --mode csv --file $$FILE --store-key $$STORE_KEY

run-json: setup
	@. ./setup.sh; FILE=$${FILE:-test.json}; STORE_KEY=$${STORE_KEY:-json_data}; python3 main.py --mode json --file $$FILE --store-key $$STORE_KEY

# Docker execution (runs in container, waits for DB)
run-csv-docker:
	FILE=$${FILE:-test.csv} STORE_KEY=$${STORE_KEY:-csv_data} docker-compose up etl-pipeline-csv

run-json-docker:
	FILE=$${FILE:-test.json} STORE_KEY=$${STORE_KEY:-json_data} docker-compose up etl-pipeline-json

build:
	docker-compose build

db-up:
	docker-compose up -d postgres-db

db-down:
	docker-compose down

logs:
	docker-compose logs -f etl-pipeline-csv

logs-json:
	docker-compose logs -f etl-pipeline-json

logs-db:
	docker-compose logs -f postgres-db

test:
	python3 -m pytest tests/ -v
