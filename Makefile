setup:
	docker-compose build

run:
	docker-compose up

logs:
	docker-compose logs -f etl-pipeline

test:
	python3 -m pytest tests/ -v

test-coverage:
	python3 -m pytest tests/ --cov=src --cov-report=html
