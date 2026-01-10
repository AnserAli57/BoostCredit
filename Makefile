setup:
	docker-compose build

run:
	docker-compose up

logs:
	docker-compose logs -f etl-pipeline
