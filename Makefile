.DEFAULT_GOAL := all
black = black src tests
isort = isort src tests

.PHONY: install
install:
	pip install -U pip
	poetry install
	poetry run pre-commit install

.PHONY: up
up:
	docker-compose -f ./tests/docker-compose.yml up -d

.PHONY: down
down:
	docker-compose -f ./tests/docker-compose.yml down

.PHONY: dev
dev:
	poetry run uvicorn sql_data_service.main:app --reload

.PHONY: test
test:
	docker-compose -f ./tests/docker-compose.yml up -d
	poetry run pytest -s --cov src --cov-report=term-missing --cov-report=xml
	docker-compose -f ./tests/docker-compose.yml down

.PHONY: format
format:
	poetry run ${black}
	poetry run ${isort}

.PHONY: lint
lint:
	poetry run flake8 src tests
	poetry run ${black} --diff --check
	poetry run ${isort} --check-only
	poetry run mypy src tests

.PHONY: all
all: lint test
