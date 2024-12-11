install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

format:
	black main.py

lint:
	ruff check main.py

test:
	python -m pytest -vv --nbval --cov=library --cov=main test_*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

all: install format lint test