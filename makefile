DIR_SRC_FILES = $(shell find . -type f -name "*.py" \
-not -path "./idea/*" \
-not -path "./build/*" \
-not -path "./.venv/*" \
-not -path "./source_data/*" \
-not -path "./dist/*" \
)

CMD_PYTEST_OPTIONS = -ra -vv --tb=short --cache-clear --color=yes --show-capture=no --strict-markers
unit_test:
	python3 -m pytest -vv -s --cache-clear tst/main/*

e2e_run:
	python3 -m src.main.flow

build:
	python3 setup.py bdist_wheel
