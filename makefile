DIR_SRC_FILES = $(shell find . -type f -name "*.py" \
-not -path "./idea/*" \
-not -path "./build/*" \
-not -path "./.venv/*" \
-not -path "./source_data/*" \
-not -path "./dist/*" \
)
setup:
	python3 -m pip install --upgrade pip
	python3 -m pip install --upgrade setuptools
	python3 -m pip install --upgrade wheel
	python3 -m pip install -r requirements.txt

unit_test:
	python3 -m pytest -vv -s --cache-clear tst/main/*

e2e_run:
	python3 -m src.main.flow

build:
	python3 setup.py bdist_wheel
