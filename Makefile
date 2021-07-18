## Requirements

.PHONY: minimum-requirements
minimum-requirements:
	@PYTHONPATH=. python -m pip install -U -r requirements.txt

.PHONY: requirements-test
requirements-test:
	@PYTHONPATH=. python -m pip install -r requirements.test.txt

.PHONY: requirements-lint
requirements-lint:
	@PYTHONPATH=. python -m pip install -r requirements.lint.txt

.PHONY: requirements-docs
requirements-docs:
	@PYTHONPATH=. python -m pip install -r docs/requirements.txt

.PHONY: requirements
## All requirements except docs
requirements: minimum-requirements requirements-test requirements-lint

## Style Checks

.PHONY: check-black
check-black:
	@echo ""
	@echo "Code Style With Black"
	@echo "=========="
	@echo ""
	@python -m black --check -t py38 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" . && echo "\n\nSuccess" || (echo "\n\nFailure\n\nRun \"make black\" to apply style formatting to your code"; return 2)
	@echo ""

.PHONY: check-flake8
check-flake8:
	@echo ""
	@echo "Flake 8 Lint"
	@echo "======="
	@echo ""
	@-python -m flake8 meli_challenge/ && echo "meli_challenge module success"
	@-python -m flake8 tests/ && echo "tests module success"
	@echo ""

.PHONY: checks
checks:
	@echo ""
	@echo "Code Style With Black & Flake 8 Lint"
	@echo "--------------------"
	@echo ""
	@make check-black
	@make check-flake8
	@echo ""

.PHONY: black
black:
	@python -m black -t py38 --exclude="build/|buck-out/|dist/|_build/|pip/|\.pip/|\.git/|\.hg/|\.mypy_cache/|\.tox/|\.venv/" .

## Tests

.PHONY: tests
tests:
	@python3 -m pytest --cov-branch --cov-report term-missing --cov=meli_challenge tests/

## Build wheel

.PHONY: build-wheel
build:
	@PYTHONPATH=. python -m setup sdist bdist_wheel

## Clean Data

.PHONY: clean
clean:
	@find ./ -type d -name 'htmlcov' -exec rm -rf {} +;
	@find ./ -type d -name 'coverage.xml' -exec rm -rf {} +;
	@find ./ -type f -name 'coverage-badge.svg' -exec rm -f {} \;
	@find ./ -type f -name '.coverage' -exec rm -f {} \;
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
