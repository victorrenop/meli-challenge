# Meli Challenge

Welcome to Meli Challenge!

The documentation on the API functionality, developing process and how to run is located at the `docs/` folder. To generate and read it, follow the last step at the [Useful Commands](#useful-commands) section. In this page you will find the challenge definition and basic commands on how to setup de environment.

## Table of Contents

- [Challenge Definition](#challenge-definition)
- [Useful Commands](#useful-commands)
- [Built With](#built-with)

## Challenge Definition

## Useful Commands

Some useful commands to help the developing process:

1. **Requirements:**
  - `make requirements`: installs all packages from all requirements files
  - `make requirements-lint`: installs all packages from `requirements.lint.txt` file
  - `make requirements-tests`: installs all packages from `requirements.tests.txt` file
2. **Lint:**
  - `make checks`: runs `black` and `flake8` lint checks
  - `make black`: runs only the `black` reformating process
  - `make check-flake8`: runs only the `flake8` lint process
  - `make check-black`: runs only the `black` lint process
3. **Tests:**
  - `make tests`: runs `pytest` with coverage
4. **Documentation:**

    The full documentation can be generated using the commands:
    ```bash
    $ make requirements-docs
    $ cd docs
    $ make html
    ```
    This generates the static HTML pages with the full documentation. You can access it by opening the file `docs/_build/html/index.html` on your browser.

## Built With

- [Python](https://www.python.org/) - Programming language.
- [Flake8](https://pypi.org/project/flake8/) - Lint tool.
- [Black](https://black.readthedocs.io/en/stable/) - Python code formater.
- [Cookiecutter](https://github.com/cookiecutter/cookiecutter) - A command-line utility that creates Python projects using templates.