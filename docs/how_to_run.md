# How to Run

Here are the steps necessary to run all executables and test all of the features of this challenge solution. There are also steps on how to run pytest and coverage reports for code coverage.

## Install dependencies

To install all dependencies of this project, simply run on project's root:
```bash
$ make requirements
```

This will install all necessary dependencies for the following steps. Docker is an external dependency, so make sure to install it to be able to test the third challenge.

If you want to install specific dependencies, there are three other commands. The first is used to install only the minimum requirements that do not include lint and tests:

```bash
$ make minimum-requirements
```

If you want to install only the lint requirements:

```bash
$ make requirements-lint
```

And if you want to install only the test requirements:

```bash
$ make requirements-test
```

## The CLI

The CLI is used to display the output of the 2 first challenges. To use it, simply run on root folder:
```
python -m meli_challenge.westeros_cli -h
```

This will yield the list of possible arguments to test the solution.

To test the first challenge solution, run:
```
python -m meli_challenge.westeros_cli assets/input_data/dataset.csv --aggregate_interactions
```

To test the second challenge solution, run:
```
python -m meli_challenge.westeros_cli assets/input_data/dataset.csv --all_characters_mutual_friends
```

And to test the mutual friends method used by the REST API, run:
```
python -m meli_challenge.westeros_cli assets/input_data/dataset.csv --mutual_friends <Character 1> <Character 2>
```

Note that these commands will output the result in standard output following the formatting guide provided in the challenge.

## How to Start the REST API

The rest API can be executed in two ways: directly at the terminal or via Docker container.

You can access the REST API full documentation [here](rest_api.md).

### Running with Terminal
Run on project's root:
```bash
$ python -m meli_challenge.westeros_api
```

This will start the Flask service and requests will be listened through the address `localhost` and port `5000`. The terminal window used to start the service will now output API logs.

### Running with Docker
To start the API using Docker, run on project's root:

```bash
$ docker-compose up -d
```

For newer versions of docker you can run like this too:

```bash
$ docker compose up -d
```

This will build the image and start a container with the port 5000 ready to receive requests. If you want to see the API logs, attach to the container:

```bash
$ docker attach westeros_api
```

To make requests to the API in both methods of starting, simply make requests to the url: `http://localhost:5000/`. If you are running on MacOS and having issues with host not resolved, start the container and attach to it right after. With this, you should see a log message from flask stating on which local ip it's listening. Use this local ip to test the API.

## Run Tests and Coverage

To run all unit and integration tests with coverage report, simply run:

```bash
$ make tests
```
