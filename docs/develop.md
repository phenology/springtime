# Developer guide

If you want to make changes to springtime, you can [create an editable
installation from source](#install-from-source).

If your work might be useful for others, we encourage you to [contribute your
changes](#contributing-guidelines) to the package. We are happy to help!

Instructions on [adding datasets](../user_guide/datasets/adding_datasets) or [adding
models](../user_guide/modelling/adding_models) are part of the user guide.

This page lists everything you need to know about [code
formatting](#linting-and-formatting), [testing](#testing), and [writing
documentation](#documentation).

## Install from source

Follow the [installation instructions](../user_guide/installation) to set up your environment. Then clone the springtime git repo and create an editable installation with development dependencies:

```shell
# Clone the repo
git clone git@github.com:phenology/springtime
cd springtime
pip install --editable .[dev,docs,extras]
```

Now, whenever, you change the source code and restart the python interpreter,
your changes will be reflected.

### Using hatch

Alternatively, you can use hatch[hatch](https://hatch.pypa.io/latest/):

```bash
# Create development environment for springtime
hatch env create

# Enter/activate development environment
hatch shell
springtime --help
exit  # get out/deactivate

# Alternatively, use hatch run to execute commands in default env, e.g.
hatch run springtime --help
hatch run pytest
```

## Linting and formatting

We use [ruff](https://beta.ruff.rs/docs/) for linting and
[black](https://black.readthedocs.io/en/stable/) for formatting.
[Mypy](https://mypy-lang.org/) is used for type checking.

```bash
# Apply black automatic formatting to both src/ and tests/
hatch run black src tests

# See what ruff can do for you
hatch run ruff help

# Lint all files in src/ and tests/ with all linters
hatch run ruff check src tests

# Try to automatically fix issues
hatch run ruff check src tests --fix

# Run static type checking
hatch run mypy --install-types  # say yes
hatch run mypy --ignore-missing-imports src tests

# Run all of the above with a single command
hatch run quality-checks
```

## Testing

[Pytest](https://docs.pytest.org/en/7.2.x/) is used for running tests.

```bash
# Run all tests in tests/
hatch run pytest

# Run all tests from one file
hatch run pytest tests/test_main.py

# Run a single test
hatch run pytest tests/test_main.py::test_cli
```

To test examples in docstrings use:

```bash
hatch run doctest <file to test>
# For example
hatch run doctest src/springtime/utils.py
```

## Documentation

Documentation is build with [mkdocs](https://www.mkdocs.org/)

```bash
# Get help for mkdocs
hatch run mkdocs --help
hatch run mkdocs serve --help

# Start a development server with auto reloads
hatch run mkdocs serve

# Build the documentation for deployment
hatch run mkdocs build --clean --strict
```

## Docker build

The docker image is hosted on [GitHub Container Registry
(GHCR)](https://github.com/phenology/springtime/pkgs/container/springtime).

You need to setup a personal access token following the instructions
[here](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic).

Then build and push the image to GHCR:

```bash
# In the root of the repository
docker build -t ghcr.io/phenology/springtime:latest .
docker push ghcr.io/phenology/springtime:latest
```

## Contributing guidelines

We welcome contributions from everyone. Please use our [GitHub issue
tracker](https://github.com/phenology/springtime/issues) for questions, ideas,
bug reports, or feature requests.

If you want to make a pull request:

1. discuss your idea first, before putting in a lot of effort
1. refer to the this documentation
1. if needed, fork the repository to your own Github profile
1. work on your own feature branch
1. make sure the existing tests still work and add new tests (if necessary)
1. update or expand the documentation;
1. make sure your code follows the style guidelines

Don't hesitate seek assistance with any of these steps. We're happy to help!

## Code of conduct

By participating in this project, you agree to abide by the [code of
conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/) as
spelled out in the contributor-covenant version 2.1
