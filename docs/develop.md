<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

## Requirements

This project requires Python and R. To simplify installation of the (indirect) R
depencencies we recommend creating a conda environment using [Mamba
forge](https://github.com/conda-forge/miniforge#mambaforge) with:

```shell
# Clone the repo
git clone git@github.com:phenology/springtime
cd springtime

# Create and activate environment
mamba env create --file environment.yml
conda activate springtime

# Install R requirements
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages(c("daymetr", "MODISTools", "phenocamr", "rnpn"), repos = "http://cran.us.r-project.org")'
```

If you already have a conda environment you can update the springtime dependencies with

```shell
mamba env update --file environment.yml --name <name of conda environment>
```

## Development setup

This package is built with [hatch](https://hatch.pypa.io/latest/).

```bash
# Create development environment for springtime
hatch env create

# Enter/activate development environment
hatch shell
springtime --help
exit  # get out/deactivate

# Alternatively, use hatch run to execute command in default env
hatch run springtime --help
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
```

## Licensing

In principle, we like to publish our code under a permissive Apache-2.0 license.
However, some of our dependencies are released under different licenses.
Therefore, we use multi-licensing where each file specifies its own licensing
conditions. We follow the REUSE specification, where every file should have
license information in its header or, if this is not possible, as a separate
`file.ext.license` file. Notebooks are by default licensed under AGPL via the
`.reuse/dep5` file.

When adding new code, you should verify that it is appropriately
licensed. For more info, see the [REUSE FAQ](https://reuse.software/tutorial/).
The [reuse](https://reuse.readthedocs.io/en/latest/index.html) tool can be used for
checking compliance with the REUSE specification.

```
# Check licenses
hatch run reuse lint
```

## Hatch run quality-checks

For convenience, we configured hatch to perform all the checks above with a
single command:

```bash
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

## Contributing guidelines

We welcome contributions from everyone. Please use our [GitHub issue
tracker](https://github.com/phenology/springtime/issues) for questions, ideas,
bug reports, or feature requests.

If you want to make a pull request:

1. discuss your idea first, before putting in a lot of effort
1. refer to the [developer
   documentation](https://springtime.readthedocs.io/en/latest/develop.html)
1. if needed, fork the repository to your own Github profile
1. work on your own feature branch
1. make sure the existing tests still work and add new tests (if necessary)
1. update or expand the documentation;
1. make sure your code follows the style guidelines
1. don't be afraid to ask help with any of the above steps. We're happy to help!

By participating in this project, you agree to abide by the [code of
conduct](https://github.com/phenology/springtime/blob/main/CODE_OF_CONDUCT.md).

## Code of conduct

We as members, contributors, and leaders pledge to make participation in our
community a harassment-free experience for everyone. We pledge to act and
interact in ways that contribute to an open, welcoming, diverse, inclusive, and
healthy community. For more information, see the
[contributor-covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
