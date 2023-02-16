#

This package is built with [hatch](https://hatch.pypa.io/latest/).

```bash
# Clone the repo
git clone git@github.com:phenology/springtime
cd springtime

# Create development environment for springtime
hatch env create

# Enter/activate development environment
hatch shell
springtime --help
exit  # to get out/deactivate

# Alternatively, use hatch run to execute command in default env
hatch run springtime --help

# Testing etc
hatch run pytest
hatch run isort src tests
hatch run black src tests

# Building docs
hatch run docs:build
# preview docs on http://localhost:8000/
hatch run docs:serve
```
