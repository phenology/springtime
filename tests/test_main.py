import subprocess

from click.testing import CliRunner

from springtime.main import cli


def test_cli():
    runner = CliRunner()
    runner.invoke(cli, ["--help"])
    subprocess.run("springtime --help", shell=True)

    # TODO after dropping pyphenology (#116) we need new sample data
    # runner.invoke(cli, ["--recipe", "tests/recipes/pyphenology.yaml"])
    # subprocess.run("springtime tests/recipes/pyphenology.yaml", shell=True)
