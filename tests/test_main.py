import subprocess

from click.testing import CliRunner

from springtime.main import Workflow, cli, main


def test_workflow():
    w = Workflow(
        datasets={
            "test1": dict(dataset="dummy", name="aspen", phenophase="budburst"),
            "testds2": dict(dataset="pyphenology", name="aspen", phenophase="budburst"),
        }
    )
    print(str(w))
    print(repr(w))
    w.execute()


def test_main():
    file = "tests/recipes/pyphenology.yaml"
    main(file)


def test_cli():
    runner = CliRunner()
    runner.invoke(cli, ["--help"])
    runner.invoke(cli, ["--recipe", "tests/recipes/pyphenology.yaml"])
    subprocess.run("springtime --help", shell=True)
    subprocess.run("springtime tests/recipes/pyphenology.yaml", shell=True)
