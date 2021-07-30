from typer.testing import CliRunner
from frictionless import helpers
from frictionless_dfour import program


runner = CliRunner()
IS_UNIX = not helpers.is_platform("windows")

# General


def test_program_workspace(dfour_url, dfour_dialect):
    result = runner.invoke(
        program, f"workspace {dfour_dialect.workspaceHash} sample -e {dfour_url} -y"
    )
    assert result.exit_code == 0
    # if IS_UNIX:
    #     assert result.stdout.count("metadata: data/table.csv")
    #     assert result.stdout.count("hash: 6c2c61dd9b0e9c6876139a449ed87933")
