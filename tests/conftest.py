import os
import pytest
from pytest_cov.embed import cleanup_on_sigterm
from dotenv import load_dotenv
from frictionless_dfour import DfourDialect


load_dotenv(".env")

# Cleanups

cleanup_on_sigterm()

# Fixtures


@pytest.fixture
def dfour_dialect():
    dialect = DfourDialect()
    workspace = os.environ.get("DFOUR_WORKSPACE")
    snapshot = os.environ.get("DFOUR_SNAPSHOT")
    username = os.environ.get("DFOUR_USERNAME")
    password = os.environ.get("DFOUR_PASSWORD")
    if not workspace or not username or not password:
        pytest.skip('Environment variable "DFOUR_WORKSPACE", "DFOUR_USERNAME" or "DFOUR_PASSWORD" is not available')

    dialect.workspaceHash = workspace
    dialect.snapshotHash = snapshot
    dialect.username = username
    dialect.password = password
    dialect.snapshotTopic = "Upload Test"
    dialect.bfsMunicipality = 3298

    yield dialect


@pytest.fixture
def dfour_url():
    url = url = os.environ.get("DFOUR_ENDPOINT")
    if not url:
        pytest.skip('Environment variable "DFOUR_WORKSPACE" is not available')
    yield url

# Settings


def pytest_addoption(parser):
    parser.addoption(
        "--ci",
        action="store_true",
        dest="ci",
        default=False,
        help="enable integrational tests",
    )


def pytest_configure(config):
    if not config.option.ci:
        expr = getattr(config.option, "markexpr")
        setattr(config.option, "markexpr",
                "{expr} and not ci" if expr else "not ci")
