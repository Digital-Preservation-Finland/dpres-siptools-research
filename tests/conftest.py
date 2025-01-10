"""Configure py.test default values and functionality."""
import os
import sys
import uuid
from configparser import ConfigParser
from pathlib import Path

import luigi.configuration
import mongoengine
import mongomock
import pymongo
import pytest
import urllib3
from click.testing import CliRunner

import siptools_research.config
from siptools_research.__main__ import Context, cli
from tests.sftp import HomeDirMockServer, HomeDirSFTPServer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TEST_CONFIG_FILE = "tests/data/configuration_files/siptools_research.conf"
UNIT_TEST_SSL_CONFIG_FILE = \
    "tests/data/configuration_files/siptools_research_unit_test_ssl.conf"

SSH_KEY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "ssh", "test_ssh_key"
)

# Prefer modules from source directory rather than from site-python
PROJECT_ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.insert(0, PROJECT_ROOT_PATH)


@pytest.fixture(autouse=True)
def config(tmp_path):
    """Create temporary config file.

    A temporary packaging root directory is created, and configuration
    is modified to use it.
    """
    # Create temporary packaging root directory
    pkg_root = tmp_path / "packaging"
    pkg_root.mkdir()

    # Read sample config
    parser = ConfigParser()
    parser.read("tests/data/configuration_files/siptools_research_unit_test.conf")

    # Modify configuration
    parser.set("siptools_research", "packaging_root", str(pkg_root))

    # Write configuration to temporary file
    config_ = tmp_path / "siptools_research.conf"
    with config_.open('w') as file:
        parser.write(file)

    return config_


@pytest.fixture(autouse=True)
def mock_upload_conf():
    """Mock upload-rest-api database connection."""
    mongoengine.disconnect()
    mongoengine.connect("upload", host="mongodb://localhost",
                        tz_aware=True,
                        mongo_client_class=mongomock.MongoClient)


@pytest.fixture(scope="function")
def testmongoclient(monkeypatch):
    """Monkeypatch pymongo.MongoClient class.

    An instance of mongomock.MongoClient is created in beginning of
    test. Whenever pymongo.MongoClient() is called during the test, the
    already initialized mongomoc.MongoClient is used instead.

    :param monkeypatch: pytest `monkeypatch` fixture
    :returns: ``None``
    """
    mongoclient = mongomock.MongoClient()
    # pylint: disable=unused-argument

    def mock_mongoclient(*args, **kwargs):
        """Return already initialized mongomock.MongoClient."""
        return mongoclient
    monkeypatch.setattr(pymongo, 'MongoClient', mock_mongoclient)


@pytest.fixture(scope="function")
# TODO: This hack can be removed when TPASPKT-516 is resolved
def upload_projects_path(tmp_path, monkeypatch):
    """Configure UPLOAD_PROJECTS_PATH for upload-rest-api."""
    # Mock upload-rest-api configuration
    path = tmp_path / "upload_projects"
    path.mkdir()
    from upload_rest_api.config import CONFIG
    monkeypatch.setitem(CONFIG, 'UPLOAD_PROJECTS_PATH', path)
    return path


@pytest.fixture(scope="function")
# pylint: disable=redefined-outer-name
def workspace(config):
    """Create a temporary workspace directory.

    Creates directory `pkg_root / "workspaces / <identifier>"`
    that contains directory structure of a new workspace.

    :returns: Path to the workspce directory
    :rtype: pathlib.Path
    """
    config = siptools_research.config.Configuration(config)
    identifier = str(uuid.uuid4())
    workspace_ = Path(config.get("packaging_root")) / "workspaces" / identifier
    workspace_.parent.mkdir()
    workspace_.mkdir()
    (workspace_ / "preservation").mkdir()
    return workspace_


@pytest.fixture(scope="function")
def mock_luigi_config_path(monkeypatch):
    """Patch luigi config file.

    Replace system luigi configuration file (/etc/luigi/luigi.cfg) with
    local sample config file.

    :param monkeypatch: pytest `monkeypatch` fixture
    :returns: ``None``
    """
    monkeypatch.setattr(luigi.configuration.LuigiConfigParser,
                        '_config_paths',
                        ['tests/data/configuration_files/luigi.cfg'])


@pytest.yield_fixture(scope="function")
def sftp_dir(tmpdir):
    """Local directory that corresponds to the DPS' SFTP server."""
    sftp_dir_ = tmpdir.mkdir("sftp_server")
    yield Path(str(sftp_dir_))


@pytest.yield_fixture(scope="function")
def sftp_server(sftp_dir, monkeypatch):
    """Return a directory in the test SFTP filesystem."""
    users = {
        "tpas": SSH_KEY_PATH
    }

    monkeypatch.setattr("mockssh.sftp.SFTPServer", HomeDirSFTPServer)

    with HomeDirMockServer(users, home_dir=str(sftp_dir)) as server:
        yield server


@pytest.fixture(scope="function")
def config_creator(tmpdir):
    """
    Factory functioon to create a modified config file and return a path to
    the configuration file
    """
    config_dir = tmpdir.mkdir("config")

    def wrapper(config_path, new_config):
        config = ConfigParser()
        config.read(config_path)

        for section in new_config.keys():
            if section not in config:
                config[section] = {}

            for key, value in new_config[section].items():
                config[section][key] = str(value)

        config_path = str(config_dir.join("config.ini"))
        with open(config_path, "w") as file_:
            config.write(file_)

        return config_path

    return wrapper


@pytest.fixture(scope="function")
def luigi_mock_ssh_config(config, config_creator, sftp_dir, sftp_server):
    """
    Luigi configuration file that connects to a mocked DPS SFTP server
    instead of a real instance
    """
    config_path = config_creator(
        config_path=config,
        new_config={
            "siptools_research": {
                "dp_host": "127.0.0.1",
                "dp_port": sftp_server.port,
                "dp_ssh_key": SSH_KEY_PATH
            }
        }
    )

    return config_path


@pytest.fixture(scope="function")
def cli_runner():
    """
    Run the CLI entrypoint using the provided arguments and return the
    result
    """
    def wrapper(args, **kwargs):
        """
        Run the CLI entrypoint using provided arguments and return
        the result.
        """
        runner = CliRunner()

        result = runner.invoke(
            cli, args, obj=Context(), catch_exceptions=False, **kwargs
        )
        return result

    return wrapper
