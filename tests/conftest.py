"""Configure py.test default values and functionality."""
import os
import sys
import uuid
from configparser import ConfigParser
from pathlib import Path
from io import StringIO

import luigi.configuration
import mongoengine
import pytest
import urllib3
from click.testing import CliRunner
from mongobox import MongoBox

import siptools_research.config
from research_rest_api.app import create_app
from siptools_research.__main__ import Context, cli
from siptools_research.database import connect_mongoengine
from siptools_research.models.dataset_entry import DatasetWorkflowEntry
from siptools_research.models.file_error import FileError
from tests.sftp import HomeDirMockServer, HomeDirSFTPServer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SSH_KEY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data", "ssh", "test_ssh_key"
)

# Prefer modules from source directory rather than from site-python
PROJECT_ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.insert(0, PROJECT_ROOT_PATH)


@pytest.fixture()
def luigi_config_fx(monkeypatch):
    """Write temporary configuration file for Luigi and force it to read it"""

    import luigi.configuration

    def _create_buffer(config_str):
        """Return StringIO buffer for given config_str"""
        config_str = '\n'.join(
            [x.strip() for x in config_str.strip().splitlines()])
        return StringIO(config_str)

    def _get_parser(_file):
        """Return LuigiConfigParser for given _file object"""
        parser = luigi.configuration.LuigiConfigParser()
        parser.readfp(_file)
        return parser

    def _set_config(config_str):

        parser = _get_parser(_create_buffer(config_str))

        monkeypatch.setattr(
            luigi.configuration.LuigiConfigParser, '_config_paths', [])
        monkeypatch.setattr(
            luigi.configuration.LuigiConfigParser, '_instance', parser)

    return _set_config


@pytest.fixture(autouse=True)
def config(test_mongo, tmp_path, luigi_config_fx):
    """Create temporary config file.

    A temporary packaging root directory is created, and configuration
    is modified to use it.

    :param request: pytest CLI arguments.
    :param tmp_path: Temporary path where packaging root directory is
        created
    """
    # Create temporary packaging root directory
    pkg_root = tmp_path / "packaging"
    pkg_root.mkdir()

    # Load empty Luigi configuration
    luigi_config_fx("")

    # Read sample config
    parser = ConfigParser()
    parser.read("include/etc/siptools_research.conf")

    # Modify configuration
    parser.set(
        "siptools_research", "packaging_root", str(pkg_root)
    )
    parser.set(
        "siptools_research", "sip_sign_key", "tests/data/sip_sign_pas.pem"
    )
    parser.set(
        "siptools_research", "metax_ssl_verification", "False"
    )
    parser.set(
        "siptools_research", "fd_download_service_ssl_verification", "False"
    )
    parser.set(
        "siptools_research", "access_rest_api_ssl_verification", "False"
    )

    # Use test MongoDB database
    parser.set("siptools_research", "mongodb_host", "localhost")
    parser.set("siptools_research", "mongodb_port", str(test_mongo.PORT))

    # Write configuration to temporary file
    config_ = tmp_path / "siptools_research.conf"
    with config_.open('w') as file:
        parser.write(file)

    return str(config_)


@pytest.yield_fixture(autouse=True, scope="session")
def test_mongo():
    """
    Initialize MongoDB test instance and return MongoDB client instance for
    the database
    """
    box = MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port

    yield client

    box.stop()


@pytest.fixture(scope="function", autouse=True)
def mongo_cleanup(test_mongo, monkeypatch):
    """
    Clear the database before each test
    """
    test_mongo.drop_database("siptools-research")
    yield


@pytest.fixture(scope="function", autouse=True)
def upload_database(test_mongo):
    """
    Connect to upload-rest-api database
    """
    # TODO: upload-rest-api connects implicitly when the model class is
    # imported instead of siptools-research which connects explicitly
    # with a function call. It also doesn't have an explicit DB alias,
    # defaulting to 'default' instead.
    #
    # upload-rest-api should be updated to work similarly to siptools-research
    # to reduce confusion.
    mongoengine.disconnect()
    mongoengine.connect(
        "upload", host=f"mongodb://localhost:{test_mongo.PORT}",
        tz_aware=True
    )


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
def mock_ssh_config(tmpdir, config, sftp_server):
    """Create modified configuration file to use mocked DPS SFTP server
    instead of a real instance.

    :returns: Configuration file path
    """
    parser = ConfigParser()
    parser.read(config)
    parser.set("siptools_research", "dp_host", "127.0.0.1")
    parser.set("siptools_research", "dp_port", str(sftp_server.port))
    parser.set("siptools_research", "dp_ssh_key", SSH_KEY_PATH)
    mocked_ssh_config = tmpdir / "siptools_research_mock_ssh.conf"
    with mocked_ssh_config.open('w') as file:
        parser.write(file)

    return mocked_ssh_config


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


# TODO: Use the name argument for pytest.fixture decorator to solve the
# funcarg-shadowing-fixture problem, when support for pytest version 2.x
# is not required anymore (the name argument was introduced in pytest
# version 3.0).
@pytest.fixture(scope="function")
def app(config, monkeypatch):
    """Create web app and Mock Metax HTTP responses.

    :returns: An instance of the REST API web app.
    """
    monkeypatch.setattr(
        "research_rest_api.app.SIPTOOLS_RESEARCH_CONFIG", config
    )

    # Create app and change the default config file path
    app_ = create_app()
    app_.config["TESTING"] = True

    # Create temporary directories
    conf = siptools_research.config.Configuration(config)
    cache_dir = os.path.join(conf.get("packaging_root"), "file_cache")
    os.mkdir(cache_dir)
    tmp_dir = os.path.join(conf.get("packaging_root"), "tmp")
    os.mkdir(tmp_dir)

    # Reconnect to MongoDB database
    connect_mongoengine(config)

    return app_
