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


@pytest.fixture(autouse=True)
def config(tmp_path):
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

    # Write configuration to temporary file
    config_ = tmp_path / "siptools_research.conf"
    with config_.open('w') as file:
        parser.write(file)

    return str(config_)


@pytest.fixture(autouse=True)
def mock_mongoengine(monkeypatch):
    """Mock upload-rest-api and siptools-research database connections."""
    mongoengine.disconnect()
    mongoengine.connect("upload", host="mongodb://localhost",
                        tz_aware=True,
                        mongo_client_class=mongomock.MongoClient)

    def mock_connect_mongoengine(config_path):
        mongoengine.register_connection(
            host="mongodb://localhost",
            alias="siptools_research",
            tz_aware=True,
            mongo_client_class=mongomock.MongoClient
        )

    mongoengine.disconnect(alias="siptools_research")

    # TODO: Monkey patching is tiresome and needs to be performed everywhere.
    # We should just move to using MongoBox as with pretty much every
    # DPRES project at this point.
    db_imports = [
        "siptools_research.database.connect_mongoengine",
        "siptools_research.dataset.connect_mongoengine",
        "siptools_research.workflow_init.connect_mongoengine",
        "siptools_research.__main__.connect_mongoengine",
        "siptools_research.metadata_generator.connect_mongoengine",
        "research_rest_api.app.connect_mongoengine"
    ]

    for db_import_path in db_imports:
        monkeypatch.setattr(
            db_import_path,
            mock_connect_mongoengine
        )

    mock_connect_mongoengine("foo")

    yield

    # Delete existing entries after test
    FileError.objects.delete()
    DatasetWorkflowEntry.objects.delete()


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
