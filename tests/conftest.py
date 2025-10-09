"""Configure py.test default values and functionality."""
import uuid
from configparser import ConfigParser
from pathlib import Path
from io import StringIO

import luigi.configuration
import mongoengine
import pytest
import urllib3
from upload_rest_api.config import CONFIG
from click.testing import CliRunner
from mongobox import MongoBox

import siptools_research.config
from siptools_research.app import create_app
from siptools_research.__main__ import Context, cli
from siptools_research.database import connect_mongoengine
from tests.sftp import HomeDirMockServer, HomeDirSFTPServer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SSH_KEY_PATH = Path("tests/data/ssh/test_ssh_key")


@pytest.fixture(name="luigi_config_fx")
def _luigi_config_fx(monkeypatch):
    """Mock luigi configuration parser.

    Writes temporary configuration file for Luigi and force it to read
    it. Note that the default luigi configuration file
    (/etc/luigi/luigi.cfg), will be read during tests anyway, because it
    is read when luigi is imported.
    """

    def _create_buffer(config_str):
        """Return StringIO buffer for given config_str"""
        config_str = '\n'.join(
            [x.strip() for x in config_str.strip().splitlines()])
        return StringIO(config_str)

    def _get_parser(_file):
        """Return LuigiConfigParser for given _file object"""
        parser = luigi.configuration.LuigiConfigParser()
        parser.read_file(_file)
        return parser

    def _set_config(config_str):

        parser = _get_parser(_create_buffer(config_str))

        monkeypatch.setattr(
            luigi.configuration.LuigiConfigParser, '_config_paths', [])
        monkeypatch.setattr(
            luigi.configuration.LuigiConfigParser, '_instance', parser)

    return _set_config


@pytest.fixture(autouse=True, name="config")
def _config(test_mongo, tmp_path, luigi_config_fx):
    """Create temporary config file and mock Luigi configuration.

    A temporary packaging root directory is created, and configuration
    is modified to use it. Also, luigi configuration file is mocked to
    prevent luigi from reading /etc/luigi/luigi.cfg.

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


@pytest.fixture(autouse=True, scope="session", name="test_mongo")
def _test_mongo():
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


@pytest.fixture(autouse=True)
def mongo_cleanup(test_mongo):
    """
    Clear the database before each test
    """
    test_mongo.drop_database("siptools-research")
    return


@pytest.fixture(autouse=True)
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


@pytest.fixture
# TODO: This hack can be removed when TPASPKT-516 is resolved
def upload_projects_path(tmp_path, monkeypatch):
    """Configure UPLOAD_PROJECTS_PATH for upload-rest-api."""
    # Mock upload-rest-api configuration
    path = tmp_path / "upload_projects"
    path.mkdir()
    monkeypatch.setitem(CONFIG, 'UPLOAD_PROJECTS_PATH', path)
    return path


@pytest.fixture
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


@pytest.fixture(name="sftp_dir")
def _sftp_dir(tmpdir):
    """Local directory that corresponds to the DPS' SFTP server."""
    sftp_dir_ = tmpdir.mkdir("sftp_server")
    return Path(str(sftp_dir_))


@pytest.fixture(name="sftp_server")
def _sftp_server(sftp_dir, monkeypatch):
    """Return a directory in the test SFTP filesystem."""
    users = {
        "tpas": SSH_KEY_PATH
    }

    monkeypatch.setattr("mockssh.sftp.SFTPServer", HomeDirSFTPServer)

    with HomeDirMockServer(users, home_dir=str(sftp_dir)) as server:
        yield server


@pytest.fixture
def mock_ssh_config(tmpdir, config, sftp_server):
    """Create modified configuration file to use mocked DPS SFTP server
    instead of a real instance.

    :returns: Configuration file path
    """
    parser = ConfigParser()
    parser.read(config)
    parser.set("siptools_research", "dp_host", "127.0.0.1")
    parser.set("siptools_research", "dp_port", str(sftp_server.port))
    parser.set("siptools_research", "dp_ssh_key", str(SSH_KEY_PATH))
    mocked_ssh_config = tmpdir / "siptools_research_mock_ssh.conf"
    with mocked_ssh_config.open('w') as file:
        parser.write(file)

    return mocked_ssh_config


@pytest.fixture
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


@pytest.fixture
def app(config, monkeypatch):
    """Create web app and Mock Metax HTTP responses.

    :returns: An instance of the REST API web app.
    """
    monkeypatch.setattr(
        "siptools_research.app.SIPTOOLS_RESEARCH_CONF", config
    )

    # Create app and change the default config file path
    app_ = create_app()
    app_.config["TESTING"] = True

    # Create temporary directories
    conf = siptools_research.config.Configuration(config)
    cache_dir = Path(conf.get("packaging_root")) / "file_cache"
    cache_dir.mkdir()
    tmp_dir = Path(conf.get("packaging_root")) / "tmp"
    tmp_dir.mkdir()

    # Reconnect to MongoDB database
    connect_mongoengine(config)

    return app_


@pytest.fixture
def client(app):
    """Create a Flask test client."""
    with app.test_client() as client:
        yield client
