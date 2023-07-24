"""Configure py.test default values and functionality."""
from configparser import ConfigParser
import logging
import os
import shutil
import sys
from pathlib import Path

import luigi.configuration
import mongomock
import pymongo
import pytest
import urllib3

import mongoengine

import siptools_research.metadata_generator
import siptools_research.utils.mimetypes
from tests.sftp import HomeDirMockServer, HomeDirSFTPServer


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_URL = "https://metaksi/rest/v2"
TEST_CONFIG_FILE = "tests/data/configuration_files/siptools_research.conf"
UNIT_TEST_CONFIG_FILE = \
    "tests/data/configuration_files/siptools_research_unit_test.conf"
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
def mock_os_link(monkeypatch):
    """Patch os.link with shutil.copyfile."""
    monkeypatch.setattr(os, "link", shutil.copyfile)


@pytest.fixture(autouse=True)
def mock_upload_conf(monkeypatch):
    mongoengine.disconnect()
    # TODO remove support for mongoengine 0.24.x when RHEL9 migration is done
    if mongoengine.__version__ <= "0.24.2":
        mongoengine.connect(host="mongomock://localhost/upload", tz_aware=True)
    else:
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
def testpath(tmpdir):
    """
    Create a temporary test directory and return a pathlib.Path object.

    This is pretty much identical to `tmp_path` fixture found in pytest
    3.9.0 and up, and can be replaced accordingly once that is
    available.
    """
    # TODO: Replace `testpath` with built-in `tmp_path` in pytest 3.9.0 and up
    return Path(str(tmpdir))


@pytest.fixture(scope="function")
# TODO: Replace tmpdir fixture with tmp_path fixture when pytest>=3.9.1
# is available on Centos
# pylint: disable=redefined-outer-name
def pkg_root(testpath, monkeypatch):
    """Create a temporary packaging root directory.

    Mocks configuration module to use the temporary directory as
    packaging root directory.

    :param testpath: pathlib.Path object
    :param monkeypatch: monkeypatch object
    :returns: pathlib.Path pointing to temporary directory
    """

    def _mock_get(self, parameter):
        """Mock get method."""
        if parameter == "packaging_root":
            return str(testpath / "packaging")
        # pylint: disable=protected-access
        return self._parser.get(self.config_section, parameter)

    monkeypatch.setattr(
        siptools_research.config.Configuration, "get", _mock_get
    )

    pkg_root_ = testpath / "packaging"
    pkg_root_.mkdir()

    # Create required directory structure in workspace root
    (pkg_root_ / "tmp").mkdir()
    (pkg_root_ / "file_cache").mkdir()
    (pkg_root_ / "workspaces").mkdir()

    return pkg_root_


@pytest.fixture(scope="function")
# TODO: This hack can be removed when TPASPKT-516 is resolved
def upload_projects_path(testpath, monkeypatch):
    """Configure UPLOAD_PROJECTS_PATH for upload-rest-api."""
    # Mock upload-rest-api configuration
    path = testpath / "upload_projects"
    path.mkdir()
    from upload_rest_api.config import CONFIG
    monkeypatch.setitem(CONFIG, 'UPLOAD_PROJECTS_PATH', path)
    return path


@pytest.fixture(scope="function")
# pylint: disable=redefined-outer-name
def workspace(pkg_root):
    """
    Create a temporary workspace directory.

    This is a shorthand for `pkg_root / "workspaces / "workspace"`.

    :returns: Path to the workspce directory
    :rtype: pathlib.Path
    """
    workspace_ = pkg_root / "workspaces" / "workspace"
    workspace_.mkdir()
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


@pytest.fixture(scope="function")
def mock_filetype_conf(monkeypatch):
    """Monkeypatch supported filetypes config file path.

    :param monkeypatch: pytest `monkeypatch` fixture
    :returns: ``None``
    """
    # Patching DEFAULT_CONFIG variable would not affect is_supported
    # -function default arguments. Therefore, the argument defaults are
    # patched instead.
    monkeypatch.setattr(siptools_research.utils.mimetypes.is_supported,
                        "__defaults__",
                        ('include/etc/dpres_mimetypes.json',))


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
def luigi_mock_ssh_config(config_creator, sftp_dir, sftp_server):
    """
    Luigi configuration file that connects to a mocked DPS SFTP server
    instead of a real instance
    """
    config_path = config_creator(
        config_path=TEST_CONFIG_FILE,
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
def mock_ida_download(requests_mock):
    """Mock the IDA download authorization endpoint."""
    requests_mock.post(
        "https://download.dl-authorize.test/authorize",
        json={
            "token": (
                "eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0."
                "eyJzZWNyZXQiOiJkUXc0dzlXZ1hjUSJ9."
            )
        }
    )
