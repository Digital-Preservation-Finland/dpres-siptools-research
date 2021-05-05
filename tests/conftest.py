"""Configure py.test default values and functionality."""
from __future__ import unicode_literals

import logging
import os
import shutil
import sys

import luigi.configuration
import mongomock
import pymongo
import pytest
import siptools_research.metadata_generator
import siptools_research.utils.mimetypes
import upload_rest_api
import urllib3
from metax_access import Metax

import tests.metax_data.contracts
import tests.metax_data.datasets
import tests.metax_data.files
from tests.sftp import HomeDirSFTPServer, HomeDirMockServer

try:
    from configparser import ConfigParser
except ImportError:  # Python 2
    from ConfigParser import ConfigParser

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_URL = "https://metaksi/rest/v1"
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
    """Patch upload_rest_api configuration parsing."""
    monkeypatch.setattr(
        upload_rest_api.database, "parse_conf",
        lambda conf: {"MONGO_HOST": "localhost", "MONGO_PORT": 27017}
    )


@pytest.fixture(autouse=False)
def mock_metax_access(monkeypatch):
    """Mock metax_access GET requests.

    Replaces get-methods of Metax object with mock functions from
    metax_data package.
    """
    monkeypatch.setattr(Metax,
                        "get_dataset",
                        tests.metax_data.datasets.get_dataset)
    monkeypatch.setattr(Metax,
                        "get_dataset_files",
                        tests.metax_data.datasets.get_dataset_files)
    monkeypatch.setattr(Metax,
                        "get_file",
                        tests.metax_data.files.get_file)


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
# TODO: Replace tmpdir fixture with tmp_path fixture when pytest>=3.9.1
# is available on Centos
def testpath(tmpdir, monkeypatch):
    """Create a temporary packaging root directory.

    Mocks configuration module to use the temporary directory as
    packaging root directory.

    :param tmpdir: py.path.local object
    :param monkeypatch: monkeypatch object
    :returns: path to temporary directory
    """

    def _mock_get(self, parameter):
        """Mock get method."""
        if parameter == "packaging_root":
            return str(tmpdir.join("packaging"))
        # pylint: disable=protected-access
        return self._parser.get(self.config_section, parameter)

    monkeypatch.setattr(
        siptools_research.config.Configuration, "get", _mock_get
    )

    pkg_root = tmpdir.mkdir("packaging")

    # Create required directory structure in workspace root
    pkg_root.mkdir("tmp")
    pkg_root.mkdir("file_cache")
    pkg_root.mkdir("workspaces")

    return str(pkg_root)


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
    """
    Local directory that corresponds to the DPS' SFTP server
    """
    sftp_dir_ = tmpdir.mkdir("sftp_server")
    yield sftp_dir_



@pytest.yield_fixture(scope="function")
def sftp_server(sftp_dir, monkeypatch):
    """
    Return a directory in the filesystem that is served by a test SFTP server
    """
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
