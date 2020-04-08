"""Configure py.test default values and functionality"""

import os
import sys
import logging
import shutil

import urllib3
import mongomock
import pymongo
import luigi.configuration
import pytest

from metax_access import Metax
import upload_rest_api

import siptools_research.metadata_generator
import siptools_research.utils.mimetypes
import tests.metax_data.datasets as datasets
import tests.metax_data.files as files


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_URL = "https://metaksi/rest/v1"
TEST_CONFIG_FILE = "tests/data/configuration_files/siptools_research.conf"
UNIT_TEST_CONFIG_FILE = \
    "tests/data/configuration_files/siptools_research_unit_test.conf"


# Prefer modules from source directory rather than from site-python
PROJECT_ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.insert(0, PROJECT_ROOT_PATH)


@pytest.fixture(autouse=True)
def mock_os_link(monkeypatch):
    """Patch os.link with shutil.copyfile"""
    monkeypatch.setattr(os, "link", shutil.copyfile)


@pytest.fixture(autouse=True)
def mock_upload_conf(monkeypatch):
    """Patch upload_rest_api configuration parsing"""
    monkeypatch.setattr(
        upload_rest_api.database, "parse_conf",
        lambda conf: {"MONGO_HOST": "localhost", "MONGO_PORT": 27017}
    )


@pytest.fixture(autouse=False)
def mock_metax_access(monkeypatch):
    """Mock metax_access GET requests to files or datasets to return
    mock functions from metax_data.datasets and metax_data.files modules.
    """
    monkeypatch.setattr(Metax, "get_dataset", datasets.get_dataset)
    monkeypatch.setattr(Metax, "get_dataset_files", datasets.get_dataset_files)
    monkeypatch.setattr(Metax, "get_file", files.get_file)


def _identifier_exists(identifier):
    """Check if identifier is defined in metax_data.datasets or
    metax_data.files
    """
    return identifier in files.FILES or identifier in datasets.DATASETS


@pytest.fixture(scope="function")
def testmongoclient(monkeypatch):
    """Monkeypatch pymongo.MongoClient class. An instance of
    mongomock.MongoClient is created in beginning of test. Whenever
    pymongo.MongoClient() is called during the test, the already initialized
    mongomoc.MongoClient is used instead.

    :param monkeypatch: pytest `monkeypatch` fixture
    :returns: ``None``
    """
    mongoclient = mongomock.MongoClient()
    # pylint: disable=unused-argument

    def mock_mongoclient(*args, **kwargs):
        """Returns already initialized mongomock.MongoClient"""
        return mongoclient
    monkeypatch.setattr(pymongo, 'MongoClient', mock_mongoclient)


@pytest.fixture(scope="function")
# TODO: Replace tmpdir fixture with tmp_path fixture when pytest>=3.9.1 is
# available on Centos
def testpath(tmpdir, monkeypatch):
    """Create a temporary workspace root directory and mock configuration
    module to use it as workspace root.

    :param tmpdir: py.path.local object
    :param monkeypatch: monkeypatch object
    :returns: path to temporary directory
    """

    def _mock_get(self, parameter):
        """Mock Configuration().get() to return temporary directory instead of
        path found in configuration file
        """
        if parameter == "packaging_root":
            return str(tmpdir)

        return self._parser.get(self.config_section, parameter)

    monkeypatch.setattr(
        siptools_research.config.Configuration, "get", _mock_get
    )

    # Create required directory structure in workspace root
    tmpdir.mkdir("tmp")
    tmpdir.mkdir("file_cache")
    tmpdir.mkdir("workspaces")

    return str(tmpdir)


@pytest.fixture(scope="function")
def mock_luigi_config_path(monkeypatch):
    """Monkeypatch luigi config file path to prevent using system luigi
    configuration file (/etc/luigi/luigi.cfg) in tests.

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
    # Patching DEFAULT_CONFIG variable would not affect is_supported -function
    # default arguments. Therefore, the argument defaults are patched instead.
    monkeypatch.setattr(siptools_research.utils.mimetypes.is_supported,
                        "__defaults__",
                        ('include/etc/dpres_mimetypes.json',))
