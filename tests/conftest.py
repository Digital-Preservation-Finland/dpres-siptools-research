"""Configure py.test default values and functionality"""

import copy
import os
import sys
import logging
import shutil
import six

import urllib3
import mongomock
import pymongo
import luigi.configuration
import pytest
import lxml.etree

from metax_access import Metax
import upload_rest_api

import siptools_research.metadata_generator
import siptools_research.utils.mimetypes
import tests.metax_data.datasets
import tests.metax_data.files
import tests.metax_data.contracts


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


def mock_metax_dataset(requests_mock,
                       dataset=copy.deepcopy(
                           tests.metax_data.datasets.BASE_DATASET
                       ),
                       files=None,
                       contract=copy.deepcopy(
                           tests.metax_data.contracts.BASE_CONTRACT
                       )):
    """Mock responses of Metax datasets API, files API, contracts API, and
    directories API using requests-mock.

    Information about files and contract are inserted to dataset metadata.
    Metax directories API is mocked based on file paths and parent directories
    of provided file metadata. The mocked datacite meatada for dataset is hard
    coded. Technical metadata for audio, video, image etc. files is NOT mocked.
    The identifiers in provided resource dicts are used in mocked URLs.


    :param requests_mok: Mocker object used for creating responses
    :param dataset: dataset metadata dict
    :param files: list of file metadata dicts
    :param contract: contract metadata dict
    :returns: ``None``
    """
    if files is None:
        files = {}

    # Add contract to dataset
    dataset['contract']['identifier'] = contract['contract_json']['identifier']

    for file_ in files:
        # Add files to dataset
        dataset["research_dataset"]["files"].append(
            {
                "identifier": file_['identifier'],
                "use_category": {
                    "pref_label": {
                        "en": file_['identifier']
                    }
                }
            }
        )

        # Mock Metax contracts API
        requests_mock.get(
            "{}/directories/{}".format(
                METAX_URL, file_['parent_directory']['identifier']
            ),
            json={
                "identifier": file_['parent_directory']['identifier'],
                "directory_path": os.path.dirname(file_['file_path'])
            }
        )

        # Mock Metax files API
        requests_mock.get("{}/files/{}".format(METAX_URL, file_['identifier']),
                          json=file_)
        requests_mock.patch(
            "{}/files/{}".format(METAX_URL, file_['identifier'])
        )
        requests_mock.get(
            "{}/files/{}/xml".format(METAX_URL, file_['identifier']),
            json={}
        )
        requests_mock.post(
            "{}/files/{}/xml".format(METAX_URL, file_['identifier']),
            status_code=201
        )

    # Mock Metax datasets API
    requests_mock.get(
        "{}/datasets/{}".format(METAX_URL, dataset['identifier']),
        json=dataset
    )
    requests_mock.patch(
        "{}/datasets/{}".format(METAX_URL, dataset['identifier']),
        json=dataset
    )
    requests_mock.get(
        "{}/datasets/{}?dataset_format=datacite".format(METAX_URL,
                                                        dataset['identifier']),
        content=six.binary_type(
            lxml.etree.tostring(tests.metax_data.datasets.BASE_DATACITE)
        )
    )
    requests_mock.get(
        "{}/datasets/{}/files".format(METAX_URL, dataset['identifier']),
        json=files
    )

    # Mock Metax contracts API
    requests_mock.get(
        "{}/contracts/{}".format(METAX_URL,
                                 contract['contract_json']["identifier"]),
        json=contract
    )
