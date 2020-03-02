"""Configure py.test default values and functionality"""

import os
import sys
import re
import logging
import shutil
try:
    from urllib import quote
except ImportError:  # Python 3
    from urllib.parse import quote
import json

import urllib3
import mongomock
import pymongo
import luigi.configuration
import httpretty
import pytest

from metax_access import Metax

import siptools_research.metadata_generator
import siptools_research.utils.mimetypes
import tests.metax_data.datasets as datasets
import tests.metax_data.files as files

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_PATH = "tests/httpretty_data/metax"
METAX_URL = "https://metaksi/rest/v1"
METAX_RPC_URL = "https://metaksi/rpc"
IDA_PATH = "tests/httpretty_data/ida/"
IDA_URL = 'https://86.50.169.61:4433'
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
def testmetax(request):
    """Use fake http-server and local sample JSON/XML files instead of real
    Metax-API. Files are searched from subdirectories of ``METAX_PATH`` and
    ``METAX_XML_PATH``.

    When https://metax-test.csc.fi/rest/v1/<subdir>/<filename> is requested
    using HTTP GET method, a HTTP response with contents of file:
    ``METAX_PATH/<subdir>/<filename>`` as message body is retrieved. The status
    of message is always *HTTP/1.1 200 OK*. To add new test responses just add
    new JSON or XML file to some subdirectory of ``METAX_PATH``. Using HTTP
    PATCH method has exactly same effect as HTTP GET methdod. Using HTTP POST
    method returns empty message with status *HTTP/1.1 201 OK*.

    If file from subsubdirectory is requested, the filename must be url encoded
    (the files are searched only from subdirectories, not from
    subsubdirectories). For example, when
    https://metax-test.csc.fi/rest/v1/<subdir>/<filename>/xml is requested
    using HTTP GET method, the file from path
    ``METAX_PATH/<subdir>/<filename>%2Fxml`` is retrieved. Another example:
    When
    https://metax-test.csc.fi/rest/v1/<subdir>/<filename>/xml?namespace=http://test.com/ns/
    is requested using HTTP GET method, the file from path
    ``METAX_PATH/<subdir>/<filename>%2Fxml%3Fnamespace%3Dhttp%3A%2F%2Ftest.com%2Fns%2F``
    is retrieved.

    :param request: pytest `request` fixture
    :returns: ``None``
    """

    # pylint: disable=unused-argument
    def dynamic_response(request, url, headers):
        """Return HTTP response according to url and query string.

        :param request: ghost parameter required by httpretty
        :param url: HTTP request url
        :param header: HTTP request headers
        :returns: HTTP status code, response headers, response body
        :rtype: tuple
        """
        logging.debug("Dynamic response for HTTP %s url: %s", request.method, url)
        # url without basepath:
        path = url.split(METAX_URL)[1]
        # subdirectory to get file from:
        subdir = path.split('/')[1]
        # file to be used as response body:
        body_file = path.split('/')[2]
        # if url contains query strings or more directories after the filename,
        # everything is added to the filename url encoded
        tail = path.split('/%s/%s' % (subdir, body_file))[1]
        if tail:
            body_file += quote(tail, safe='%')

        full_path = "%s/%s/%s" % (METAX_PATH, subdir, body_file)
        logging.debug("Looking for file: %s", full_path)
        if not os.path.isfile(full_path) and not _identifier_exists(body_file):
            return (403, headers, "File not found")

        # Retrieve PATCHed files or datasets from metax_data.files
        if request.method == "PATCH" and subdir == "files":
            body = json.dumps(files.get_file("", body_file))
        elif request.method == "PATCH" and subdir == "datasets":
            body = json.dumps(datasets.get_dataset("", body_file))
        else:
            with open(full_path, 'rb') as open_file:
                body = open_file.read()

        return (200, headers, body)

    # Enable http-server in beginning of test function
    httpretty.enable()

    # Register response for GET method for any url starting with METAX_URL
    httpretty.register_uri(
        httpretty.GET,
        re.compile(METAX_URL + '/(.*)'),
        body=dynamic_response,
    )

    # Register response for PATCH method for any url starting with METAX_URL
    httpretty.register_uri(
        httpretty.PATCH,
        re.compile(METAX_URL + '/(.*)'),
        body=dynamic_response,
    )

    # Register response for POST method for any url starting with METAX_URL
    httpretty.register_uri(
        httpretty.POST,
        re.compile(METAX_URL + '/(.*)'),
        status=201
    )

    # Register response for POST method for any url starting with METAX_RPC_URL
    httpretty.register_uri(
        httpretty.POST,
        re.compile(METAX_RPC_URL + '/(.*)'),
        status=200
    )

    # Didable http-server after executing the test function
    def fin():
        """Disable fake http-server"""
        httpretty.disable()
        httpretty.reset()
    request.addfinalizer(fin)


@pytest.fixture(scope="function")
def testida(request):
    """Use fake http-server and local sample files instead of real Ida.

    Files are searched from subdirectories of ``IDA_PATH``. When
    https://86.50.169.61:4433/<file_id>/download is requested using HTTP GET
    method, a HTTP response with contents of file: ``IDA_PATH/<file_id>`` as
    message body is retrieved. The status of message is always *HTTP/1.1 200
    OK*. To add new test responses just add new  file to ``IDA_PATH``.

    :param request: pytest `request` fixture
    :returns: ``None``
    """

    httpretty.enable()

    # Register all files in subdirectories of ``IDA_PATH`` to httpretty.
    for idafile in os.listdir(IDA_PATH):
        with open(os.path.join(IDA_PATH, idafile), 'rb') as open_file:
            body = open_file.read()
            httpretty.register_uri(
                httpretty.GET,
                '%s/files/%s/download' % (IDA_URL, idafile),
                body=body,
                status=200,
            )

    # Register a "404 Not found" response for url:
    # https://86.50.169.61:4433/pid:urn:does_not_exist/download
    httpretty.register_uri(
        httpretty.GET,
        '%s/files/%s/download' % (IDA_URL, 'pid:urn:does_not_exist'),
        status=404,
    )

    def fin():
        """Disable fake http-server"""
        httpretty.disable()

    request.addfinalizer(fin)


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
