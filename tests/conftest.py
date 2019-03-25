"""Configure py.test default values and functionality"""

import os
import sys
import re
import logging
import tempfile
import shutil
import urllib
import mongomock
import pymongo
import luigi.configuration
import httpretty
import pytest
import siptools_research.metadata_generator
import siptools_research.utils.mimetypes


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

    When url https://metax-test.csc.fi/es/reference_data/use_category/_search?pretty&size=100
    is requested using HTTP GET method, the response is content of file:
    'tests/httpretty_data/metax_elastic_search.json'

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
        logging.debug("Dynamic response for HTTP GET url: %s", url)
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
            body_file += urllib.quote(tail, safe='%')

        full_path = "%s/%s/%s" % (METAX_PATH, subdir, body_file)
        status = 400 if body_file.split('?')[0].endswith('400') else 200
        logging.debug("Looking for file: %s", full_path)
        if not os.path.isfile(full_path):
            return (403, headers, "File not found")

        with open(full_path) as open_file:
            body = open_file.read()

        return (status, headers, body)

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

    # register response for get_elasticsearchdata-function
    elasticsearchdata_url = 'https://metax-test.csc.fi/es/reference_data/'\
                            'use_category/_search?pretty&size=100'
    with open('tests/httpretty_data/metax_elastic_search.json') as open_file:
        body = open_file.read()
    httpretty.register_uri(
        httpretty.GET,
        elasticsearchdata_url,
        body=body,
        status=200,
        content_type='application/json'
    )

    # Didable http-server after executing the test function
    def fin():
        """Disable fake http-server"""
        httpretty.disable()

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
        with open(os.path.join(IDA_PATH, idafile)) as open_file:
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
def testpath(request, monkeypatch):
    """Create a temporary directory that will be removed after execution of
    function.

    :param request: Pytest `request` fixture
    :returns: path to temporary directory
    """

    temp_path = tempfile.mkdtemp()
    tmpdir = os.path.join(temp_path, "tmp")
    os.mkdir(tmpdir)

    monkeypatch.setattr(
        siptools_research.metadata_generator, "TEMPDIR", tmpdir
    )

    def fin():
        """remove temporary path"""
        shutil.rmtree(temp_path)

    request.addfinalizer(fin)

    return temp_path


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
