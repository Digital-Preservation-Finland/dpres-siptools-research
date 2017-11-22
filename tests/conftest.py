"""Configure py.test default values and functionality"""

import os
import sys
import logging
import tempfile
import subprocess
import shutil
import posixpath
from uuid import uuid4
import mongomock
import pymongo
import httpretty
import pytest

from luigi.scheduler import Scheduler
from luigi.rpc import RemoteScheduler

import siptools_research.utils.shell


LOGGER = logging.getLogger('tests.conftest')
METAX_PATH = "tests/data/metax/"
METAX_URL = "https://metax-test.csc.fi/rest/v1/"
IDA_PATH = "tests/data/ida/"
IDA_URL = 'https://86.50.169.61:4433'


# Prefer modules from source directory rather than from site-python
PROJECT_ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
sys.path.insert(0, PROJECT_ROOT_PATH)


@pytest.fixture(scope="function")
def testmetax(request):
    """Use fake http-server and local sample JSON files instead of real
    Metax-API.

    Files are searched from subdirectories of ``METAX_PATH``. When
    https://metax-test.csc.fi/rest/v1/``subdir``/``filename`` is requested
    using HTTP GET method, a HTTP response with contents of file:
    ``METAX_PATH/subdir/filename`` as message body is retrieved. The status of
    message is always *HTTP/1.1 200 OK* and the Content-Type is
    *application/json*. To add new test responses just add new JSON file to
    some subdirectory of ``METAX_PATH``. Using HTTP PATCH method has exactly
    same effect as HTTP GET methdod.
    """

    httpretty.enable()

    # Register all files in subdirectories of ``METAX_PATH`` to httpretty.
    for subdir in os.listdir(METAX_PATH):
        for jsonfile in os.listdir(os.path.join(METAX_PATH, subdir)):
            with open(os.path.join(METAX_PATH, subdir, jsonfile)) as open_file:
                body = open_file.read()
                # Register response for GET method
                httpretty.register_uri(
                    httpretty.GET,
                    # Join url using posixpath because urlparse.urljoin does
                    # not work with multiple arguments (and os.path.join would
                    # not work on windows, for example)
                    posixpath.join(METAX_URL, subdir, jsonfile),
                    body=body,
                    status=200,
                    content_type='application/json'
                )
                # Register response for PATCH method
                httpretty.register_uri(
                    httpretty.PATCH,
                    posixpath.join(METAX_URL, subdir, jsonfile),
                    body=body,
                    status=200,
                    content_type='application/json'
                )

    # register response for get_elasticsearchdata-function
    elasticsearchdata_url = 'https://metax-test.csc.fi/es/reference_data/'\
                            'use_category/_search?pretty&size=100'
    with open('tests/data/metax_elastic_search.json') as open_file:
        body = open_file.read()
    httpretty.register_uri(
        httpretty.GET,
        elasticsearchdata_url,
        body=body,
        status=200,
        content_type='application/json'
    )

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
    """
    mongoclient = mongomock.MongoClient()
    # pylint: disable=unused-argument
    def mock_mongoclient(*args, **kwargs):
        """Returns already initialized mongomock.MongoClient"""
        return mongoclient
    monkeypatch.setattr(pymongo, 'MongoClient', mock_mongoclient)


@pytest.fixture(scope="function")
def testpath(request):
    """Create and cleanup a temporary directory

    :request: Pytest request fixture
    """

    temp_path = tempfile.mkdtemp()

    def fin():
        """remove temporary path"""
        shutil.rmtree(temp_path)

    request.addfinalizer(fin)

    return temp_path
