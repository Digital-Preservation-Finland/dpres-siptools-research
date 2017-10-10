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
    some subdirectory of ``METAX_PATH``.
    """

    httpretty.enable()

    # Register all files in subdirectories of ``METAX_PATH`` to httpretty.
    for subdir in os.listdir(METAX_PATH):
        for jsonfile in os.listdir(os.path.join(METAX_PATH, subdir)):
            with open(os.path.join(METAX_PATH, subdir, jsonfile)) as open_file:
                body = open_file.read()
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
    def mock_mongoclient(*args):
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


@pytest.fixture(scope="function")
def packagefile(request):
    """Fixture that creates a temporary zip/tar archive file and returns path
    to the file.

    Should create a zip/tar file with the following files and directories::

        test-package/testfile.0
        test-package/testfile.1
        ...


    Should return the followind data structure::

        {
             "filename": '/tmp/tests.packagefile.mG56N4/test-package.zip',
            "filenames: [
                "test-package/testfile.0", "test-package/testfile.1",
                ...
            ]
        }

    :request: Pytest request-fixture
    :returns: Filenames for the archive and contained filenames

    """

    temp_path = tempfile.mkdtemp(prefix="tests.packagefile.")

    def fin():
        """remove temporary path"""
        subprocess.call(['find', temp_path, '-ls'])
        shutil.rmtree(temp_path)

    request.addfinalizer(fin)

    def make_packagefile(sip_path='tests/data/transfer/valid_transfer',
                         packagetype='tar.gz', command='tar', param='czf'):
        """Returns created archive"""

        sip_path = os.path.abspath(sip_path)
        base_path = os.path.dirname(sip_path)
        sip_name = os.path.basename(sip_path)

        unique = str(uuid4())

        package_filename = os.path.join(
            temp_path,
            '%s-%s.%s' % (sip_name, unique, packagetype))

        proc = siptools_research.utils.shell.Shell(
            [command, param, os.path.join(temp_path, package_filename),
             sip_name], cwd=os.path.dirname(sip_path))
        proc.check_call()

        filenames = []

        for root, dirs, files in os.walk(sip_path):
            for filename in files + dirs:
                path = os.path.join(root, filename)
                filenames.append(path.replace(base_path + '/', ''))

        return {
            "unique": unique,
            "filename": package_filename,
            "filenames": filenames}

    return make_packagefile


@pytest.fixture(scope="function")
def tarfile(request):
    """Fixture that creates a temporary zip/tar archive file and returns path
    to the file.

    Should create a zip/tar file with the following files and directories::

        test-package/testfile.0
        test-package/testfile.1
        ...


    Should return the followind data structure::

        {
             "filename": '/tmp/tests.packagefile.mG56N4/test-package.zip',
            "filenames: [
                "test-package/testfile.0", "test-package/testfile.1",
                ...
            ]
        }

    :request: Pytest request-fixture
    :returns: Filenames for the archive and contained filenames

    """

    temp_path = tempfile.mkdtemp(prefix="tests.tarfile.")

    def fin():
        """remove temporary path"""
        subprocess.call(['find', temp_path, '-ls'])
        shutil.rmtree(temp_path)

    request.addfinalizer(fin)

    def make_tarfile(tar_path='tests/data/transfer/transfer.tar.gz',
                     packagetype='tar.gz'):
        """Returns created archive"""

        tar_path = os.path.abspath(tar_path)
        base_path = os.path.dirname(tar_path)
        sip_name = os.path.basename(tar_path)

        unique = str(uuid4())

        tar_filename = os.path.join(
            temp_path,
            '%s-%s.%s' % (sip_name, unique, packagetype))

        shutil.copy(tar_path, os.path.join(temp_path, tar_filename))

        filenames = []

        for root, dirs, files in os.walk(tar_path):
            for filename in files + dirs:
                path = os.path.join(root, filename)
                filenames.append(path.replace(base_path + '/', ''))

        return {
            "unique": unique,
            "filename": tar_filename,
            "filenames": filenames}

    return make_tarfile

@pytest.fixture(scope="session")
def scheduler_host():
    """TODO: Docstring for scheduler_host_port.
    :returns: TODO

    """

    """host_port = pytest.config.getoption("--scheduler")

    if pytest.config.getoption("--e2e"):
        if not host_port:
            return 'localhost:8082'

    if not host_port:
        return None

    if ':' in host_port:
        return host_port
    else:
        return '%s:8082' % host_port
    """
    return None

@pytest.fixture(scope='session')
def scheduler():
    """Return scheduler instance for tests"""

    if hasattr(scheduler, 'cached_scheduler') and scheduler.cached_scheduler:
        return scheduler.cached_scheduler

    def _select_scheduler():
        """Select scheduler to use"""
        host_port = scheduler_host()
        if scheduler_host():
            (host, port) = host_port.split(':')
            return RemoteScheduler(host=host, port=port)
        else:
            return Scheduler()

    scheduler.cached_scheduler = _select_scheduler()

    return scheduler.cached_scheduler

@pytest.fixture(scope='function')
def popen_fx(monkeypatch):
    """Monkeypatch the ``subprocess.Popen()`` class.

    Example usage::

    def test_something(popen_fx)

    popen_fx.stdout = 'foo'
    popen_fx.returncode = 42

    proc = subprocess(['command'])
    stdout, _ = proc.communicate()

    assert stdout == 'foo'
    assert proc.returncode == 42

    """

    class _Popen(object):
        """Mockup the subprocess.Popen class"""

        stdout = ""
        stderr = ""
        returncode = 0
        command = None

        def __init__(self, command, **kwargs):
            """Setup the input command and output streams"""

            LOGGER.debug('_Popen():command:%s', command)

            self._stdout = None
            self._stderr = None
            _Popen.command = command

            for arg in kwargs:
                LOGGER.debug('_Popen():%s:%s', kwargs, kwargs[arg])
                setattr(self, '_%s' % arg, kwargs[arg])

        def communicate(self):
            """Mockup Popen communicate"""
            LOGGER.debug('_Popen():communicate():%s')

            if self._stdout is not None and self._stdout != -1:
                self._stdout.write(self.stdout)

            if self._stderr is not None and self._stderr != -1:
                self._stderr.write(self.stderr)

            return (self.stdout, self.stderr)

        @classmethod
        def patch(cls, module):
            """Patch Popen in some other module than
            subprocess"""
            monkeypatch.setattr(module, 'Popen', cls)

        def to_string(self):
            """annoying pytest too few methods"""
            return '\n'.join([self.command, self.stdout, self.stderr])

        def __str__(self):
            """to_string"""
            return self.to_string()

    monkeypatch.setattr(subprocess, 'Popen', _Popen)

    return _Popen
