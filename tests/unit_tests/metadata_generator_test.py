"""Tests for :mod:`siptools_research.metadata_generator` module"""

import os

import pytest
import lxml.etree
from requests.exceptions import HTTPError
from siptools.utils import decode_path
from metax_access import Metax, DatasetNotFoundError

import siptools_research
from siptools_research.metadata_generator import (
    generate_metadata, MetadataGenerationError
)
from siptools_research.utils import download
import tests.conftest


DEFAULT_PROVENANCE = {
    "preservation_event": {
        "identifier":
        "http://uri.suomi.fi/codelist/fairdata/preservation_event/code/cre",
        "pref_label": {
            "en": "creation"
        }
    },
    "description": {
        "en": "Value unavailable, possibly unknown"
    },
    "event_outcome": {
        "identifier":
        "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/unknown",
        "pref_label": {
            "en": "(:unav)"
        }
    },
    "outcome_description": {
        "en": "Value unavailable, possibly unknown"
    }
}


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata(requests_mock):
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_1"
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:generate_metadata_1/download",
        content=b'foo'
    )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_1"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_1/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_1/"
        "xml?namespace=http://www.loc.gov/mix/v20",
        status_code=201
    )
    generate_metadata('generate_metadata_test_dataset_1',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    json_message = requests_mock.request_history[-2].json()

    # The file should recognised as plain text file
    assert json_message['file_characteristics']['file_format'] == 'text/plain'

    # The format version should not be set since there is no different versions
    # of plain text files
    assert 'format_version' not in json_message['file_characteristics']

    # Encoding should not be changed since it was already defined by user
    assert json_message['file_characteristics']['encoding'] == \
        'user_defined_charset'

    # All other fields should be same as in the original file_charasteristics
    # object in Metax
    assert json_message['file_characteristics']['dummy_key'] == 'dummy_value'

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_file_characteristics_not_present(requests_mock):
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values when
    file_characteristics block was not present in file metadata

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/"
        "generate_metadata_test_dataset_file_characteristics"
    )
    requests_mock.get(
        "https://ida.test/files/"
        "pid:urn:generate_metadata_file_characteristics/download",
        content=b'foo'
    )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/"
        "pid:urn:generate_metadata_file_characteristics"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/"
        "pid:urn:generate_metadata_file_characteristics/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/"
        "pid:urn:generate_metadata_file_characteristics/"
        "xml?namespace=http://www.loc.gov/mix/v20",
        status_code=201
    )
    generate_metadata(
        'generate_metadata_test_dataset_file_characteristics',
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    json_message = requests_mock.request_history[-2].json()
    # The file should be recognised as plain text file
    assert json_message['file_characteristics']['file_format'] == 'text/plain'

    # The format version should not be set  since there is no different
    # versions of plain text files
    assert 'format_version' not in json_message['file_characteristics']

    # Encoding should be set correctly since it was not defined by user
    assert json_message['file_characteristics']['encoding'] == 'UTF-8'

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_mix(requests_mock):
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_2"
    )
    with open('tests/data/sample_files/image_png.png', 'rb') as file_:
        requests_mock.get(
            "https://ida.test/files/pid:urn:generate_metadata_2/download",
            content=file_.read()
        )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_2"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_2/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_2/"
        "xml?namespace=http://www.loc.gov/mix/v20",
        status_code=201
    )

    generate_metadata('generate_metadata_test_dataset_2',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    last_request = requests_mock.request_history[-2].body
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(last_request)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '10'

    # Check HTTP request query string
    assert requests_mock.request_history[-2].qs['namespace'][0]\
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert requests_mock.request_history[-2].method == "POST"

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_mix_larger_file(requests_mock):
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file larger than 512 bytes and checks that
    message sent to Metax is valid XML. The method of last HTTP request should
    be POST, and the querystring should contain the namespace of XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_5"
    )
    with open('tests/data/sample_files/image_tiff_large.tif', 'rb') as file_:
        requests_mock.get(
            "https://ida.test/files/pid:urn:generate_metadata_5/download",
            content=file_.read()
        )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_5"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_5/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_5/xml"
        "?namespace=http://www.loc.gov/mix/v20",
        status_code=201
    )

    generate_metadata('generate_metadata_test_dataset_5',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(requests_mock.request_history[-2].body)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '640'

    # Check HTTP request query string
    assert requests_mock.request_history[-2].qs['namespace'][0]\
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert requests_mock.request_history[-2].method == "POST"

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_addml(requests_mock):
    """Tests addml metadata generation for a CSV file. Generates metadata for a
    dataset that contains a CSV file and checks that message sent to Metax
    is valid XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_3"
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:generate_metadata_3/download",
        content=b'foo'
    )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_3"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_3/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_3/"
        "xml?namespace=http://www.arkivverket.no/standarder/addml",
        status_code=201
    )

    generate_metadata('generate_metadata_test_dataset_3',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The decoded filename should be /testpath/csvfile.csv
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(requests_mock.request_history[-2].body)

    flatfile = xml.xpath(
        '//addml:flatFile',
        namespaces={"addml": "http://www.arkivverket.no/standarder/addml"}
    )
    name = decode_path(flatfile[0].get("name"))
    assert name == "path/to/file"

    # Check HTTP request query string
    query = requests_mock.request_history[-2].qs['namespace'][0]
    assert query == 'http://www.arkivverket.no/standarder/addml'

    # Check HTTP request method
    assert requests_mock.request_history[-2].method == "POST"

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_audiomd(requests_mock):
    """Tests audiomd metadata generation for a WAV file. Generates metadata for
    a dataset that contains a WAV file and checks that message sent to Metax is
    valid XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """

    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_4"
    )
    with open('tests/data/sample_files/audio_x-wav.wav', 'rb') as file_:
        requests_mock.get(
            "https://ida.test/files/pid:urn:generate_metadata_4/download",
            content=file_.read()
        )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_4"
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_4/xml",
        json=[]
    )
    requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_4/"
        "xml?namespace=http://www.loc.gov/audioMD/",
        status_code=201
    )

    generate_metadata('generate_metadata_test_dataset_4',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(requests_mock.request_history[-2].body)

    freq = xml.xpath(
        '//amd:samplingFrequency',
        namespaces={"amd": "http://www.loc.gov/audioMD/"}
    )[0].text
    assert freq == '48'

    # Check HTTP request query string
    # requests_mock is case insensitive, therefore the querystrings in request
    # history are always lower case. That functionality might change in future,
    # and this test must then be modified.
    assert requests_mock.request_history[-2].qs['namespace'][0] \
        == 'http://www.loc.gov/audioMD/'.lower()

    # Check HTTP request method
    assert requests_mock.request_history[-2].method == "POST"

    # verify preservation_state is set as last operation
    _assert_metadata_generated(
        requests_mock.request_history[-1].json()
    )


@pytest.mark.usefixtures('mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_tempfile_removal(testpath, requests_mock):
    """Tests that temporary files downloaded from Ida are removed.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get(
        "https://ida.test/files/pid:urn:generate_metadata_1/download"
    )
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_1"
    )
    requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:generate_metadata_1"
    )

    tmp_path = "{}/tmp".format(testpath)

    # Check contents of /tmp before calling generate_metadata()
    tmp_dir_before_test = os.listdir(tmp_path)

    generate_metadata('generate_metadata_test_dataset_1',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # There should not be new files or directories in /tmp
    assert os.listdir(tmp_path) == tmp_dir_before_test


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_missing_csv_info(requests_mock):
    """Tests addml metadata generation for a dataset that does not contain all
    metadata required for addml generation.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get("https://ida.test/files/missing_csv_info/download")
    requests_mock.patch("https://metaksi/rest/v1/datasets/missing_csv_info")
    requests_mock.patch("https://metaksi/rest/v1/files/missing_csv_info")

    with pytest.raises(MetadataGenerationError) as exception_info:
        generate_metadata('missing_csv_info',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert str(exception_info.value) == (
        'Required attribute "csv_delimiter" is missing from file '
        'characteristics of a CSV file. '
        '[ dataset=missing_csv_info, file=missing_csv_info ]'
    )


# pylint: disable=invalid-name
@pytest.mark.parametrize('dataset', ['missing_provenance', 'empty_provenance'])
@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_provenance(dataset, requests_mock):
    """Tests that provenance data is generated and added to Metax if it is
    missing from dataset metadata.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/{}".format(dataset),
        json={}
    )

    generate_metadata(dataset,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)
    json_message = requests_mock.request_history[-2].json()
    assert json_message['research_dataset']['provenance'] \
        == [DEFAULT_PROVENANCE]

    # verify preservation_state is set as last operation
    _assert_metadata_generated(requests_mock.request_history[-1].json())


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_dataset_not_found(monkeypatch, requests_mock):
    """Verifies that preservation state is not set when DatasetNotFoundError is
    raised by Metax get_dataset.

    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    :returns: ``None``
    """

    def _get_dataset_exception(*_arg1):
        raise DatasetNotFoundError

    monkeypatch.setattr(Metax, "get_dataset", _get_dataset_exception)
    with pytest.raises(DatasetNotFoundError):
        generate_metadata('foobar',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # No HTTP request done
    assert not requests_mock.request_history


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_ida_download_error(monkeypatch, requests_mock):
    """Verifies that preservation state is set correctly when file download
    from IDA fails and MetadataGenerationError is raised.

    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_1"
    )

    def _get_dataset_exception(*_args):
        raise download.FileNotFoundError(
            "File '/path/to/file' not found in Ida"
        )

    monkeypatch.setattr(
        siptools_research.metadata_generator,
        "download_file",
        _get_dataset_exception
    )
    with pytest.raises(MetadataGenerationError):
        generate_metadata('generate_metadata_test_dataset_1',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
    # Assert preservation state is set correctly
    assert requests_mock.last_request.method == "PATCH"
    body = requests_mock.last_request.json()
    assert body['preservation_state'] == 30
    assert body['preservation_description'] == (
        "File '/path/to/file' not found in Ida "
        "[ dataset=generate_metadata_test_dataset_1 ]"
    )


@pytest.mark.usefixtures('testpath', 'mock_metax_access')
def test_generate_metadata_httperror(monkeypatch, requests_mock):
    """Verifies that preservation state is set when HTTPError occurs.

    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        'https://metaksi/rest/v1/datasets/generate_metadata_test_dataset_1'
    )

    def _get_dataset_exception(*_arg1):
        raise HTTPError('httperror')

    monkeypatch.setattr(Metax, "get_dataset_files", _get_dataset_exception)
    with pytest.raises(HTTPError):
        generate_metadata('generate_metadata_test_dataset_1',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
    # Assert preservation state is set correctly
    assert requests_mock.last_request.method == "PATCH"
    body = requests_mock.last_request.json()
    assert body['preservation_state'] == 30
    assert body['preservation_description'] == "httperror"


def _assert_metadata_generated(request_body):
    """Verifies tht preservation state is set as
    DS_STATE_TECHNICAL_METADATA_GENERATED
    :param request_body: http request body
    """
    assert request_body['preservation_description'] == \
        "Technical metadata generated"
    assert request_body['preservation_state'] == 20
