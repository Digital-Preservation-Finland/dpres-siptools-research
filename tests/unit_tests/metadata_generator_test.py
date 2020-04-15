"""Tests for :mod:`siptools_research.metadata_generator` module"""

import os
import copy

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
import tests.metax_data.datasets
import tests.metax_data.files


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


@pytest.mark.parametrize(
    ('path', 'file_format', 'encoding', 'namespace'),
    (
        ('tests/data/sample_files/text_plain_UTF-8',
         'text/plain',
         'UTF-8',
         None),
        ('tests/data/sample_files/image_png.png',
         'image/png',
         None,
         'http://www.loc.gov/mix/v20'),
        ('tests/data/sample_files/image_tiff_large.tif',
         'image/tiff',
         None,
         'http://www.loc.gov/mix/v20'),
        ('tests/data/sample_files/audio_x-wav.wav',
         'audio/x-wav',
         None,
         'http://www.loc.gov/audioMD/')
    )
)
@pytest.mark.usefixtures('testpath')
def test_generate_metadata(requests_mock,
                           path,
                           file_format,
                           encoding,
                           namespace):
    """Tests metadata generation. Generates metadata for a dataset that
    contains one file, and checks that correct file characteristics and XML
    metadata are sent to Metax. Lastly the preservation state should be
    updated.

    :param requests_mock: Mocker object
    :param path: path to file for which the metadata is created
    :param file_format: excepted file format
    :param encoding: excepted character set
    :param namespace: name space of xml metadata
    :returns: ``None``
    """
    # create mocked dataset in Metax and Ida
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    dataset_metadata = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)

    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_id",
                      json=dataset_metadata)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_id/files",
                      json=[file_metadata])
    requests_mock.patch("https://metaksi/rest/v1/datasets/dataset_id")
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier",
                      json=file_metadata)
    with open(path, 'rb') as file_:
        requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                          content=file_.read())
    file_metadata_patch = requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:identifier"
    )
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml",
                      json=[])
    xml_post = requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:identifier/xml?namespace={}"
        .format(namespace),
        status_code=201
    )

    # generate metadata for dataset
    generate_metadata('dataset_id', tests.conftest.UNIT_TEST_CONFIG_FILE)

    # verify the file characteristics that were sent to Metax
    file_characteristics \
        = file_metadata_patch.last_request.json()['file_characteristics']
    assert file_characteristics.get('file_format') == file_format
    assert file_characteristics.get('encoding') == encoding

    # verify xml metadata that was posted to Metax (if required for file type)
    if namespace:
        xml = lxml.etree.fromstring(xml_post.last_request.text)
        # The expected namespace should be defined in posted XML
        assert namespace in xml.nsmap.values()

    # Verify that preservation state is set as
    # DS_STATE_TECHNICAL_METADATA_GENERATED
    request_body = requests_mock.last_request.json()
    assert request_body['preservation_description'] \
        == "Technical metadata generated"
    assert request_body['preservation_state'] == 20


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_predefined(requests_mock):
    """Tests metadata generation for files that already have some
    file_characteristics defined. File characteristics should not be
    overwritten, but missing information should be added.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset_metadata = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_metadata['file_characteristics'] = {
        'encoding': 'user_defined',
        'dummy_key': 'dummy_value'
    }

    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_id",
                      json=dataset_metadata)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_id/files",
                      json=[file_metadata])
    requests_mock.patch("https://metaksi/rest/v1/datasets/dataset_id")
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier",
                      json=file_metadata)
    requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                      content=b'foo')
    requests_mock.patch("https://metaksi/rest/v1/files/pid:urn:identifier")

    generate_metadata('dataset_id', tests.conftest.UNIT_TEST_CONFIG_FILE)

    # verify the file characteristics that were sent to Metax
    patch_request = requests_mock.request_history[-3].json()
    assert patch_request['file_characteristics'] == {
        'file_format': 'text/plain',  # missing keys are added
        'encoding': 'user_defined',  # user defined value is not overwritten
        'dummy_key': 'dummy_value'  # additional keys are copied
    }


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
