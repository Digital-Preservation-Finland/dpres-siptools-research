"""Tests for :mod:`siptools_research.metadata_generator` module."""

import os
import copy

import pytest
import lxml.etree
from requests.exceptions import HTTPError
from siptools.utils import decode_path
from metax_access import DatasetNotAvailableError

from siptools_research.metadata_generator import generate_metadata
from siptools_research.exceptions import MissingFileError
from siptools_research.exceptions import InvalidFileMetadataError
from siptools_research.exceptions import InvalidFileError
import tests.conftest
from tests.conftest import mock_metax_dataset
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
    """Test metadata generation.

    Generates metadata for a dataset that contains one file, and checks that
    correct file characteristics and XML metadata are sent to Metax.

    :param requests_mock: Mocker object
    :param path: path to file for which the metadata is created
    :param file_format: expected file format
    :param encoding: expected character set
    :param namespace: name space of xml metadata
    :returns: ``None``
    """
    # create mocked dataset in Metax and Ida
    mock_metax_dataset(requests_mock, files=[tests.metax_data.files.BASE_FILE])
    file_metadata_patch = requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:identifier",
        json={}
    )
    xml_post = requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:identifier/xml?namespace={}"
        .format(namespace),
        status_code=201
    )
    with open(path, 'rb') as file_:
        requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                          content=file_.read())

    # generate metadata for dataset
    generate_metadata('dataset_identifier',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

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


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_unrecognized(requests_mock):
    """Test metadata generation for unrecognized file.

    File scraper does not recognize for example empty files. Metadata
    generation should raise error if file type is (:unav).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # create mocked dataset in Metax and Ida
    mock_metax_dataset(requests_mock, files=[tests.metax_data.files.BASE_FILE])
    requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                      text="")

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) == 'File format was not recognized.'
    assert exception_info.value.files == ['pid:urn:identifier']


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_predefined(requests_mock):
    """Test generate_metadata.

    Tests metadata generation for files that already have some
    file_characteristics defined. File characteristics should not be
    overwritten, but missing information should be added.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_metadata['file_characteristics'] = {
        'encoding': 'user_defined',
        'dummy_key': 'dummy_value'
    }
    mock_metax_dataset(requests_mock, files=[file_metadata])
    requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                      content=b'foo')
    patch_request = requests_mock.patch(
        "https://metaksi/rest/v1/files/pid:urn:identifier",
        json={}
    )

    generate_metadata('dataset_identifier',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # verify the file characteristics that were sent to Metax
    assert patch_request.last_request.json()['file_characteristics'] == {
        'file_format': 'text/plain',  # missing keys are added
        'encoding': 'user_defined',  # user defined value is not overwritten
        'dummy_key': 'dummy_value'  # additional keys are copied
    }


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_addml(requests_mock):
    """Test generate metadata.

    Tests addml metadata generation for a CSV file. Generates metadata for a
    dataset that contains a CSV file and checks that message sent to Metax
    is valid XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock, files=[tests.metax_data.files.CSV_FILE])
    addml_post_request = requests_mock.post(
        "https://metaksi/rest/v1/files/pid:urn:identifier/xml?"
        "namespace=http://www.arkivverket.no/standarder/addml",
        status_code=201
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:identifier/download", content=b'foo'
    )

    generate_metadata('dataset_identifier',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(addml_post_request.last_request.body)
    flatfile = xml.xpath(
        '//addml:flatFile',
        namespaces={"addml": "http://www.arkivverket.no/standarder/addml"}
    )
    name = decode_path(flatfile[0].get("name"))
    assert name == "path/to/file"


# pylint: disable=invalid-name
def test_generate_metadata_tempfile_removal(testpath, requests_mock):
    """Tests that temporary files downloaded from Ida are removed.

    :param testpath: path to packaging root directory
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock, files=[tests.metax_data.files.BASE_FILE])
    requests_mock.get("https://ida.test/files/pid:urn:identifier/download",
                      text='foo')

    tmp_path = "{}/tmp".format(testpath)
    file_cache_path = "{}/file_cache".format(testpath)

    # tmp and file_cache should be empty before calling generate_metadata()
    assert os.listdir(tmp_path) == []
    assert os.listdir(file_cache_path) == []

    generate_metadata('dataset_identifier',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # There should not be new files or directories in tmp after metadata
    # generation, but the downloaded file should be left in file cache
    assert os.listdir(tmp_path) == []
    assert os.listdir(file_cache_path) == ['pid:urn:identifier']


@pytest.mark.usefixtures('testpath')
# pylint: disable=invalid-name
def test_generate_metadata_missing_csv_info(requests_mock):
    """Test generate_metadata.

    Tests addml metadata generation for a dataset that does not contain all
    metadata required for addml generation.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    invalid_file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    invalid_file_metadata['file_characteristics'] = {'file_format': 'text/csv'}
    mock_metax_dataset(requests_mock, files=[invalid_file_metadata])
    requests_mock.get("https://ida.test/files/pid:urn:identifier/download")

    with pytest.raises(InvalidFileMetadataError) as exception_info:
        generate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) == (
        'Required attribute "csv_delimiter" is missing from file '
        'characteristics of a CSV file.'
    )
    assert exception_info.value.files == ['pid:urn:identifier']


# pylint: disable=invalid-name
@pytest.mark.parametrize('provenance', (None, [], [{}], [{'foo': 'bar'}]))
@pytest.mark.usefixtures('testpath')
def test_generate_metadata_provenance(provenance, requests_mock):
    """Test generate metadata.

    Tests that provenance data is generated and added to Metax if it is
    missing from dataset metadata. If provenance exists already, it should not
    be overwritten.

    :param provenance: Provenance metadata dictionary
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    if provenance is None:
        del dataset['research_dataset']['provenance']
    else:
        dataset['research_dataset']['provenance'] = provenance
    mock_metax_dataset(requests_mock, dataset=dataset)
    patch_dataset_metadata = requests_mock.patch(
        'https://metaksi/rest/v1/datasets/dataset_identifier',
        json={}
    )

    generate_metadata('dataset_identifier',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    if provenance:
        # Some provenance existed already, so the metadata should not be
        # patched.
        assert not patch_dataset_metadata.request_history
    else:
        # Provenance list did not exist in metadata (or it was empty).
        # Default provenance should be added to Metax.
        assert len(patch_dataset_metadata.request_history) == 1
        provenance_patch = patch_dataset_metadata.last_request.json()
        assert provenance_patch['research_dataset']['provenance'] \
            == [DEFAULT_PROVENANCE]


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_dataset_not_found(requests_mock):
    """Test metadatageneration for dataset that does not exist.

    DatasetNotAvailableError should be raised.

    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get('https://metaksi/rest/v1/datasets/foobar',
                      status_code=404)

    expected_error = 'Dataset not found'
    with pytest.raises(DatasetNotAvailableError, match=expected_error):
        generate_metadata('foobar',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_ida_download_error(requests_mock):
    """Test metadatageneration when file is available.

    If file is not available, the dataset is not valid.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock, files=[tests.metax_data.files.BASE_FILE])
    requests_mock.get('https://ida.test/files/pid:urn:identifier/download',
                      status_code=404)

    with pytest.raises(MissingFileError) as exception_info:
        generate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) == "File is not available"
    assert exception_info.value.files == ['pid:urn:identifier']


@pytest.mark.usefixtures('testpath')
def test_generate_metadata_httperror(requests_mock):
    """Test metadata generation when Metax fails.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock)
    requests_mock.get(
        'https://metaksi/rest/v1/datasets/dataset_identifier/files',
        status_code=500,
        reason='Fake error'
    )

    # TODO: The message of HTTPErrors will be different in newer versions of
    # requests library (this test works with version 2.6 which is available in
    # centos7 repositories).
    expected_error = "500 Server Error: Fake error"
    with pytest.raises(HTTPError, match=expected_error):
        generate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
