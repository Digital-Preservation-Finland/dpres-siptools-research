"""Tests for :mod:`siptools_research.metadata_generator` module"""

import os
import json

import pytest
import pymongo
import httpretty
import lxml.etree

from siptools.utils import decode_path

from siptools_research.config import Configuration
import siptools_research.metadata_generator as metadata_generator
from siptools_research.metadata_generator import (
    generate_metadata, MetadataGenerationError
)
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


@pytest.fixture(autouse=True)
# pylint: disable=unused-argument
def _init_mongo_client(testmongoclient):
    """Initializes mocked mongo collection upload.files"""
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get("mongodb_host"))
    files_col = mongoclient.upload.files

    files = [
        "pid:urn:generate_metadata_1",
        "pid:urn:generate_metadata_2",
        "pid:urn:generate_metadata_3",
        "pid:urn:generate_metadata_4",
        "pid:urn:generate_metadata_5",
        "pid:urn:generate_metadata_file_characteristics"
    ]

    for _file in files:
        files_col.insert_one({
            "_id": _file + "_local",
            "file_path": os.path.abspath(
                "tests/httpretty_data/ida/%s_ida" % _file
            )
        })


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
def test_generate_metadata(file_storage):
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values.

    :returns: ``None``
    """
    generate_metadata('generate_metadata_test_dataset_1_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    json_message = json.loads(httpretty.last_request().body)

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


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_file_characteristics_not_present(file_storage):
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values when
    file_characteristics block was not present in file metadata

    :returns: ``None``
    """
    generate_metadata(
        'generate_metadata_test_dataset_file_characteristics_%s'
        % file_storage,
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    json_message = json.loads(httpretty.last_request().body)
    # The file should recognised as plain text file
    assert json_message['file_characteristics']['file_format'] == 'text/plain'

    # The format version should not be set  since there is no different
    # versions of plain text files
    assert 'format_version' not in json_message['file_characteristics']

    # Encoding should be set correctly since it was not defined by user
    assert json_message['file_characteristics']['encoding'] == 'UTF-8'


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
def test_generate_metadata_mix(file_storage):
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.

    :returns: ``None``
    """
    generate_metadata('generate_metadata_test_dataset_2_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    last_request = httpretty.last_request().body
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(last_request)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '10'

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] \
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_mix_larger_file(file_storage):
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file larger than 512 bytes and checks that
    message sent to Metax is valid XML. The method of last HTTP request should
    be POST, and the querystring should contain the namespace of XML.

    :returns: ``None``
    """
    generate_metadata('generate_metadata_test_dataset_5_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(httpretty.last_request().body)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '640'

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] \
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
def test_generate_metadata_addml(file_storage):
    """Tests addml metadata generation for a CSV file. Generates metadata for a
    dataset that contains a CSV file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.

    :returns: ``None``
    """
    generate_metadata('generate_metadata_test_dataset_3_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The decoded filename should be /testpath/csvfile.csv
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(httpretty.last_request().body)

    flatfile = xml.xpath(
        '//addml:flatFile',
        namespaces={"addml": "http://www.arkivverket.no/standarder/addml"}
    )
    name = decode_path(flatfile[0].get("name"))
    assert name == "path/to/file"

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] == \
        'http://www.arkivverket.no/standarder/addml'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
def test_generate_metadata_audiomd(file_storage):
    """Tests addml metadata generation for a WAV file. Generates metadata for a
    dataset that contains a WAV file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.

    :returns: ``None``
    """
    generate_metadata('generate_metadata_test_dataset_4_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # pylint: disable=no-member
    xml = lxml.etree.fromstring(httpretty.last_request().body)

    freq = xml.xpath(
        '//amd:samplingFrequency',
        namespaces={"amd": "http://www.loc.gov/audioMD/"}
    )[0].text

    assert freq == '48'

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] \
        == 'http://www.loc.gov/audioMD/'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_tempfile_removal(file_storage):
    """Tests that temporary files downloaded from Ida are removed.

    :returns: ``None``
    """
    # Check contents of /tmp before calling generate_metadata()
    tmp_dir_before_test = os.listdir(metadata_generator.TEMPDIR)

    generate_metadata('generate_metadata_test_dataset_1_%s' % file_storage,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # There should not be new files or directories in /tmp
    assert os.listdir(metadata_generator.TEMPDIR) == tmp_dir_before_test


@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
# pylint: disable=invalid-name
def test_generate_metadata_missing_csv_info():
    """Tests addml metadata generation for a dataset that does not contain all
    metadata required for addml generation.

    :returns: ``None``
    """
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
@pytest.mark.usefixtures('testmetax', 'testida', 'testpath',
                         'mock_metax_access')
def test_generate_metadata_provenance(dataset):
    """Tests that provenance data is generated and added to Metax if it is
    missing from dataset metadata.

    :returns: ``None``
    """
    generate_metadata(dataset,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    json_message = json.loads(httpretty.last_request().body)
    assert json_message['research_dataset']['provenance'] \
        == [DEFAULT_PROVENANCE]
