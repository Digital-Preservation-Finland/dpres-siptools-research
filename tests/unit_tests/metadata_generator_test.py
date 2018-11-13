"""Tests for ``siptools_research.generate_metadata`` module"""

import os
import tempfile
import json
import pytest
import httpretty
import lxml.etree

from siptools.utils import decode_path
from siptools_research.metadata_generator import generate_metadata
import tests.conftest


@pytest.mark.usefixtures('testmetax', 'testida')
def test_generate_metadata():
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values.
    """
    generate_metadata('generate_metadata_test_dataset_1',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    json_message = json.loads(httpretty.last_request().body)

    # The file should recognised as plain text file
    assert json_message['file_characteristics']['file_format'] == 'text/plain'

    # The format version should be set empty string since there is no
    # different versions of plain text files
    assert json_message['file_characteristics']['format_version'] == ''

    # Encoding should not be changed since it was already defined by user
    assert json_message['file_characteristics']['encoding'] == \
        'user_defined_charset'

    # All other fields should be same as in the original file_charasteristics
    # object in Metax
    assert json_message['file_characteristics']['dummy_key'] == \
        'dummy_value'


@pytest.mark.usefixtures('testmetax', 'testida')
# pylint: disable=invalid-name
def test_generate_metadata_file_characteristics_block_not_present():
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values when
    file_characteristics block was not present in file metadata
    """
    generate_metadata('generate_metadata_test_dataset_' +
                      'file_characteristics_block_not_present',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    json_message = json.loads(httpretty.last_request().body)
    # The file should recognised as plain text file
    assert json_message['file_characteristics']['file_format'] == 'text/plain'

    # The format version should be set empty string since there is no
    # different versions of plain text files
    assert json_message['file_characteristics']['format_version'] == ''

    # Encoding should not be changed since it was already defined by user
    assert json_message['file_characteristics']['encoding'] == \
        'ISO-8859-15'


@pytest.mark.usefixtures('testmetax', 'testida')
def test_generate_metadata_mix():
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.
    """
    generate_metadata('generate_metadata_test_dataset_2',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    xml = lxml.etree.fromstring(httpretty.last_request().body)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '10'

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] \
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.usefixtures('testmetax', 'testida')
# pylint: disable=invalid-name
def test_generate_metadata_mix_larger_file():
    """Tests mix metadata generation for a image file. Generates metadata for a
    dataset that contains an image file larger than 512 bytes and checks that
    message sent to Metax is valid XML. The method of last HTTP request should
    be POST, and the querystring should contain the namespace of XML.
    """
    generate_metadata('generate_metadata_test_dataset_5',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The file is 10x10px image, so the metadata should contain image width.
    xml = lxml.etree.fromstring(httpretty.last_request().body)
    assert xml.xpath('//ns0:imageWidth',
                     namespaces={"ns0": "http://www.loc.gov/mix/v20"})[0].text\
        == '640'

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] \
        == 'http://www.loc.gov/mix/v20'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.usefixtures('testmetax', 'testida')
def test_generate_metadata_addml():
    """Tests addml metadata generation for a CSV file. Generates metadata for a
    dataset that contains a CSV file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.
    """
    generate_metadata('generate_metadata_test_dataset_3',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
    # The decoded filename should be /testpath/csvfile.csv
    xml = lxml.etree.fromstring(httpretty.last_request().body)

    flatfile = xml.xpath(
        '//addml:flatFile',
        namespaces={"addml": "http://www.arkivverket.no/standarder/addml"}
    )
    name = decode_path(flatfile[0].get("name"))
    assert name == "/testpath/csvfile.csv"

    # Check HTTP request query string
    assert httpretty.last_request().querystring['namespace'][0] == \
        'http://www.arkivverket.no/standarder/addml'

    # Check HTTP request method
    assert httpretty.last_request().method == "POST"


@pytest.mark.usefixtures('testmetax', 'testida')
def test_generate_metadata_audiomd():
    """Tests addml metadata generation for a WAV file. Generates metadata for a
    dataset that contains a WAV file and checks that message sent to Metax
    is valid XML. The method of last HTTP request should be POST, and the
    querystring should contain the namespace of XML.
    """
    generate_metadata('generate_metadata_test_dataset_4',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Read one element from XML to ensure it is valid and contains correct data
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


@pytest.mark.usefixtures('testmetax', 'testida')
# pylint: disable=invalid-name
def test_generate_metadata_tempfile_removal():
    """Tests that temporary files downloaded from Ida are removed.
    """
    # Check contents of /tmp before calling generate_metadata()
    tmp_dir_before_test = os.listdir(tempfile.gettempdir())

    generate_metadata('generate_metadata_test_dataset_1',
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # There should not be new files or directories in /tmp
    assert os.listdir(tempfile.gettempdir()) == tmp_dir_before_test
