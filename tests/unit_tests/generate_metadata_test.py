"""Tests for ``siptools_research.generate_metadata`` module"""

import json
import pytest
import httpretty
from siptools_research.generate_metadata import generate_metadata

@pytest.mark.usefixtures('testmetax', 'testida')
def test_generate_metadata():
    """Tests metadata generation. Generates metadata for a dataset and checks
    that JSON message sent to Metax has correct keys/values.
    """
    generate_metadata('generate_metadata_test_dataset_1',
                      'tests/data/configuration_files/siptools_research.conf')

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
