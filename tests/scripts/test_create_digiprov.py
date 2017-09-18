"""Tests for create_digiprov script"""
import os
import httpretty
from siptools_research.scripts import create_digiprov

DATASET_PATH = "tests/data/metax_datasets/"
SAMPLE_CREATION_EVENT_PATH = "tests/data/sample_creation_event.xml"

def test_get_dataset(testpath):
    """Test get_dataset function"""

    # Use fake http-server and local sample JSON-file instead real Metax-API.
    # @httpretty.activate decorator is not used because it does not work with
    # fixture
    httpretty.enable()
    data_file_name = "provenance_data.json"
    with open(os.path.join(DATASET_PATH, data_file_name)) as data_file:
        data = data_file.read()

    httpretty.register_uri(httpretty.GET,
                           "https://metax-test.csc.fi/rest/v1/datasets/1",
                           body=data,
                           status=200,
                           content_type='application/json'
                          )

    # Create provenance info xml-file to tempdir
    workspace = testpath
    create_digiprov.main(['1', '--workspace', workspace])

    # Disable fake http-server
    httpretty.disable()

    # Check that one arbitrary line in the created xml-file is what it should
    # be. (The file is not always exactly the same.)
    with open(os.path.join(workspace, 'creation-event.xml')) as xmlfile:
        for i, line in enumerate(xmlfile):
            if i == 13:
                assert line.strip() == "<premis:eventDetail>Description of "\
                                       "provenance</premis:eventDetail>"
