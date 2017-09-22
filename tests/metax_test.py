"""Tests for Metax-class"""
import os
import httpretty
from siptools_research.metax import Metax

DATASET_PATH = "tests/data/metax_datasets/"

@httpretty.activate
def test_get_dataset():
    """Test get_dataset function"""
    data_file_name = "provenance_data.json"
    with open(os.path.join(DATASET_PATH, data_file_name)) as data_file:
        data = data_file.read()

    httpretty.register_uri(httpretty.GET,
                           "https://metax-test.csc.fi/rest/v1/datasets/1",
                           body=data,
                           status=200,
                           content_type='application/json'
                          )

    dataset = Metax().get_data('datasets', "1")
    print dataset
    print type(dataset)
    assert dataset["research_dataset"]["provenance"][0]['type']['pref_label']\
        [0]['en'] == 'creation'

#TODO: test for other entities: contracts, files...
