"""Tests for ``siptools_research.utils.metax`` module"""
from siptools_research.utils.metax import Metax

def test_get_dataset(testmetax):
    """Test get_dataset function"""
    dataset = Metax().get_data('datasets', "1")
    print dataset
    print type(dataset)
    assert dataset["research_dataset"]["provenance"][0]['type']['pref_label']\
        ['en'] == 'creation'

def test_get_xml(testmetax):
    """Test get_xml function"""
    xml = Metax().get_xml('files', "metax_xml_test")
    print xml

#TODO: test for other entities: contracts, files...

#TODO: Test for set_preservation_state function
