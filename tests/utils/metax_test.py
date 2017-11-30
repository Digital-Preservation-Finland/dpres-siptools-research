"""Tests for ``siptools_research.utils.metax`` module"""
from siptools_research.utils.metax import Metax
import lxml.etree

def test_get_dataset(testmetax):
    """Test get_dataset function. Reads sample dataset JSON from testmetax and
    checks that returned dict contains the correct values."""
    dataset = Metax('tests/data/siptools_research.conf').get_data('datasets', "1")
    print dataset
    print type(dataset)
    assert dataset["research_dataset"]["provenance"][0]['type']['pref_label']\
        ['en'] == 'creation'

def test_get_xml(testmetax):
    """Test get_xml function. Reads some test xml from testmetax checks that
    the function returns dictionary with correct items
    """
    xml = Metax('tests/data/siptools_research.conf').get_xml('files', "metax_xml_test")
    assert isinstance(xml, dict)
    assert xml.keys() == ['http://www.arkivverket.no/standarder/addml',
                          'http://www.loc.gov/METS/']
    assert xml.values()[0].tag == 'root'
    assert xml.values()[1].tag == 'root'

def test_reading_config_file():
    """Test that options from configuration file are used instead of default
    values."""

    # Init metax using config file that has no metax-related options
    metax_client1 = Metax("tests/data/configuration_files/metax_test1.conf")
    # The client should have default values for each options
    assert metax_client1.baseurl == "https://metax-test.csc.fi/rest/v1/"
    assert metax_client1.username == "tpas"
    assert metax_client1.password == ""

    # Init metax using config file that has options for Metax
    metax_client2 = Metax("tests/data/configuration_files/metax_test2.conf")
    # The client should have default values for each options
    assert metax_client2.baseurl == "https://testurl.fi/rest/v1/"
    assert metax_client2.username == "teppo"
    assert metax_client2.password == "VerySecret123"

#TODO: test for other entities: contracts, files...

#TODO: Test for set_preservation_state function
