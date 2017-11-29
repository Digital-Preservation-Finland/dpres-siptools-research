"""Tests for ``siptools_research.utils.metax`` module"""
from siptools_research.utils.metax import Metax
import lxml.etree

def test_get_dataset(testmetax):
    """Test get_dataset function. Reads sample dataset JSON from testmetax and
    checks that returned dict contains the correct values."""
    dataset = Metax().get_data('datasets', "1")
    print dataset
    print type(dataset)
    assert dataset["research_dataset"]["provenance"][0]['type']['pref_label']\
        ['en'] == 'creation'

def test_get_xml(testmetax):
    """Test get_xml function. Reads some test xml from testmetax checks that
    the function returns dictionary with correct items
    """
    xml = Metax().get_xml('files', "metax_xml_test")
    assert isinstance(xml, dict)
    assert xml.keys() == ['http://www.arkivverket.no/standarder/addml',
                          'http://www.loc.gov/METS/']
    assert xml.values()[0].tag == 'root'
    assert xml.values()[1].tag == 'root'

#TODO: test for other entities: contracts, files...

#TODO: Test for set_preservation_state function
