# coding=utf-8
"""Tests for ``siptools_research.utils.metax`` module"""
import json
from siptools_research.utils.metax import Metax
import httpretty

def test_get_dataset(testmetax):
    """Test get_dataset function. Reads sample dataset JSON from testmetax and
    checks that returned dict contains the correct values."""
    client = Metax('tests/data/siptools_research.conf')
    dataset = client.get_data('datasets', "mets_test_dataset_1")
    print dataset
    print type(dataset)
    assert dataset["research_dataset"]["provenance"][0]['type']['pref_label']\
        ['en'] == 'creation'


def test_get_xml(testmetax):
    """Test get_xml function. Reads some test xml from testmetax checks that
    the function returns dictionary with correct items
    """
    # Get XML objects from Metax
    client = Metax('tests/data/siptools_research.conf')
    xml_dict = client.get_xml('files', "metax_xml_test")
    assert isinstance(xml_dict, dict)

    # The keys of returned dictionary should be xml namespace urls and
    # the values of returned dictionary should be lxml.etree.ElementTree
    # objects with the namespaces defined
    addml_url = "http://www.arkivverket.no/standarder/addml"
    assert xml_dict[addml_url].getroot().nsmap['addml'] == addml_url
    mets_url = "http://www.loc.gov/METS/"
    assert xml_dict[mets_url].getroot().nsmap['mets'] == mets_url


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


def test_get_datacite(testmetax):
    """Test get_datacite function. Read one field from returned etree object
    and check its correctness"""
    client = Metax('tests/data/siptools_research.conf')
    xml = client.get_datacite("datacite_test_1")

    # Read field "creatorName" from xml file
    ns_string = 'http://datacite.org/schema/kernel-3'
    xpath_str = '/ns:resource/ns:creators/ns:creator/ns:creatorName'
    creatorname = xml.xpath(xpath_str, namespaces={'ns': ns_string})[0].text
    # Check that "creatorName" is same as in the original XML file
    assert creatorname == u"Puupää, Pekka"


def test_set_preservation_state(testmetax):
    """Test set_preservation_state function. Metadata in Metax is modified by
    sending HTTP PATCH request with modified metadata in JSON format. This test
    checks that correct HTTP request is sent to Metax. The effect of the
    request is not tested.
    """
    client = Metax('tests/data/siptools_research.conf')
    client.set_preservation_state("mets_test_dataset_1", 7,
                                  'Accepted to preservation')

    # Check the body of last HTTP request
    request_body = json.loads(httpretty.last_request().body)
    assert request_body["preservation_state"] == 7
    assert request_body["preservation_state_description"] \
        == "Accepted to preservation"

    # Check the method of last HTTP request
    assert httpretty.last_request().method == 'PATCH'

#TODO: test for retrieving other entities: contracts, files...
