"""Tests for create_digiprov script"""
import os
import httpretty
from lxml import etree
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

    # Check that the created xml-file contains correct elements.
    tree = etree.parse(os.path.join(testpath, 'creation-event.xml'))

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventIdentifier'\
                          '/premis:eventIdentifierType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "UUID"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "creation"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventDateTime',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "2014-01-01T08:19:58Z"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventDetail',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "Description of provenance"
