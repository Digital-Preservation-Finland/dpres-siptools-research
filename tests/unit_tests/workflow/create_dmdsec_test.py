# coding=utf-8
"""Tests for :mod:`siptools_research.workflow.create_dmdsec` module"""
import os

import pytest
from lxml import etree

import tests.conftest
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_createdescriptivemetadata(testpath, requests_mock):
    """Test `CreateDescriptiveMetadata` task.

    :param testpath: Testpath fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    with open('tests/data/datacite_sample.xml', 'rb') as datacite:
        requests_mock.get(
            "https://metaksi/rest/v1/datasets/datacite_test_1"
            "?dataset_format=datacite&dummy_doi=false",
            content=datacite.read()
        )

    # Create workspace with "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = CreateDescriptiveMetadata(
        dataset_id="datacite_test_1",
        workspace=workspace,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # datacite.xml should be in workspace directory
    assert os.path.isfile(os.path.join(workspace, 'datacite.xml'))

    # Check that XML is created in sip creation directory
    for filename in os.listdir(os.path.join(workspace, 'sip-in-progress')):
        if filename.endswith('dmdsec.xml'):
            dmdsecfile = os.path.join(workspace, 'sip-in-progress', filename)
    assert os.path.isfile(dmdsecfile)

    # Check that the created xml-file contains correct elements.
    tree = etree.parse(dmdsecfile)
    common_xpath = '/mets:mets//mets:dmdSec/mets:mdWrap/mets:xmlData/'\
        'ns1:resource/'
    xpath_namespaces = {'mets': "http://www.loc.gov/METS/",
                        'ns1': "http://datacite.org/schema/kernel-4"}

    elements = tree.xpath('/mets:mets//mets:dmdSec/mets:mdWrap',
                          namespaces=xpath_namespaces)
    assert elements[0].attrib["OTHERMDTYPE"] == "DATACITE"
    assert elements[0].attrib["MDTYPEVERSION"] == "4.1"

    elements = tree.xpath(common_xpath + 'ns1:identifier',
                          namespaces=xpath_namespaces)
    assert elements[0].text == "10.1234/datacite-example"
    assert elements[0].attrib["identifierType"] == "DOI"

    elements = tree.xpath(
        common_xpath + 'ns1:creators/ns1:creator/ns1:creatorName',
        namespaces=xpath_namespaces
    )
    assert elements[0].text == u"Puupää, Pekka"

    elements = tree.xpath(
        common_xpath + 'ns1:creators/ns1:creator/ns1:nameIdentifier',
        namespaces=xpath_namespaces
    )
    assert elements[0].attrib["nameIdentifierScheme"] == "ORCID"
    assert elements[0].attrib["schemeURI"] == "http://orcid.org/"
    assert elements[0].text == "0000-0001-1234-5678"

    elements = tree.xpath(
        common_xpath + 'ns1:creators/ns1:creator/ns1:affiliation',
        namespaces=xpath_namespaces
    )
    assert elements[0].text == "TAMPEREEN EI-tURKULAINEN YLIOPISTO"

    elements = tree.xpath(common_xpath + "ns1:titles/ns1:title",
                          namespaces=xpath_namespaces)
    assert elements[0].attrib["{http://www.w3.org/XML/1998/namespace}lang"]\
        == "en-us"
    assert elements[0].text == "ExampleDataCite XML"
    assert elements[1].attrib["titleType"] == "Subtitle"
    assert elements[1].attrib["{http://www.w3.org/XML/1998/namespace}lang"]\
        == "en-us"
    assert elements[1].text == "Sample file for TPAS testing"

    elements = tree.xpath(common_xpath + "ns1:publisher",
                          namespaces=xpath_namespaces)
    assert elements[0].text == "CSC Digital Preservation"

    elements = tree.xpath(common_xpath + "ns1:publicationYear",
                          namespaces=xpath_namespaces)
    assert elements[0].text == "2017"
