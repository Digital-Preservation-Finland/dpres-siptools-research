# coding=utf-8
"""Test the `siptools_research.create_sip.create_dmdsec` module"""

import os
import shutil
from lxml import etree
from siptools_research.workflow_b.create_dmdsec\
    import CreateDescriptiveMetadata
DATASET_PATH = "tests/data/metax_datasets/"

def test_createdescriptivemetadata(testpath):
    """Test `CreateDescriptiveMetadata` task.

    :testpath: Testpath fixture
    :returns: None
    """

    # Create workspace with "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Copy sample datacite.xml to workspace directory
    shutil.copy('tests/data/datacite_sample.xml',
                os.path.join(workspace, 'sip-in-progress', 'datacite.xml'))

    # Init task
    task = CreateDescriptiveMetadata(home_path=workspace,
                                     workspace=workspace)
    assert not task.complete()

    # Run task. Task returns generator, so it must be iterated to really run
    # the code
    returned_tasks = task.run()
    for task in returned_tasks:
        pass
    assert task.complete()

    # Check that XML is created
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'dmdsec.xml'))

    # Check that the created xml-file contains correct elements.
    tree = etree.parse(os.path.join(workspace,
                                    'sip-in-progress',
                                    'dmdsec.xml'))
    common_xpath = '/mets:mets//mets:dmdSec/mets:mdWrap/mets:xmlData/'\
        'ns1:resource/'
    xpath_namespaces = {'mets': "http://www.loc.gov/METS/",
                        'ns1': "http://datacite.org/schema/kernel-3"}

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


# TODO: Test for CreateDescriptiveMetadata.requires()

# TODO: Test for ReadyForThis

# TODO: Test for DmdsecComplete
