"""Test module ``siptools_research.workflow.create_mets``"""
import shutil
import os
import pytest
import tests.conftest
import lxml
from siptools_research.workflow.create_mets import CreateMets
from siptools.scripts import import_object
from siptools.scripts import import_description, premis_event, \
    compile_structmap


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_mets_ok(testpath):
    """Test the workflow task CreateMets.

    :testpath: Temporary directory fixture
    :returns: None
    """
    # Create workspace with contents required by the tested task
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    create_sip = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(create_sip)
    create_test_data(workspace=create_sip)

    # Init and run task
    task = CreateMets(workspace=workspace, dataset_id='create_mets_dataset',
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()
    assert os.path.isfile(os.path.join(create_sip, 'mets.xml'))

    # Check that the created xml-file contains correct elements.
    tree = lxml.etree.parse(os.path.join(create_sip, 'mets.xml'))

    elements = tree.xpath('/mets:mets',
                            namespaces={'mets': "http://www.loc.gov/METS/"}
                          )

    assert elements[0].attrib["OBJID"] == "create_mets_dataset"

    assert len(tree.xpath(
        '/mets:mets[@fi:CONTRACTID="urn:uuid:99ddffff-2f73-46b0-92d1-614409d83001"]', 
        namespaces={'mets': "http://www.loc.gov/METS/", 
                    'fi': "http://digitalpreservation.fi/schemas/mets/fi-extensions"})) == 1

    elements = tree.xpath('/mets:mets/mets:metsHdr/mets:agent/mets:name',
                            namespaces={'mets': "http://www.loc.gov/METS/"}
                          )
    assert elements[0].text == "Helsingin Yliopisto"


def create_test_data(workspace):
    """Create data needed to run ``CreateMets`` task

    :workspace: Workspace directory in which the data is created.
    """

    # Copy sample datacite.xml to workspace directory
    dmdpath = os.path.join(workspace, 'datacite.xml')
    shutil.copy('tests/data/datacite_sample.xml', dmdpath)

    # Create dmdsec
    import_description.main([dmdpath, '--workspace', workspace])

     # Create provenance
    premis_event.main(['creation', '2016-10-13T12:30:55',
                       '--workspace', workspace,
                       '--event_outcome', 'success',
                       '--event_detail', 'Poika, 2.985 kg'])

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main(['--workspace', workspace,
                        '--skip_inspection',
                        test_data_folder])

    # Create structmap
    compile_structmap.main(['--workspace', workspace])
