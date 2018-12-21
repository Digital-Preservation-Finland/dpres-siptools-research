"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os
import pytest
import tests.conftest
from siptools_research.workflow.create_logical_structmap \
    import CreateLogicalStructMap
from siptools.scripts import import_description
from siptools.scripts import import_object
from siptools.scripts import premis_event
from siptools.scripts import compile_structmap
from siptools.xml.mets import NAMESPACES
import lxml.etree


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_structmap_ok(testpath):
    """Test the workflow task CreateLogicalStructMap.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create sip directory in workspace
    sip_creation_path = os.path.join(testpath, "sip-in-progress")
    os.makedirs(os.path.join(testpath, 'sip-in-progress'))

    # Create digiprov
    event_type = 'creation'
    event_datetime = '2014-12-31T08:19:58Z'
    event_detail = 'Description of provenance'
    premis_event.main([
        event_type, event_datetime,
        "--event_detail", event_detail,
        "--event_outcome", 'success',
        "--workspace", sip_creation_path
    ])

    # Create dmdsec (required to create valid physical structmap)
    import_description.main(['tests/data/datacite_sample.xml',
                             '--workspace', sip_creation_path])
    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main(['--workspace', sip_creation_path,
                        '--skip_inspection',
                        test_data_folder])

    # Create physical structmap
    compile_structmap.main(['--workspace', sip_creation_path,
                            '--type_attr', 'Fairdata-physical'])

    # Init and run CreateStructMap task
    task = CreateLogicalStructMap(workspace=testpath,
                                  dataset_id='create_structmap_test_dataset',
                                  config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    validate_logical_structmap_file(os.path.join(sip_creation_path,
                                                 'logical_structmap.xml'))


def validate_logical_structmap_file(logical_structmap_file):
    """Validates logical structuremap XML-file. Checks that XML-file has the
    correct elements. Raises exception if XML is not valid.

    :param logical_structmap_file: XML file to be validated
    :returns: ``None``
    """
    tree = lxml.etree.parse(logical_structmap_file)

    directories = tree.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
        namespaces=NAMESPACES
    )
    assert len(directories) == 5
    assert 'Documentation files' in directories
    assert 'Machine-readable metadata' in directories
    assert 'Access and use rights files' in directories
    assert 'Software files' in directories
    assert 'Publication files' in directories

    assert tree.xpath('/mets:mets/mets:structMap',
                      namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "Fairdata-logical"

    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Documentation files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 5
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Machine-readable metadata"]/mets:fptr'
                          '[@FILEID]',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Access and use rights files"]/mets:fptr'
                          '/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Software files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Publication files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
