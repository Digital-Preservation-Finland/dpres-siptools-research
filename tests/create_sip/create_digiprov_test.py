"""Test the `siptools_research.create_sip.create_digiprov` module"""

import os
import httpretty
from siptools_research.create_sip.create_digiprov \
    import CreateProvenanceInformation

DATASET_PATH = "tests/data/metax_datasets/"

def test_createprovenanceinformation(testpath):
    """Test `CreateProvenanceInformation` task.

    :testpath: Testpath fixture
    :returns: None
    """

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


    # Create workspace with "logs" directory in temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))

    # Create file in workspace directory
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Init task
    task = CreateProvenanceInformation(home_path=workspace,
                                       workspace=workspace)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Check that XML is created
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'creation-event.xml'))

    # Disable fake http-server
    httpretty.disable()

    # TODO: Test for CreateProvenanceInformation.requires()

    # TODO: Test for DigiprovComplete.requires()

