"""Unit tests for GenerateMetadata task."""
import copy

import pytest
from metax_access.metax import DS_STATE_TECHNICAL_METADATA_GENERATED

from siptools_research.workflow import generate_metadata
import tests.metax_data.files
import tests.utils


@pytest.mark.usefixtures('testmongoclient')
def test_generatemetadata(workspace, requests_mock):
    """Test metadata generation.

    :param testpath: Temporary directory
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Create a dataset that contains one text file which is available in
    # Ida
    textfile = copy.deepcopy(tests.metax_data.files.TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[textfile])
    tests.utils.add_mock_ida_download(requests_mock,
                                      dataset_id='dataset_identifier',
                                      filename='path/to/file',
                                      content=b'foo')

    # Init and run task
    task = generate_metadata.GenerateMetadata(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # The text file should be detected, and technical metadata should be
    # posted to Metax
    file_metadata_patch_request = requests_mock.request_history[-2]
    assert file_metadata_patch_request.url \
        == 'https://metaksi/rest/v2/files/pid:urn:identifier'
    metadata = file_metadata_patch_request.json()['file_characteristics']
    assert metadata['file_format'] == 'text/plain'
    assert metadata['encoding'] == 'UTF-8'

    # The dataset preservation state should be updated
    preservation_state_patch_request = requests_mock.request_history[-1]
    assert preservation_state_patch_request.url \
        == 'https://metaksi/rest/v2/datasets/dataset_identifier'
    assert preservation_state_patch_request.json() \
        == {'preservation_state': DS_STATE_TECHNICAL_METADATA_GENERATED,
            'preservation_description': 'Metadata generated'}