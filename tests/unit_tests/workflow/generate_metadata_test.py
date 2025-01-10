"""Unit tests for GenerateMetadata task."""
import copy

import pytest
from metax_access.metax import DS_STATE_TECHNICAL_METADATA_GENERATED

import tests.metax_data
import tests.utils
from siptools_research.workflow import generate_metadata


@pytest.mark.usefixtures('testmongoclient')
def test_generatemetadata(config, workspace, requests_mock):
    """Test metadata generation.

    :param config: Configuration file
    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    """
    # Create a dataset that contains one text file which is available in
    # Ida
    textfile_metadata = copy.deepcopy(tests.metax_data.files.TXT_FILE)
    dataset_metadata = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset_metadata['identifier'] = workspace.name
    tests.utils.add_metax_v2_dataset(requests_mock,
                                  dataset=dataset_metadata,
                                  files=[textfile_metadata])

    textfile = workspace / 'metadata_generation/dataset_files/path/to/file'
    textfile.parent.mkdir(parents=True)
    textfile.write_text('foo')

    # Init and run task
    task = generate_metadata.GenerateMetadata(
        dataset_id=workspace.name,
        config=config,
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # The text file should be detected, and technical metadata should be
    # posted to Metax
    file_metadata_patch_request = requests_mock.request_history[-3]
    assert file_metadata_patch_request.url \
        == "https://metaksi/rest/v2/files/pid:urn:identifier"

    metadata = file_metadata_patch_request.json()['file_characteristics']
    assert metadata['file_format'] == 'text/plain'
    assert metadata['encoding'] == 'UTF-8'

    # The dataset preservation state should be updated
    preservation_state_patch_request = requests_mock.request_history[-1]
    assert preservation_state_patch_request.url \
        == f"https://metaksi/rest/v2/datasets/{workspace.name}"
    assert preservation_state_patch_request.json() \
        == {'preservation_state': DS_STATE_TECHNICAL_METADATA_GENERATED,
            'preservation_description': 'Metadata generated'}
