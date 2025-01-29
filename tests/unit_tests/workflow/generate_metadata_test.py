"""Unit tests for GenerateMetadata task."""
import copy

import pytest
from metax_access.metax import DS_STATE_TECHNICAL_METADATA_GENERATED

import tests.metax_data
import tests.metax_data.reference_data
import tests.utils
from siptools_research.workflow import generate_metadata


@pytest.mark.usefixtures('testmongoclient')
def test_generatemetadata(config, workspace, requests_mock, request):
    """Test metadata generation.

    :param config: Configuration file
    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param reque: Pytest CLI arguments
    """
    # Mock Metax API V3. Create a dataset that contains one text file
    # which is available in Ida.
    textfile_metadata = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    dataset_metadata = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset_metadata["id"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset_metadata,
                                  files=[textfile_metadata])
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)
    patch_characteristics = requests_mock.patch(
        "/v3/files/pid:urn:identifier/characteristics"
    )
    patch_dataset \
        = requests_mock.patch(f"/v3/datasets/{workspace.name}/preservation")

    # Mock Metax API V2
    textfile_metadata = copy.deepcopy(tests.metax_data.files.TXT_FILE)
    dataset_metadata = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset_metadata['identifier'] = workspace.name
    tests.utils.add_metax_v2_dataset(requests_mock,
                                  dataset=dataset_metadata,
                                  files=[textfile_metadata])
    v2_patch_dataset \
        = requests_mock.patch(f"/rest/v2/datasets/{workspace.name}")

    # Create the text file in workspace
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

    if request.config.getoption("--v3"):
        # The text file should be detected, and technical metadata should be
        # posted to Metax API V3
        metadata = patch_characteristics.last_request.json()
        assert metadata["file_format_version"]["url"] == "url-for-txt"
        assert metadata["encoding"] == "UTF-8"
        # The dataset preservation state should be updated
        assert patch_dataset.called_once
        assert patch_dataset.last_request.json() \
            == {"state": DS_STATE_TECHNICAL_METADATA_GENERATED,
                "description": {"en": "Metadata generated"}}
    else:
        # Same in Metax API V2
        file_metadata_patch_request = requests_mock.request_history[-3]
        assert file_metadata_patch_request.url \
            == "https://metax.localhost/rest/v2/files/pid:urn:identifier"

        metadata = file_metadata_patch_request.json()['file_characteristics']
        assert metadata['file_format'] == 'text/plain'
        assert metadata['encoding'] == 'UTF-8'
        assert v2_patch_dataset.called_once
        assert v2_patch_dataset.last_request.json() \
            == {'preservation_state': DS_STATE_TECHNICAL_METADATA_GENERATED,
                'preservation_description': 'Metadata generated'}
