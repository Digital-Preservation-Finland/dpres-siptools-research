"""Unit tests for GenerateMetadata task."""
import copy

import pytest
from metax_access.metax import DS_STATE_TECHNICAL_METADATA_GENERATED

import tests.metax_data.reference_data
import tests.utils
from siptools_research.workflow import generate_metadata
from metax_access.template_data import DATASET
from tests.metax_data.files import TXT_FILE


def test_generatemetadata(config, workspace, requests_mock):
    """Test metadata generation.

    :param config: Configuration file
    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax. Create a dataset that contains one text file
    # which is available in Ida.
    textfile_metadata = copy.deepcopy(TXT_FILE)
    dataset_metadata = copy.deepcopy(DATASET)
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

    # The text file should be detected, and technical metadata should be
    # posted to Metax
    metadata = patch_characteristics.last_request.json()
    assert metadata["file_format_version"]["url"] == "url-for-txt"
    assert metadata["encoding"] == "UTF-8"
    # The dataset preservation state should be updated
    assert patch_dataset.called_once
    assert patch_dataset.last_request.json() \
        == {"state": DS_STATE_TECHNICAL_METADATA_GENERATED,
            "description": {"en": "Metadata generated"}}
