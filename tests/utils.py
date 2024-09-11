"""Common utility functions for tests."""

import copy
import os
import uuid

import lxml.etree

import tests.metax_data.datasets
import tests.metax_data.files
import tests.metax_data.contracts


def add_metax_dataset(requests_mock,
                      dataset=copy.deepcopy(
                          tests.metax_data.datasets.BASE_DATASET
                      ),
                      datacite=tests.metax_data.datasets.BASE_DATACITE,
                      files=None,
                      contract=copy.deepcopy(
                          tests.metax_data.contracts.BASE_CONTRACT
                      )):
    """Mock responses of Metax APIs.

    Mocks datasets API, files API, contracts API, and directories API
    using requests-mock.

    Information about files and contract are inserted to dataset
    metadata. Metax directories API is mocked based on file paths and
    parent directories of provided file metadata. Technical metadata for
    audio, video, image etc. files is NOT mocked. The identifiers in
    provided resource dicts are used in mocked URLs.

    :param requests_mock: Mocker object used for creating responses
    :param dataset: dataset metadata dict
    :param datacite: dataset metadata in datacite XML format
    :param files: list of file metadata dicts
    :param contract: contract metadata dict
    :returns: ``None``
    """
    if files is None:
        files = {}

    # Add contract to dataset
    dataset['contract']['identifier'] = contract['contract_json']['identifier']

    for file_ in files:
        # Add files to dataset
        dataset["research_dataset"]["files"].append(
            {
                "identifier": file_['identifier'],
                "use_category": {
                    "pref_label": {
                        "en": "dummy-use-category"
                    }
                }
            }
        )

        # Mock Metax directories API
        requests_mock.get(
            "{}/directories/{}".format(
                tests.conftest.METAX_URL,
                file_['parent_directory']['identifier']
            ),
            json={
                "identifier": file_['parent_directory']['identifier'],
                "directory_path": os.path.dirname(file_['file_path'])
            }
        )

        # Mock Metax files API
        requests_mock.get("{}/files/{}".format(tests.conftest.METAX_URL,
                                               file_['identifier']),
                          json=file_)
        requests_mock.patch(
            "{}/files/{}".format(tests.conftest.METAX_URL,
                                 file_['identifier']),
            json=file_
        )

    # Mock Metax datasets API
    requests_mock.get(
        "{}/datasets/{}".format(tests.conftest.METAX_URL,
                                dataset['identifier']),
        json=dataset
    )
    requests_mock.patch(
        "{}/datasets/{}".format(tests.conftest.METAX_URL,
                                dataset['identifier']),
        json=dataset
    )
    requests_mock.patch(
        "{}/datasets/{}".format(tests.conftest.METAX_URL,
                                dataset['preservation_identifier']),
        json=dataset
    )
    requests_mock.get(
        "{}/datasets/{}?dataset_format=datacite".format(
            tests.conftest.METAX_URL,
            dataset['identifier']
        ),
        content=lxml.etree.tostring(datacite)
    )
    requests_mock.get(
        "{}/datasets/{}/files".format(tests.conftest.METAX_URL,
                                      dataset['identifier']),
        json=files
    )

    # Mock Metax contracts API
    requests_mock.get(
        "{}/contracts/{}".format(tests.conftest.METAX_URL,
                                 contract['contract_json']["identifier"]),
        json=contract
    )


def add_mock_ida_download(requests_mock, dataset_id, filename, content):
    """
    Mock IDA requests for downloading a file from IDA
    """
    # In reality the returned token would have the dataset id and file path
    # encoded in it. For mocking purposes any random string that can be used to
    # connect the authorize and download requests together is good enough.
    token = str(uuid.uuid4())
    requests_mock.post(
        "https://download.dl-authorize.test/authorize",
        json={
            "token": token
        },
        additional_matcher=lambda req: (
            req.json()["file"] == filename
            and req.json()["dataset"] == dataset_id
        )
    )

    return requests_mock.get(
        f"https://download.dl.test/download?token={token}",
        content=content
    )
