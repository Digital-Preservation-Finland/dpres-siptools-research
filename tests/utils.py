"""Common utility functions for tests."""

import copy
import uuid

import lxml.etree

import tests.metax_data.contracts
import tests.metax_data.contractsV3
import tests.metax_data.datasets
import tests.metax_data.datasetsV3
import tests.metax_data.files
import tests.metax_data.filesV3


def add_metax_dataset(
        requests_mock,
        dataset=None,
        datacite=tests.metax_data.datasets.BASE_DATACITE,
        files=None,
        contract=tests.metax_data.contractsV3.BASE_CONTRACT,
):
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
    """
    if dataset is None:
        dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)

    if files is None:
        files = {}

    # Add contract to dataset
    dataset["preservation"]["contract"] = contract["id"]

    # Mock Metax files API
    for file in files:
        requests_mock.patch(f"/v3/files/{file['id']}", json=file)
        requests_mock.patch(f"/v3/files/{file['id']}/characteristics",
                            json=file)

    # Mock Metax datasets API
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)
    requests_mock.patch(f"/v3/datasets/{dataset['id']}/preservation",
                        json=dataset)
    requests_mock.get(
        f"/v3/datasets/{dataset['id']}/metadata-download?format=datacite",
        content=lxml.etree.tostring(datacite)
    )
    requests_mock.get(
        f"/v3/datasets/{dataset['id']}/files",
        json={"next": None, "results": files}
    )

    # Mock Metax contracts API
    requests_mock.get("/v3/contracts/{}".format(contract["id"]), json=contract)


def add_metax_v2_dataset(
        requests_mock,
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
        # Mock Metax files API
        requests_mock.get(
            "/rest/v2/files/{}".format(file_['identifier']),
            json=file_
        )
        requests_mock.patch(
            "/rest/v2/files/{}".format(file_['identifier']),
            json=file_
        )

    # Mock Metax datasets API
    requests_mock.get(
        "/rest/v2/datasets/{}".format(dataset['identifier']),
        json=dataset
    )
    requests_mock.get(
        "/rest/v2/datacatalogs/{}".format(dataset['data_catalog']['identifier']),
        json={}
    )
    requests_mock.get(
        "/rest/v2/contracts/{}".format(dataset['contract']['identifier']),
        json={}
    )
    requests_mock.patch(
        "/rest/v2/datasets/{}".format(dataset['identifier']),
        json=dataset
    )
    requests_mock.get(
        "/rest/v2/datasets/{}?dataset_format=datacite".format(dataset['identifier']),
        content=lxml.etree.tostring(datacite)
    )
    requests_mock.get(
        "/rest/v2/datasets/{}/files".format(dataset["identifier"]),
        json=files
    )

    # Mock Metax contracts API
    requests_mock.get(
        "/rest/v2/contracts/{}".format(contract["contract_json"]["identifier"]),
        json=contract
    )


def add_mock_ida_download(requests_mock, dataset_id, filename, content):
    """Mock IDA requests for downloading a file from IDA."""
    # In reality the returned token would have the dataset id and file
    # path encoded in it. For mocking purposes any random string that
    # can be used to connect the authorize and download requests
    # together is good enough.
    token = str(uuid.uuid4())
    requests_mock.post(
        "https://download.localhost:4431/authorize",
        json={
            "token": token
        },
        additional_matcher=lambda req: (
            req.json()["file"] == filename
            and req.json()["dataset"] == dataset_id
        )
    )

    return requests_mock.get(
        f"https://download.localhost:4430/download?token={token}",
        content=content
    )
