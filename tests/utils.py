"""Common utility functions for tests."""

import copy
import uuid

import lxml.etree

from tests.metax_data.contracts import BASE_CONTRACT
from tests.metax_data.datasets import BASE_DATACITE, BASE_DATASET


def add_metax_dataset(
        requests_mock,
        dataset=None,
        datacite=BASE_DATACITE,
        files=None,
        contract=BASE_CONTRACT,
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
        dataset = copy.deepcopy(BASE_DATASET)

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

    pas_dataset_id = (
        dataset.get('preservation', {}).get('dataset_version', {}).get('id')
        or dataset['id']
    )
    requests_mock.get(
        f"/v3/datasets/{pas_dataset_id}/metadata-download?format=datacite",
        content=lxml.etree.tostring(datacite)
    )
    requests_mock.get(
        f"/v3/datasets/{dataset['id']}/files",
        json={"next": None, "results": files}
    )
    requests_mock.post(f"/v3/datasets/{dataset['id']}/create-preservation-version")

    # Mock Metax contracts API
    requests_mock.get("/v3/contracts/{}".format(contract["id"]), json=contract)


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
