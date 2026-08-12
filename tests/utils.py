"""Common utility functions for tests."""

import copy
import uuid

import lxml.etree

from pathlib import Path

from metax_access.template_data import CONTRACT, DATASET
from tests.metax_data.datasets import BASE_DATACITE


def add_metax_dataset(
        requests_mock,
        dataset=None,
        datacite=BASE_DATACITE,
        files=None,
        contract=CONTRACT,
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
        dataset = copy.deepcopy(DATASET)

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

    # Mock patching dataset files. This is used to lock/unlock all files
    # of the dataset, when the dataset is locked.
    requests_mock.post("/v3/files/patch-many")

    return dataset


def add_mock_ida_file(ida_mount_path: Path, project_id: str, filename: str,
                      content: bytes) -> None:
    """Mock IDA file mount by adding a file to the location."""
    path = ida_mount_path/project_id/project_id/filename.strip('/')
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_bytes(content)


def add_dps_upload_mock(requests_mock, transfer_id,
                        access_rest_api_host="https://access.localhost"):
    """Mock the DPS REST API v3 TUS endpoint used for uploading SIPs.

    :param requests_mock: Mocker object used for creating responses
    :param transfer_id: Transfer ID assigned to the uploaded SIP
    :param access_rest_api_host: Base URL of DPS REST API
    """
    tus_url = f"{access_rest_api_host}/api/3.0/transfers"

    def patch_response(request, context):
        context.headers = {
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": request.headers["Content-Length"],
        }
        return b""

    requests_mock.post(
        tus_url,
        headers={
            "Location": f"{tus_url}/{transfer_id}",
            "Tus-Resumable": "1.0.0",
        },
        status_code=201,
    )
    requests_mock.patch(
        f"{tus_url}/{transfer_id}",
        content=patch_response,
        status_code=204,
    )


def add_dps_transfer_mock(requests_mock, contract_id, transfer_id, status,
                          access_rest_api_host="https://access.localhost"):
    """Mock the DPS REST API v3 endpoint used to poll a transfer's
    status.

    :param requests_mock: Mocker object used for creating responses
    :param contract_id: Contract identifier
    :param transfer_id: Transfer ID to mock
    :param status: Status reported for the transfer, e.g. "accepted",
        "rejected" or "in_progress"
    :param access_rest_api_host: Base URL of DPS REST API
    """
    transfer_url = (
        f"{access_rest_api_host}/api/3.0/{contract_id}/transfers/"
        f"{transfer_id}"
    )

    requests_mock.get(
        transfer_url,
        json={
            "data": {
                "transfer_id": transfer_id,
                "filename": f"{transfer_id}.tar",
                "status": status,
                "actions": {},
                "sip": {},
                "timestamp": "2000-01-01T00:00:00Z",
            }
        }
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
