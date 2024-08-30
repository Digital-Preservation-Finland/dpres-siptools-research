"""IDA and upload-rest-api interface module"""
from pathlib import Path
import os

import requests
from requests.exceptions import HTTPError
from upload_rest_api.models.file_entry import FileEntry

from siptools_research.config import Configuration


class FileNotAvailableError(Exception):
    """Exception raised when file is not available."""


class FileAccessError(Exception):
    """Raised when file cannot be accessed."""


def _link_local_file(file_metadata, path):
    """Create a hard link to upload-rest-api file.

    :param file_metadata: Metax file metadata
    :param path: Path where file is downloaded
    :param conf: Configuration object
    """
    identifier = file_metadata["id"]
    try:
        filepath = FileEntry.objects.get(identifier=identifier).path
    except FileEntry.DoesNotExist:
        filepath = None

    # Even if FileEntry (which should not be used for this purpose
    # anyway: TPASPKT-516) exists it does not mean that the file
    # actually exists in upload-rest-api. Therefore we must check that
    # file really exists.
    if (filepath is None) or (not Path(filepath).is_file()):
        raise FileNotAvailableError(
            f"File '{file_metadata['pathname']}' not found in pre-ingest "
            f"file storage"
        )

    os.link(filepath, path)


def _download_ida_file(file_metadata, dataset_id, path, conf):
    """Download file from IDA.

    :param file_metadata: Metax file metadata
    :param dataset_id: Identifier for the dataset containing the file
    :param path: Path where file is downloaded
    :param conf: Configuration object
    :returns: Path to the file
    """
    fd_download_service_token = conf.get('fd_download_service_token')
    verify = conf.getboolean('fd_download_service_ssl_verification')

    auth_base_url = conf.get('fd_download_service_authorize_url')
    download_base_url = conf.get('fd_download_service_url')

    try:
        # Retrieve a single-use download token
        response = requests.post(
            f"{auth_base_url}/authorize",
            headers={
                "Authorization": f"Bearer {fd_download_service_token}"
            },
            verify=verify,
            json={
                "dataset": dataset_id,
                "file": file_metadata["pathname"]
            }
        )
        response.raise_for_status()

        token = response.json()["token"]

    except HTTPError as error:
        if error.response.status_code == 404:
            raise FileNotAvailableError(
                f"File '{file_metadata['pathname']}' not found in Ida"
            )
        if error.response.status_code == 502:
            raise FileAccessError("Ida service temporarily unavailable. "
                                  "Please, try again later.")

        raise

    response = requests.get(
        f"{download_base_url}/download",
        params={
            "token": token
        },
        verify=verify,
        stream=True
    )
    response.raise_for_status()

    # Write the stream to disk.
    tmp_path = path.parent / (path.name + '.tmp')
    with open(tmp_path, 'wb') as new_file:
        for chunk in response.iter_content(chunk_size=1024):
            new_file.write(chunk)

    # To make the download atomic, the actual file is created after the
    # stream has been successfully written to disk.
    tmp_path.rename(path)


def download_file(
        file_metadata,
        dataset_id,
        path,
        config_file="/etc/siptools_research.conf",
):
    """Get file from IDA or upload-rest-api and create a hard
    link to linkpath.

    :param file_metadata: File metadata from Metax
    :param dataset_id: Identifier for the dataset containing the file
    :param path: Path where the file is downloaded
    :param config_file: Configuration file
    """
    conf = Configuration(config_file)
    pas_storage_id = conf.get("pas_storage_id")
    file_storage = file_metadata["storage_identifier"]

    if file_storage == pas_storage_id:
        _link_local_file(
            file_metadata=file_metadata,
            path=path,
        )
    else:
        _download_ida_file(
            file_metadata=file_metadata,
            path=path,
            dataset_id=dataset_id,
            conf=conf
        )
