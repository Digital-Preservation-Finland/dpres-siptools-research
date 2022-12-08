"""IDA and upload-rest-api interface module"""
import os
import time

import requests
from requests.exceptions import HTTPError
from siptools_research.config import Configuration

from upload_rest_api.models import FileEntry


class FileNotAvailableError(Exception):
    """Exception raised when file is not available."""


class FileLockError(Exception):
    """Exception raised when file is locked by another process"""


class FileAccessError(Exception):
    """Raised when file cannot be accessed."""


def _get_local_file(file_metadata, conf):
    """Get upload-rest-api file.

    :param file_metadata: Metax file metadata
    :param conf: Configuration object
    :returns: Path to the file
    """
    identifier = file_metadata["identifier"]
    try:
        filepath = FileEntry.objects.get(identifier=identifier).path
    except FileEntry.DoesNotExist:
        filepath = None
    cache_path = os.path.join(
        conf.get("packaging_root"), "file_cache", identifier
    )

    if (filepath is None) or (not os.path.isfile(filepath)):
        raise FileNotAvailableError(
            f"File '{file_metadata['file_path']}' not found in pre-ingest "
            f"file storage"
        )

    if not os.path.exists(cache_path):
        os.link(filepath, cache_path)

    return cache_path


def _get_ida_file(file_metadata, dataset_id, conf):
    """Get file from IDA. If file is already cached, just return path to it.

    :param file_metadata: Metax file metadata
    :param dataset_id: Identifier for the dataset containing the file
    :param conf: Configuration object
    :returns: Path to the file
    """
    identifier = file_metadata["identifier"]
    filepath = os.path.join(
        conf.get("packaging_root"), "file_cache", identifier
    )
    tmp_path = os.path.join(
        conf.get("packaging_root"), "tmp", f"IDA-{identifier}"
    )

    ida_token = conf.get('ida_token')
    verify = conf.getboolean('ida_ssl_verification')

    auth_base_url = conf.get('ida_dl_authorize_url')
    download_base_url = conf.get('ida_dl_url')

    # Check that no other process is fetching the file from IDA
    if os.path.exists(tmp_path):
        raise FileLockError(f"Lock file '{tmp_path}' exists")

    # If cached file doesn't exists, GET it from IDA
    if not os.path.exists(filepath):
        try:
            # Retrieve a single-use download token
            response = requests.post(
                f"{auth_base_url}/authorize",
                headers={
                    "Authorization": f"Bearer {ida_token}"
                },
                verify=verify,
                json={
                    "dataset": dataset_id,
                    "file": file_metadata["file_path"]
                }
            )
            response.raise_for_status()

            token = response.json()["token"]
        except HTTPError as error:
            if error.response.status_code == 404:
                raise FileNotAvailableError(
                    f"File '{file_metadata['file_path']}' not found in Ida"
                )
            elif error.response.status_code == 502:
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

        # Write the stream to tmp_path, create a hard link to file_cache
        # and cleanup the tmp_path
        try:
            with open(tmp_path, 'wb') as new_file:
                for chunk in response.iter_content(chunk_size=1024):
                    new_file.write(chunk)
            os.link(tmp_path, filepath)
        finally:
            os.remove(tmp_path)

    return filepath


def download_file(
        file_metadata,
        dataset_id,
        linkpath="",
        config_file="/etc/siptools_research.conf",
):
    """Get file from IDA or upload-rest-api and create a hard
    link to linkpath.

    :param file_metadata: File metadata from Metax
    :param dataset_id: Identifier for the dataset containing the file
    :param linkpath: Path where the hard link is created
    :param config_file: Configuration file
    :returns: ``None``
    """
    conf = Configuration(config_file)
    pas_storage_id = conf.get("pas_storage_id")
    file_storage = file_metadata["file_storage"]["identifier"]

    if file_storage == pas_storage_id:
        filepath = _get_local_file(
            file_metadata=file_metadata,
            conf=conf
        )
    else:
        filepath = _get_ida_file(
            file_metadata=file_metadata,
            dataset_id=dataset_id,
            conf=conf
        )

    if linkpath:
        os.link(filepath, linkpath)


def clean_file_cache(config_file):
    """Remove all files from <packaging_root>/file_cache that have not been
    accessed in two weeks.

    :returns: ``None``
    """
    conf = Configuration(config_file)
    files_path = os.path.join(conf.get("packaging_root"), "file_cache")

    current_time = time.time()
    time_lim = 60*60*24*14

    # Remove all old files
    for dirpath, _, files in os.walk(files_path):
        for filename in files:
            filepath = os.path.join(dirpath, filename)
            last_access = os.stat(filepath).st_atime
            if current_time - last_access > time_lim:
                os.remove(filepath)
