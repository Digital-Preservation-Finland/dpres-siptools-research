"""IDA and upload-rest-api interface module"""
import os
import time

import requests
from requests.exceptions import HTTPError, ConnectionError

from upload_rest_api.database import FilesCol

from siptools_research.config import Configuration


class FileNotFoundError(Exception):
    """Exception raised when files can't be accessed"""


class FileLockError(Exception):
    """Exception raised when file is locked by another process"""


def _get_response(identifier, conf, stream=False):
    """Send authenticated HTTP request to IDA.

    :param identifier: File identifier
    :param conf: Configuration object
    :param stream (bool): Stream the request content
    :returns: requests Response
    """
    user = conf.get('ida_user')
    password = conf.get('ida_password')
    baseurl = conf.get('ida_url')
    url = '%s/files/%s/download' % (baseurl, identifier)

    try:
        response = requests.get(url,
                                auth=(user, password),
                                verify=False,
                                stream=stream)
    except ConnectionError as exc:
        raise ConnectionError("Could not connect to Ida: %s" % str(exc))

    response.raise_for_status()
    return response


def _get_local_file(file_metadata, upload_files, conf):
    """Get upload-rest-api file.

    :param file_metadata: Metax file metadata
    :param upload_files: FilesCol object
    :param conf: Configuration object
    :returns: Path to the file
    """
    identifier = file_metadata["identifier"]
    filepath = upload_files.get_path(identifier)
    cache_path = os.path.join(
        conf.get("packaging_root"), "file_cache", identifier
    )

    if (filepath is None) or (not os.path.isfile(filepath)):
        raise FileNotFoundError(
            "File '%s' not found in pre-ingest file storage"
            % file_metadata["file_path"]
        )

    if not os.path.exists(cache_path):
        os.link(filepath, cache_path)

    return cache_path


def _get_ida_file(file_metadata, conf):
    """Get file from IDA. If file is already cached, just return path to it.

    :param file_metadata: Metax file metadata
    :param conf: Configuration object
    :returns: Path to the file
    """
    identifier = file_metadata["identifier"]
    filepath = os.path.join(
        conf.get("packaging_root"), "file_cache", identifier
    )
    tmp_path = os.path.join(
        conf.get("packaging_root"), "tmp", identifier
    )

    # Check that no other process is fetching the file from IDA
    if os.path.exists(tmp_path):
        raise FileLockError("Lock file '%s' exists" % tmp_path)

    # If cached file doesn't exists, GET it from IDA
    if not os.path.exists(filepath):
        # Send the GET request
        try:
            response = _get_response(identifier, conf, stream=True)
        except HTTPError as error:
            if error.response.status_code == 404:
                raise FileNotFoundError(
                    "File '%s' not found in Ida" % file_metadata["file_path"]
                )
            raise

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
        linkpath="",
        config_file="/etc/siptools_research.conf",
        upload_files=None
):
    """Get file from IDA or upload-rest-api and create a hard
    link to linkpath.

    :param file_metadata: File metadata from Metax
    :param linkpath: Path where the hard link is created
    :param config_file: Configuration file
    :param upload_files: FilesCol object
    :returns: ``None``
    """
    conf = Configuration(config_file)
    pas_storage_id = conf.get("pas_storage_id")
    file_storage = file_metadata["file_storage"]["identifier"]
    if upload_files is None:
        upload_files = FilesCol()

    if file_storage == pas_storage_id:
        filepath = _get_local_file(file_metadata, upload_files, conf)
    else:
        filepath = _get_ida_file(file_metadata, conf)

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
