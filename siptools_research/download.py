"""IDA and upload-rest-api interface module."""
import hashlib
import os
from pathlib import Path

from upload_rest_api.models.file_entry import FileEntry

from siptools_research.config import Configuration

CHECKSUM_ALGO_TO_CONSTRUCTOR = {
    "sha256": hashlib.sha256,
    "sha512": hashlib.sha512,
    "md5": hashlib.md5
}


PAS_STORAGE_SERVICE = "pas"


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
    identifier = file_metadata["storage_identifier"]
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


def _download_ida_file(file_metadata, path, conf):
    """Download file from IDA.

    :param file_metadata: Metax file metadata
    :param path: Path where file is downloaded
    :param conf: Configuration object
    :returns: Path to the file
    """
    project = file_metadata["csc_project"]

    checksum_algo, checksum_value = file_metadata["checksum"].split(":")
    hasher = CHECKSUM_ALGO_TO_CONSTRUCTOR[checksum_algo]()

    # Write the stream to disk and calculate checksum
    tmp_path = path.parent / (path.name + '.tmp')

    try:
        offline_message = Path(conf.get("ida_mount"), "OFFLINE").read_text()

        raise FileAccessError(
            f"IDA service offline with message: {offline_message}. "
            "Please try again later."
        )
    except FileNotFoundError:
        pass  # No service break active

    # Project name is provided twice in the path. The second part can
    # also be suffixed with '+' to refer to the staging directory. But
    # we always download the frozen file.
    mount_path = (
        Path(conf.get("ida_mount")) / project / project
        / file_metadata["pathname"].strip("/")
    )
    if not mount_path.exists():
        raise FileNotAvailableError("File not available.")

    with tmp_path.open('wb') as new_file, mount_path.open("rb") as src_file:
        while chunk := src_file.read(4096):
            new_file.write(chunk)
            hasher.update(chunk)

    # Ensure the checksum matches with that recorded in Metax
    if (hash_ := hasher.hexdigest()) != checksum_value:
        tmp_path.unlink()
        raise ValueError(
            f"Computed checksum was different. "
            f"Expected {checksum_value}, got {hash_}."
        )

    # To make the download atomic, the actual file is created after the
    # stream has been successfully written to disk.
    tmp_path.rename(path)


def download_file(
        file_metadata,
        path,
        config_file="/etc/siptools_research.conf",
):
    """Get file from IDA or upload-rest-api and create a hard
    link to linkpath.

    :param file_metadata: File metadata from Metax
    :param path: Path where the file is downloaded
    :param config_file: Configuration file
    """
    conf = Configuration(config_file)
    file_storage = file_metadata["storage_service"]

    if file_storage == PAS_STORAGE_SERVICE:
        _link_local_file(
            file_metadata=file_metadata,
            path=path,
        )
    else:
        _download_ida_file(
            file_metadata=file_metadata,
            path=path,
            conf=conf
        )
