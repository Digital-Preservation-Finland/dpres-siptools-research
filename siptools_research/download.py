"""IDA and upload-rest-api interface module."""
import hashlib
import os
from pathlib import Path

from metax_access.response import MetaxFile

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


def _link_local_file(upload_rest_api_path: Path,
                     file_metadata: MetaxFile,
                     path: Path) -> None:
    """Create a hard link to upload-rest-api file.

    `upload_rest_api_path` and `path` are expected to be on same
    filesystem, so hard link can be used to save some disk space.

    :param upload_rest_api_path: Root directory for files in
        upload-rest-api
    :param file_metadata: Metax file metadata
    :param path: Path where file is copied
    """
    source_path = upload_rest_api_path \
        / file_metadata["csc_project"] \
        / file_metadata["pathname"].strip("/")

    # Even if file_metadata exists it currently does not mean that the
    # file actually exists in upload-rest-api. See TPASPKT-749 for more
    # information. Therefore we must check that file really exists.
    if not source_path.is_file():
        raise FileNotAvailableError(
            f"File '{file_metadata['pathname']}' not found in pre-ingest "
            f"file storage"
        )

    os.link(source_path, path)


def _download_ida_file(ida_path: Path,
                       file_metadata: MetaxFile,
                       path: Path) -> None:
    """Download file from IDA.

    Ida files are expected to be on some kind of network mount. The file
    is copied to local disk, and the checksum of copied file is
    verified.

    :param ida_path: Root directory for files in Ida
    :param file_metadata: Metax file metadata
    :param path: Path where file is copied
    """
    project = file_metadata["csc_project"]
    checksum_algo, checksum_value = file_metadata["checksum"].split(":")

    try:
        offline_message = (ida_path / "OFFLINE").read_text()
        raise FileAccessError(
            f"IDA service offline with message: {offline_message}. "
            "Please try again later."
        )
    except FileNotFoundError:
        pass  # No service break active

    # Project name is provided twice in the path. The second part can
    # also be suffixed with '+' to refer to the staging directory. But
    # we always download the frozen file.
    project_path = ida_path / project / project
    mount_path = project_path / file_metadata["pathname"].strip("/")
    if not mount_path.exists():
        if project_path.exists():
            raise FileNotAvailableError(
                "Project directory exists, but file is missing. "
                "IDA filesystem might be in an inconsistent state."
            )
        else:
            raise FileNotAvailableError(
                "Project directory not available. "
                "IDA NFS mount might require reconfiguration "
                "to allow access to project."
            )

    # Write the stream to disk and calculate checksum
    tmp_path = path.parent / (path.name + '.tmp')
    hasher = CHECKSUM_ALGO_TO_CONSTRUCTOR[checksum_algo]()
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
    file_metadata: MetaxFile,
    path: Path,
    config_file: str = "/etc/siptools_research.conf",
) -> None:
    """Get file from IDA or upload-rest-api.

    :param file_metadata: File metadata from Metax
    :param path: Path where the file is downloaded
    :param config_file: Configuration file
    """
    conf = Configuration(config_file)
    file_storage = file_metadata["storage_service"]

    if file_storage == PAS_STORAGE_SERVICE:
        _link_local_file(
            upload_rest_api_path=Path(conf.get("upload_rest_api_path")),
            file_metadata=file_metadata,
            path=path,
        )
    else:
        _download_ida_file(
            ida_path=Path(conf.get("ida_path")),
            file_metadata=file_metadata,
            path=path,
        )
