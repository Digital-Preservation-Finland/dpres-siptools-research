"""File validation tools."""
import os

from file_scraper.scraper import Scraper
from metax_access.metax import (Metax, DS_STATE_INVALID_METADATA,
                                DS_STATE_VALID_METADATA,
                                DS_STATE_METADATA_VALIDATION_FAILED)

from requests.exceptions import HTTPError

from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from siptools_research.utils.ida import download_file, IdaError


def _download_files(metax_client, dataset_id,
                    config_file="/etc/siptools_research.conf"):
    """Download all dataset files from IDA

    :param metax_client: metax access
    :param dataset_id: dataset identifier
    :param config_file: configuration file path
    :returns: A list of the metadata of all downloaded files
    """
    conf = Configuration(config_file)
    pas_storage_id = conf.get("pas_storage_id")

    dataset_files = metax_client.get_dataset_files(dataset_id)
    for dataset_file in dataset_files:
        identifier = dataset_file["identifier"]
        path = dataset_file["file_path"]
        if not dataset_file["file_storage"]["identifier"] == pas_storage_id:
            try:
                download_file(identifier, config_file=config_file)
            except (HTTPError, IdaError):
                raise FileAccessError(
                    "Could not download file '%s' from IDA" % path
                )
    return dataset_files


class FileValidationError(Exception):
    """Raised when file validation fails."""

    def __init__(self, message, paths=None):
        super(FileValidationError, self).__init__(message)
        self.paths = paths

    def __str__(self):
        message = super(FileValidationError, self).__str__()
        if self.paths:
            for path in self.paths:
                message += ("\n" + path)

        return message


class FileAccessError(Exception):
    """Raised when file cannot be accessed."""

    def __init__(self, message, paths=None):
        super(FileAccessError, self).__init__(message)
        self.paths = paths

    def __str__(self):
        message = super(FileAccessError, self).__str__()
        if self.paths:
            for path in self.paths:
                message += ("\n" + path)

        return message


def validate_files(dataset_id, config_file="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    # set default values
    message = "Metadata validation failed"
    success = False
    status_code = DS_STATE_METADATA_VALIDATION_FAILED
    errors = []
    try:
        conf = Configuration(config_file)
        storage_id = conf.get("pas_storage_id")
        metax_client = Metax(
            conf.get('metax_url'),
            conf.get('metax_user'),
            conf.get('metax_password'),
            verify=conf.getboolean('metax_ssl_verification')
        )
        mongo_files = Database(config_file).client.upload.files
        ida_path = os.path.join(conf.get("workspace_root"), "ida_files")

        dataset_files = _download_files(metax_client, dataset_id,
                                        config_file=config_file)
        for dataset_file in dataset_files:
            mongo_file = mongo_files.find_one(
                {"_id": dataset_file["identifier"]}
            )
            _validate_file(dataset_file, mongo_file, storage_id,
                           ida_path, errors)
        if errors:
            raise FileValidationError(
                "Following files are not well-formed:",
                errors
            )
    except FileValidationError as exc:
        status_code = DS_STATE_INVALID_METADATA
        message = str(exc)
        raise
    except FileAccessError as exc:
        message = str(exc)
        raise
    else:
        success = True
        status_code = DS_STATE_VALID_METADATA
        message = "Files passed validation"
    finally:
        message = message[:199] if len(message) > 200 else message
        metax_client.set_preservation_state(dataset_id,
                                            state=status_code,
                                            system_description=message)
    return success


def _validate_file(file_, mongo_file, storage_id, ida_path, errors):
    """validates file using Scraper

    :param file_: file metadata
    :param mongo_file: file data in mongo
    :param storage_id: pas storage id
    :param ida_path: ida path
    :param errors: array to store non-valid files
    :returns: None
    """
    identifier = file_["identifier"]
    path = file_["file_path"]
    file_chars = file_["file_characteristics"]
    mimetype = file_chars["file_format"]
    encoding = file_chars.get("encoding", None)

    if file_["file_storage"]["identifier"] == storage_id:
        if not mongo_file:
            raise FileAccessError(
                "File '%s' not found in pre-ingest file storage" % path
            )
        filepath = mongo_file["file_path"]
    else:
        filepath = os.path.join(ida_path, identifier)

    scraper = Scraper(filepath, mimetype=mimetype, charset=encoding)
    scraper.scrape(check_wellformed=True)
    if not scraper.well_formed:
        errors.append(path)

    del scraper
