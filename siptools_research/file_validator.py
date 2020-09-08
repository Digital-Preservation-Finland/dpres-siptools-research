"""File validation tools."""
import os

from file_scraper.scraper import Scraper
from metax_access.metax import (Metax, DS_STATE_VALIDATING_METADATA,
                                DS_STATE_INVALID_METADATA,
                                DS_STATE_VALID_METADATA,
                                DS_STATE_METADATA_VALIDATION_FAILED)

from requests.exceptions import HTTPError

import upload_rest_api.database

from siptools_research.config import Configuration
from siptools_research.exceptions import InvalidFileError
from siptools_research.utils.download import (
    download_file, FileNotAvailableError, FileAccessError
)


def _download_files(
        metax_client,
        dataset_id,
        config_file="/etc/siptools_research.conf"
):
    """Download all dataset files.

    :param metax_client: metax access
    :param dataset_id: dataset identifier
    :param config_file: configuration file path
    :returns: A list of the metadata of all downloaded files
    """
    upload_database = upload_rest_api.database.Database()
    dataset_files = metax_client.get_dataset_files(dataset_id)
    for dataset_file in dataset_files:
        try:
            download_file(
                dataset_file,
                config_file=config_file,
                upload_database=upload_database
            )
        except (HTTPError, FileNotAvailableError):
            raise FileAccessError(
                "Could not download file '%s'" % dataset_file["file_path"]
            )

    return dataset_files


def validate_files(dataset_id, config_file="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    # set default values
    message = "Files passed validation"
    status_code = DS_STATE_VALID_METADATA

    errors = []
    conf = Configuration(config_file)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    cache_path = os.path.join(conf.get("packaging_root"), "file_cache")

    try:
        metax_client.set_preservation_state(
            dataset_id,
            state=DS_STATE_VALIDATING_METADATA
        )
        dataset_files = _download_files(
            metax_client,
            dataset_id,
            config_file=config_file
        )
        for dataset_file in dataset_files:
            _validate_file(dataset_file, cache_path, errors)
        if errors:
            raise InvalidFileError(
                "Some files are not well-formed.",
                errors
            )
    except InvalidFileError as exception:
        status_code = DS_STATE_INVALID_METADATA
        message = "Following files are not well-formed:\n{}".format(
            "\n".join(exception.files)
        )
        raise
    except FileAccessError as exception:
        status_code = DS_STATE_METADATA_VALIDATION_FAILED
        message = str(exception)
        raise
    finally:
        message = message[:199] if len(message) > 200 else message
        metax_client.set_preservation_state(dataset_id,
                                            state=status_code,
                                            system_description=message)

    return True


def _validate_file(file_, cache_path, errors):
    """Validate file using file-scraper.

    :param file_: file metadata
    :param mongo_file: file data in mongo
    :param cache_path: Path to the file_cache
    :param errors: array to store non-valid files
    :returns: None
    """
    identifier = file_["identifier"]
    path = file_["file_path"]
    file_chars = file_["file_characteristics"]
    mimetype = file_chars["file_format"]
    encoding = file_chars.get("encoding", None)
    version = file_chars.get("format_version", None)

    filepath = os.path.join(cache_path, identifier)

    scraper = Scraper(
        filepath, mimetype=mimetype, charset=encoding, version=version
    )
    scraper.scrape(check_wellformed=True)
    if not scraper.well_formed:
        errors.append(path)

    del scraper
