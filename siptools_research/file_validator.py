"""File validation tools."""
import os

from file_scraper.scraper import Scraper
from metax_access.metax import Metax

import upload_rest_api.database

from siptools_research.config import Configuration
from siptools_research.exceptions import InvalidFileError
from siptools_research.exceptions import MissingFileError
from siptools_research.utils.download import (download_file,
                                              FileNotAvailableError)


def _download_files(metax_client, dataset_id, config_file, missing_files):
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
                file_metadata=dataset_file,
                dataset_id=dataset_id,
                config_file=config_file,
                upload_database=upload_database
            )
        except FileNotAvailableError:
            missing_files.append(dataset_file['identifier'])

    return dataset_files


def validate_files(dataset_id, config_file="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    Raises InvalidFileError or MissingFileError if files are invalid.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    invalid_files = []
    missing_files = []
    conf = Configuration(config_file)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    cache_path = os.path.join(conf.get("packaging_root"), "file_cache")

    dataset_files = _download_files(
        metax_client, dataset_id, config_file, missing_files
    )
    if missing_files:
        raise MissingFileError(
            f"{len(missing_files)} files are missing", missing_files
        )

    for dataset_file in dataset_files:
        _validate_file(dataset_file, cache_path, invalid_files)
    if invalid_files:
        raise InvalidFileError(
            f"{len(invalid_files)} files are not well-formed",
            invalid_files
        )

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
        errors.append(identifier)

    del scraper
