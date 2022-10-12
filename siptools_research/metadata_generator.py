"""Generates metadata required to create SIP."""
import os
import shutil
import tempfile


import upload_rest_api.database
from file_scraper.scraper import Scraper
from metax_access import Metax
from siptools.scripts.import_object import (DEFAULT_VERSIONS,
                                            UNKNOWN_VERSION,
                                            NO_VERSION)

from siptools_research.exceptions import InvalidFileError
from siptools_research.exceptions import MissingFileError
from siptools_research.utils.download import (download_file,
                                              FileNotAvailableError)
from siptools_research.config import Configuration


def generate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Generate dataset metadata.

    Generates metadata required for creating SIP:
    - provenance metadata if it does not exist already
    - techincal metadata for all dataset files
    - file format specific metadata for files

    Raises InvalidDatasetError if metadata can not be generated due to
    missing/invalid files or metadata.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    config_object = Configuration(config)
    metax_client = Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification')
    )
    tmpdir = tempfile.mkdtemp(
        prefix='generate_metadata-',
        dir=os.path.join(config_object.get('packaging_root'), 'tmp')
    )

    try:
        _generate_file_metadata(metax_client, dataset_id, tmpdir, config)
    finally:
        shutil.rmtree(tmpdir)


def _generate_file_metadata(metax_client, dataset_id, tmpdir, config_file):
    """Generate metadata for dataset files.

    :param metax_client: metax access
    :param dataset_id: identifier of dataset
    :param tmpdir: path to directory where files are downloaded to
    :param config_file: path to configuration file
    :returns: ``None``
    """
    upload_database = upload_rest_api.database.Database()

    for file_ in metax_client.get_dataset_files(dataset_id):
        # Get file info
        file_id = file_['identifier']
        file_metadata = metax_client.get_file(file_id)

        # Download file to tmp directory
        tmpfile = os.path.join(tmpdir, file_id)
        try:
            download_file(
                file_metadata=file_metadata,
                dataset_id=dataset_id,
                linkpath=tmpfile,
                config_file=config_file,
                upload_database=upload_database
            )
        except FileNotAvailableError as error:
            raise MissingFileError("File is not available",
                                   [file_id]) from error

        # Generate and update file_characteristics
        tech_metadata = _generate_file_tech_metadata(
            tmpfile, file_metadata
        )

        if '(:unav)' in tech_metadata['file_characteristics'].values():
            raise InvalidFileError("File format was not recognized",
                                   [file_id])

        metax_client.patch_file(
            file_id,
            {
                'file_characteristics': tech_metadata['file_characteristics'],
                'file_characteristics_extension':
                tech_metadata['file_characteristics_extension']
            }
        )


def _generate_file_tech_metadata(filepath, original_file_metadata):
    """Read file and generates technical metadata.

    `file_characteristics` object is read from original meta. Generated
    metadata is appended `file_characteristics` object. If a field
    already has a value (set by user) it will not be updated.

    :param filepath: path to file for which the metadata is generated
    :param original_file_metadata: full original metadata dictionary
    :returns: New dictionary containing 'file_characteristics'
              and 'file_characteristics_extension' fields
    """
    # Generate technical metadata from file
    original_file_characteristics = original_file_metadata.get(
        "file_characteristics", {}
    )

    mimetype = original_file_characteristics.get("file_format", None)
    charset = original_file_characteristics.get("encoding", None)
    version = original_file_characteristics.get("format_version", None)
    scraper = Scraper(
        filepath, mimetype=mimetype, charset=charset, version=version
    )
    scraper.scrape(check_wellformed=False)

    # Create file_characteristics object
    file_characteristics = {
        'file_format': scraper.mimetype,
    }
    file_characteristics_extension = {
        'streams': scraper.streams
    }

    if scraper.version != NO_VERSION:
        file_characteristics['format_version'] = scraper.version

        # Use default version if file scraper does not return version
        if (file_characteristics['format_version'] == UNKNOWN_VERSION
                and file_characteristics['file_format'] in DEFAULT_VERSIONS):
            file_characteristics['format_version'] \
                = DEFAULT_VERSIONS[file_characteristics['file_format']]

    if 'charset' in scraper.streams[0]:
        file_characteristics['encoding'] = scraper.streams[0]['charset']

    # Merge generated file_characteristics with original data from
    # Metax. If a field was already defined in original data, it will
    # override the generated value.
    file_characteristics.update(original_file_characteristics)

    del scraper

    return {
        'file_characteristics': file_characteristics,
        'file_characteristics_extension': file_characteristics_extension
    }
