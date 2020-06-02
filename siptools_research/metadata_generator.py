"""Module that generates metadata required to create SIP, and posts it to
Metax.
"""
import os
import shutil
import tempfile

from requests.exceptions import HTTPError

from upload_rest_api.database import FilesCol
from file_scraper.scraper import Scraper
from metax_access import (Metax, DS_STATE_TECHNICAL_METADATA_GENERATED,
                          DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED,
                          MetaxError)
from siptools.scripts.import_object import (DEFAULT_VERSIONS,
                                            UNKNOWN_VERSION,
                                            NO_VERSION)

from siptools_research.utils.download import download_file, FileNotFoundError
from siptools_research.config import Configuration
from siptools_research.xml_metadata import (
    XMLMetadataGenerator, MetadataGenerationError
)

DEFAULT_PROVENANCE = {
    "preservation_event": {
        "identifier":
        "http://uri.suomi.fi/codelist/fairdata/preservation_event/code/cre",
        "pref_label": {
            "en": "creation"
        }
    },
    "description": {
        "en": "Value unavailable, possibly unknown"
    },
    "event_outcome": {
        "identifier":
        "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/unknown",
        "pref_label": {
            "en": "(:unav)"
        }
    },
    "outcome_description": {
        "en": "Value unavailable, possibly unknown"
    }
}


def generate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Generates

    - preservation identifier for dataset
    - provenance metadata if it does not exist already
    - techincal metadata for all dataset files
    - file format specific metadata for files

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

    # set default values
    status_code = DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED
    message = "Metadata generation failed"

    dataset = metax_client.get_dataset(dataset_id)
    try:

        # Generate default provenance metada if provenance list is empty or
        # does not exist at all
        research_dataset = dataset['research_dataset']
        if not research_dataset.get('provenance', []):
            metax_client.patch_dataset(
                dataset_id,
                {'research_dataset': {'provenance': [DEFAULT_PROVENANCE]}}
            )

        _generate_file_metadata(metax_client, dataset_id, tmpdir, config)

    except (MetadataGenerationError, MetaxError) as error:
        message = str(error)[:199] if len(str(error)) > 200 else str(error)
        status_code = DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED
        raise
    except HTTPError as error:
        message = "Internal error"
        status_code = DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED
        raise
    else:
        status_code = DS_STATE_TECHNICAL_METADATA_GENERATED
        message = 'Technical metadata generated'
    finally:
        shutil.rmtree(tmpdir)
        metax_client.set_preservation_state(dataset_id, state=status_code,
                                            system_description=message)


def _generate_file_metadata(metax_client, dataset_id, tmpdir, config_file):
    """Generates metadata for dataset files.

    :param metax_client: metax access
    :param dataset_id: identifier of dataset
    :param tmpdir: path to directory where files are downloaded to
    :param config_file: path to configuration file
    :returns: ``None``
    """
    upload_files = FilesCol()

    for file_ in metax_client.get_dataset_files(dataset_id):
        # Get file info
        file_id = file_['identifier']
        file_metadata = metax_client.get_file(file_id)

        # Download file to tmp directory
        tmpfile = os.path.join(tmpdir, file_id)
        try:
            download_file(file_metadata, tmpfile, config_file, upload_files)
        except FileNotFoundError as error:
            raise MetadataGenerationError(str(error), dataset=dataset_id)

        # Generate and update file_characteristics
        file_characteristics = _generate_file_characteristics(
            tmpfile, file_metadata.get('file_characteristics', {})
        )
        metax_client.patch_file(
            file_id,
            {'file_characteristics': file_characteristics}
        )
        file_metadata['file_characteristics'] = file_characteristics

        # Generate file format specific XML metadata
        generator = XMLMetadataGenerator(tmpfile, file_metadata)
        try:
            xml = generator.create()
        except MetadataGenerationError as error:
            raise MetadataGenerationError(
                str(error),
                dataset=dataset_id,
                file=os.path.split(tmpfile)[1]
            )
        if xml is not None:
            metax_client.set_xml(file_id, xml)


def _generate_file_characteristics(filepath, original_file_characteristics):
    """Reads file and generates technical metadata. `file_characteristics`
    object is read from original meta. Generated metadata is appended
    `file_characteristics` object. If a field already has a value (set by
    user) it will not be updated.

    :param filepath: path to file for which the metadata is generated
    :param original_file_characteristics: full original metadata dictionary
    :returns: New `file_characteristics` dictionary
    """
    mimetype = original_file_characteristics.get("file_format", None)
    charset = original_file_characteristics.get("encoding", None)
    version = original_file_characteristics.get("format_version", None)

    # Generate technical metadata from file
    scraper = Scraper(
        filepath, mimetype=mimetype, charset=charset, version=version
    )
    scraper.scrape(check_wellformed=False)

    # Create file_characteristics object
    file_characteristics = {}
    file_characteristics['file_format'] = scraper.mimetype

    if scraper.version != NO_VERSION:
        file_characteristics['format_version'] = scraper.version

        # Use default version if file scraper does not return version
        if (file_characteristics['format_version'] == UNKNOWN_VERSION
                and file_characteristics['file_format'] in DEFAULT_VERSIONS):
            file_characteristics['format_version'] \
                = DEFAULT_VERSIONS[file_characteristics['file_format']]

    if 'charset' in scraper.streams[0]:
        file_characteristics['encoding'] = scraper.streams[0]['charset']

    # Merge generated file_characteristics with original data from Metax.
    # If a field was already defined in original data, it will override the
    # generated value.
    file_characteristics.update(
        original_file_characteristics
    )

    del scraper

    return file_characteristics
