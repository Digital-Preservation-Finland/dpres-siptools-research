"""Module that generates metadata required to create SIP, and posts it to
Metax.
"""
import os
import shutil
import tempfile

from metax_access import Metax
from siptools.scripts import import_object
from siptools_research.utils import ida
from siptools_research.utils.database import Database
from siptools_research.config import Configuration
from siptools_research.xml_metadata import (
    XMLMetadataGenerator, MetadataGenerationError
)

TEMPDIR = "/var/spool/siptools_research/tmp"

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
    storage_id = config_object.get("pas_storage_id")
    metax_client = Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification')
    )
    tmpdir = tempfile.mkdtemp(
        prefix='generate_metadata-',
        dir=TEMPDIR
    )

    # Generate preservation_identifier
    metax_client.set_preservation_id(dataset_id)

    # Generate default provenance metada if provenance list is empty or does
    # not exist at all
    research_dataset = metax_client.get_dataset(dataset_id)['research_dataset']
    if not research_dataset.get('provenance', []):
        metax_client.patch_dataset(
            dataset_id,
            {'research_dataset': {'provenance': [DEFAULT_PROVENANCE]}}
        )

    try:
        for file_ in metax_client.get_dataset_files(dataset_id):

            # Get file info
            file_id = file_['identifier']
            file_metadata = metax_client.get_file(file_id)

            # Download file to tmp directory
            tmpfile = os.path.join(tmpdir, file_id)

            local = file_metadata["file_storage"]["identifier"] == storage_id
            if local:
                # Local file storage
                files_col = Database(config).client.upload.files
                file_path = files_col.find_one({"_id": file_id})["file_path"]
                os.link(file_path, tmpfile)
            else:
                # IDA
                try:
                    ida.download_file(file_id, tmpfile, config)
                except ida.IdaError as error:
                    message = ("File {} was not found in Ida."
                               .format(file_["file_path"]))
                    raise MetadataGenerationError(message, dataset=dataset_id)

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
    finally:
        shutil.rmtree(tmpdir)


def _generate_file_characteristics(filepath, original_file_characteristics):
    """Reads file and generates technical metadata. `file_characteristics`
    object is read from original meta. Generated metadata is appended
    `file_characteristics` object. If a field already has a value (set by
    user) it will not be updated.

    :param filepath: path to file for which the metadata is generated
    :param original_file_characteristics: full original metadata dictionary
    :returns: New `file_characteristics` dictionary
    """

    # Generate technical metadata from file
    metadata = import_object.metadata_info(filepath)

    # Create file_characteristics object
    file_characteristics = {}
    file_characteristics['file_format'] = metadata['format']['mimetype']
    file_characteristics['format_version'] = metadata['format']['version']
    if 'charset' in metadata['format'].keys():
        file_characteristics['encoding'] = metadata['format']['charset']

    # Merge generated file_characteristics with original data from Metax.
    # If a field was already defined in original data, it will override the
    # generated value.
        file_characteristics.update(
            original_file_characteristics
        )

    return file_characteristics
