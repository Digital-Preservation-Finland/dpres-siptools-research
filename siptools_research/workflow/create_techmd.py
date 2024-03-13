"""Luigi task that creates technical metadata."""

import datetime

import siptools.mdcreator
import siptools.scripts.import_object

from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.xml_metadata import (TECH_ATTR_TYPES,
                                            XMLMetadataGenerator)


class CreateTechnicalMetadata(WorkflowTask):

    @property
    def dataset_files_directory(self):
        """Directory where files have been downloaded to."""
        return self.dataset.metadata_generation_workspace / 'dataset_files'

    def run(self):
        """Create METS documents that contain technical metadata.

        The PREMIS object metadata is created to all dataset files and
        it is written to
        `<sip_creation_path>/<url_encoded_filepath>-PREMIS%3AOBJECT-amd.xml`.
        File properties are written to
        `<sip_creation_path>/<url_encoded_filepath>-scraper.json`.
        PREMIS event metadata and PREMIS agent metadata are written to
        `<sip_creation_path>/<premis_event_id>-PREMIS%3AEVENT-amd.xml`
        and
        `<sip_creation_path>/<premis_agent_id>-PREMIS%3AEVENT-amd.xml`.
        Import object PREMIS event metadata references are written to
        `<sip_creation_path>/import-object-md-references.jsonl`.

        The file format specific metadata is copied from metax if it is
        available. It is written to
        `<sip_creation_path>/<url_encoded_filepath>-<metadata_type>-amd.xml`,
        where <metadata_type> is NISOIMG, ADDML, AudioMD, or VideoMD.
        File format specific metadata references are written to a
        json-file depending on file format. For example, refences to
        NISOIMG metadata are written to
        `<sip_creation_path>/create-mix-md-references`.

        List of PREMIS event references is written to
        <sip_creation_path>/premis-event-md-references.jsonl.
        """
        files \
            = get_metax_client(self.config).get_dataset_files(self.dataset_id)

        # Create one timestamp for import_object events to avoid
        # creating new events each time import_object is iterated
        event_datetime \
            = datetime.datetime.now(datetime.timezone.utc).isoformat()

        for file_ in files:

            filepath = self.dataset_files_directory \
                / file_['file_path'].strip('/')

            # Create METS document that contains PREMIS metadata
            self.create_objects(file_, filepath, event_datetime,
                                self.dataset.sip_creation_path)

            # Create METS documents that contain technical
            # attributes
            self.create_technical_attributes(file_, filepath,
                                             self.dataset.sip_creation_path)

    def create_objects(self, metadata, filepath, event_datetime, output):
        """Create PREMIS metadata for file.

        Reads file metadata from Metax. Technical metadata is generated
        by siptools import_object script.

        :param metadata: file metadata dictionary
        :param filepath: file path
        :param event_datetime: the timestamp for the import_object
                               events
        :param output: output directory for import_object script
        :returns: ``None``
        """
        # Read character set if it defined for this file
        try:
            charset = metadata["file_characteristics"]["encoding"]
        except KeyError:
            charset = None

        # Read format version if it is defined for this file
        try:
            formatversion = metadata["file_characteristics"]["format_version"]
        except KeyError:
            formatversion = ""

        digest_algorithm = metadata["checksum"]["algorithm"]

        # figure out the checksum algorithm
        if digest_algorithm in ["md5", "sha2"]:
            digest_algorithm = algorithm_name(
                digest_algorithm, metadata["checksum"]["value"]
            )

        # Read file creation date if it is defined for this file
        try:
            date_created = metadata["file_characteristics"]["file_created"]
        except KeyError:
            date_created = None

        # Create PREMIS file metadata XML
        siptools.scripts.import_object.import_object(
            filepaths=[
                str(filepath.relative_to(self.dataset_files_directory.parent))
            ],
            base_path=self.dataset_files_directory.parent,
            workspace=output,
            skip_wellformed_check=True,
            file_format=(
                metadata["file_characteristics"]["file_format"],
                formatversion
            ),
            checksum=(digest_algorithm, metadata["checksum"]["value"]),
            charset=charset,
            date_created=date_created,
            event_datetime=event_datetime,
            event_target='.'
        )

    def create_technical_attributes(self, metadata, filepath, output):
        """Create technical metadata for a file.

        Create METS TechMD files for each metadata type based on
        previously scraped file characteristics.

        :param metadata: Metax metadata of the file
        :param filepath: path of file
        :param output: Path to the temporary workspace
        :returns: ``None``
        """
        mdcreator = siptools.mdcreator.MetsSectionCreator(output)
        metadata_generator = XMLMetadataGenerator(
            file_path=filepath,
            file_metadata=metadata
        )

        metadata_entries = metadata_generator.create()
        for md_entry in metadata_entries:

            md_elem = md_entry.md_elem
            md_namespace = md_elem.nsmap[md_elem.prefix]
            md_attributes = TECH_ATTR_TYPES[md_namespace]

            # Create METS TechMD file
            techmd_id, _ = mdcreator.write_md(
                metadata=md_elem,
                mdtype=md_attributes["mdtype"],
                mdtypeversion=md_attributes["mdtypeversion"],
                othermdtype=md_attributes.get("othermdtype", None)
            )

            # Add reference from fileSec to TechMD
            mdcreator.add_reference(
                techmd_id,
                str(filepath.relative_to(self.dataset_files_directory.parent)),
                stream=md_entry.stream_index
            )
            mdcreator.write(ref_file=md_attributes["ref_file"])


def algorithm_name(algorithm, value):
    """Guess the checksum algorithm.

    The name of checksum algorithm in Metax is either 'md5' or 'sha2'.
    If it is 'sha2' the exact algorithm has to be deduced from the
    length of checksum value.

    :param algorithm: algorithm string, 'md5' or 'sha2'
    :param value: the checksum value
    :returns: 'MD5', 'SHA-224', 'SHA-256', 'SHA-384', or 'SHA-512'
    """
    sha2_bit_lengths = {
        224: 'SHA-224', 256: 'SHA-256', 384: 'SHA-384', 512: 'SHA-512'
    }
    hash_bit_length = len(value) * 4

    if algorithm == 'md5':
        algorithm_key = 'MD5'
    elif algorithm == 'sha2':
        algorithm_key = sha2_bit_lengths[hash_bit_length]

    return algorithm_key
