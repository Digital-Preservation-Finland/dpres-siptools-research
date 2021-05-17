"""Luigi task that creates technical metadata."""
# encoding=utf8

import datetime
import os
import shutil

import siptools.mdcreator
import siptools.scripts.import_object
from luigi import LocalTarget
from metax_access import Metax
from mets import NAMESPACES
from siptools_research.config import Configuration
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.get_files import GetFiles
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflowtask import WorkflowTask
from siptools_research.xml_metadata import (TECH_ATTR_TYPES,
                                            XMLMetadataGenerator)

try:
    from tempfile import TemporaryDirectory
except ImportError:
    # TODO: Remove this when Python 2 support can be dropped
    from siptools_research.temporarydirectory import TemporaryDirectory


class CreateTechnicalMetadata(WorkflowTask):
    """Create METS documents that contain technical metadata.

    The PREMIS object metadata is created to all dataset files and it is
    written to
    `<sip_creation_path>/<url_encoded_filepath>-PREMIS%3AOBJECT-amd.xml`.
    File properties are written to
    `<sip_creation_path>/<url_encoded_filepath>-scraper.json`.
    PREMIS event metadata and PREMIS agent metadata are written to
    `<sip_creation_path>/<premis_event_id>-PREMIS%3AEVENT-amd.xml` and
    `<sip_creation_path>/<premis_agent_id>-PREMIS%3AEVENT-amd.xml`.
    Import object PREMIS event metadata references are written to
    `<sip_creation_path>/import-object-md-references.jsonl`.

    The file format specific metadata is copied from metax if it is
    available. It is written to
    `<sip_creation_path>/<url_encoded_filepath>-<metadata_type>-amd.xml`,
    where <metadata_type> is NISOIMG, ADDML, AudioMD, or VideoMD.
    File format specific metadata references are written to a json-file
    depending on file format. For example, refences to NISOIMG metadata
    are written to `<sip_creation_path>/create-mix-md-references`.

    List of PREMIS event references is written to
    `<workspace>/create-technical-metadata.jsonl`

    The task requires workspace to be created, dataset metadata to be
    validated and dataset files to be downloaded.
    """

    success_message = 'Technical metadata for objects created'
    failure_message = 'Technical metadata for objects could not be created'

    def __init__(self, *args, **kwargs):
        """Initialize Task."""
        super(CreateTechnicalMetadata, self).__init__(*args, **kwargs)
        self.config_object = Configuration(self.config)
        self.metax_client = Metax(
            self.config_object.get('metax_url'),
            self.config_object.get('metax_user'),
            self.config_object.get('metax_password'),
            verify=self.config_object.getboolean('metax_ssl_verification')
        )

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: dictionary of required tasks
        """
        return {
            'workspace': CreateWorkspace(workspace=self.workspace,
                                         dataset_id=self.dataset_id,
                                         config=self.config),
            'validation': ValidateMetadata(workspace=self.workspace,
                                           dataset_id=self.dataset_id,
                                           config=self.config),
            'files': GetFiles(workspace=self.workspace,
                              dataset_id=self.dataset_id,
                              config=self.config)
        }

    def output(self):
        """Return output target of this Task.

        :returns: `<workspace>/create-technical-metadata.jsonl`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace,
                'create-technical-metadata.jsonl'
            )
        )

    def run(self):
        """Create techincal metadta.

        Creates PREMIS technical metadata files and technical attribute
        files.

        :returns: ``None``
        """
        files = self.metax_client.get_dataset_files(self.dataset_id)

        # Create one timestamp for import_object events to avoid
        # creating new events each time import_object is iterated
        event_datetime = datetime.datetime.now(datetime.timezone.utc).isoformat()

        tmp = os.path.join(self.config_object.get('packaging_root'), 'tmp/')
        with TemporaryDirectory(prefix=tmp) as temporary_workspace:
            for file_ in files:

                filepath = os.path.join('dataset_files',
                                        file_['file_path'].strip('/'))

                # Create METS document that contains PREMIS metadata
                self.create_objects(file_, filepath, event_datetime,
                                    temporary_workspace)

                # Create METS documents that contain technical
                # attributes
                self.create_technical_attributes(file_, filepath,
                                                 temporary_workspace)

            # Move created files to sip creation directory. PREMIS event
            # reference file is moved to output target path after
            # everything else is done.
            with self.output().temporary_path() as target_path:
                shutil.move(
                    os.path.join(temporary_workspace,
                                 'premis-event-md-references.jsonl'),
                    target_path
                )
                for file_ in os.listdir(temporary_workspace):
                    shutil.move(os.path.join(temporary_workspace, file_),
                                self.sip_creation_path)

    def create_objects(self, metadata, filepath, event_datetime, output):
        """Create PREMIS metadata for file.

        Reads file metadata from Metax. Technical metadata is generated
        by siptools import_object script.

        :param metadata: file metadata dictionary
        :param filepath: file path in SIP
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
            filepaths=[filepath],
            base_path=self.workspace,
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
        """Create technical metadata for a file

        Create METS TechMD files for each metadata type based on previously
        scraped file characteristics

        :param file_identifier: file identifier
        :param filepath: path of file in SIP
        :param output: Path to the temporary workspace
        :returns: ``None``
        """
        creator = siptools.mdcreator.MetsSectionCreator(output)
        metadata_generator = XMLMetadataGenerator(
            file_path=os.path.join(
                self.input()['files'].path,
                metadata['file_path'].strip('/')
            ),
            file_metadata=metadata
        )

        md_elems = metadata_generator.create()

        for md_elem in md_elems:
            # Retrieve the wrapped MD document
            md_namespace = md_elem.nsmap[md_elem.prefix]

            mdtype = TECH_ATTR_TYPES[md_namespace]["mdtype"]
            mdtypeversion = TECH_ATTR_TYPES[md_namespace]["mdtypeversion"]
            othermdtype = TECH_ATTR_TYPES[md_namespace].get("othermdtype", None)
            ref_file = TECH_ATTR_TYPES[md_namespace]["ref_file"]

            # Create METS TechMD file
            techmd_id, _ = creator.write_md(
                metadata=md_elem,
                mdtype=mdtype,
                mdtypeversion=mdtypeversion,
                othermdtype=othermdtype
            )

            # Add reference from fileSec to TechMD
            creator.add_reference(techmd_id, filepath)
            creator.write(ref_file=ref_file)


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
