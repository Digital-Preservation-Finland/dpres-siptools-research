"""Luigi task that creates technical metadata."""
# encoding=utf8

import os
import datetime
from luigi import LocalTarget
from metax_access import Metax
import siptools.scripts.import_object
import siptools.mdcreator
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.get_files import GetFiles


TECH_ATTR_TYPES = [
    {'mdtype': 'NISOIMG',
     'namespace': 'http://www.loc.gov/mix/v20',
     'mdtypeversion': '2.0',
     'othermdtype': None,
     'ref_file': 'create-mix-md-references.jsonl'},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.arkivverket.no/standarder/addml',
     'mdtypeversion': '8.3',
     'othermdtype': 'ADDML',
     'ref_file': 'create-addml-md-references.jsonl'},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.loc.gov/audioMD/',
     'mdtypeversion': '2.0',
     'othermdtype': 'AudioMD',
     'ref_file': 'create-audiomd-md-references.jsonl'},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.loc.gov/videoMD/',
     'mdtypeversion': '2.0',
     'othermdtype': 'VideoMD',
     'ref_file': 'create-videomd-md-references.jsonl'},
]


class CreateTechnicalMetadata(WorkflowTask):
    """Create METS documents that contain technical metadata.

    The PREMIS metadata is created to all dataset files and it is
    written to
    `<sip_creation_path>/<url_encoded_filepath>-premis-techmd.xml`.

    The file format specific metadata is copied from metax if it is
    available.  It is written to
    `<sip_creation_path>/<url_encoded_filepath>-<metadata_type>-techmd.xml`,
    where <metadata_type> is NISOIMG, ADDML, AudioMD, or VideoMD.

    Since the output files are not known beforehand, a false target
    `task-create-technical-metadata.finished` is created into workspace
    directory to notify luigi that this task has finished.

    The task requires workspace to be created, dataset metadata to be
    validated and dataset files to be downloaded.
    """

    success_message = 'Technical metadata for objects created'
    failure_message = 'Technical metadata for objects could not be created'

    def __init__(self, *args, **kwargs):
        """Initialize Task."""
        super(CreateTechnicalMetadata, self).__init__(*args, **kwargs)
        config_object = Configuration(self.config)
        self.metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of tasks: CreateWorkspace, ValidateMetadata, and
           GetFiles
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config),
                GetFiles(workspace=self.workspace,
                         dataset_id=self.dataset_id,
                         config=self.config)]

    def output(self):
        """Return output target of this Task.

        :returns: local target:
                  `task-create-technical-metadata.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace,
                'task-create-technical-metadata.finished'
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
        event_datetime = datetime.datetime.utcnow().isoformat()

        for file_ in files:
            # Create METS document that contains PREMIS metadata
            self.create_objects(file_, event_datetime)

            # Create METS documents that contain technical attributes
            self.create_technical_attributes(file_)

        with self.output().open('w') as output:
            output.write("Dataset id=" + self.dataset_id)

    def create_objects(self, metadata, event_datetime):
        """Create PREMIS metadata for file.

        Reads file metadata from Metax. Technical metadata is generated
        by siptools import_object script.

        :param metadata: file metadata dictionary
        :param event_datetime: the timestamp for the import_object
                               events
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
            filepaths=[metadata['file_path'].strip('/')],
            base_path=self.sip_creation_path,
            workspace=self.sip_creation_path,
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

    def create_technical_attributes(self, metadata):
        """Read technical attribute XML from Metax.

        Create METS TechMD files for each metadata type, if it is
        available in Metax.

        :param metadata: file metadata dictionary
        :returns: ``None``
        """
        file_id = metadata["identifier"]
        filepath = metadata['file_path'].strip('/')
        xmls = self.metax_client.get_xml(file_id)

        creator = siptools.mdcreator.MetsSectionCreator(self.sip_creation_path)

        for type_ in TECH_ATTR_TYPES:
            if type_["namespace"] in xmls:

                # Create METS TechMD file
                techmd_id, _ = creator.write_md(
                    xmls[type_['namespace']].getroot(),
                    type_['mdtype'],
                    type_['mdtypeversion'],
                    type_['othermdtype']
                )

                # Add reference from fileSec to TechMD
                creator.add_reference(techmd_id, filepath)
                creator.write(ref_file=type_["ref_file"])


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
