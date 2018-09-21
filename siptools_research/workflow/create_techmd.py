"""Luigi task that creates technical metadata"""
# encoding=utf8

import os
from luigi import LocalTarget
import siptools_research.utils.create_addml
from siptools_research.utils.contextmanager import redirect_stdout
from metax_access import Metax
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.get_files import GetFiles
from siptools_research.config import Configuration
import siptools.scripts.import_object
import siptools.utils


TECH_ATTR_TYPES = [
    {'mdtype': 'NISOIMG',
     'namespace': 'http://www.loc.gov/mix/v20',
     'mdtypeversion': '2.0',
     'othermdtype': None},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.arkivverket.no/standarder/addml',
     'mdtypeversion': 'ADDML',
     'othermdtype': '8.3'},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.loc.gov/audioMD/',
     'mdtypeversion': '2.0',
     'othermdtype': 'AudioMD'},
    {'mdtype': 'OTHER',
     'namespace': 'http://www.loc.gov/videoMD/',
     'mdtypeversion': '2.0',
     'othermdtype': 'VideoMD'}
]


class CreateTechnicalMetadata(WorkflowTask):
    """Create technical metadata files.
    """
    success_message = 'Technical metadata for objects created'
    failure_message = 'Technical metadata for objects could not be created'

    def requires(self):
        """Return required tasks.

        :returns: CreateWorkspace task
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
        """Outputs log to ``logs/task-create-technical-metadata.log``"

        :returns: LocalTarget"""
        return LocalTarget(os.path.join(self.logs_path,
                                        'task-create-technical-metadata.log'))

    def run(self):
        """Creates PREMIS technical metadata files for files in transfer.

        :returns: None
        """

        with self.output().open('w') as log:
            with redirect_stdout(log):
                import_objects(self.dataset_id, self.workspace, self.config)


def create_objects(file_id=None, metax_filepath=None, workspace=None,
                   config=None):
    """Gets file metadata from Metax and calls create_premis_object function"""

    config_object = Configuration(config)
    metadata = Metax(config_object.get('metax_url'),
                     config_object.get('metax_user'),
                     config_object.get('metax_password')).get_file(file_id)

    # Read character set if it defined for this file
    try:
        charset = metadata["file_characteristics"]["encoding"]
    except KeyError:
        charset = None

    # Read format version if it defined for this file
    try:
        formatversion = metadata["file_characteristics"]["format_version"]
    except KeyError:
        formatversion = ""

    # create ADDML if file format is text/csv
    if metadata["file_characteristics"]["file_format"] == 'text/csv':
        create_addml(workspace, metax_filepath, charset, metadata)

    # figure out the checksum algorithm
    digest_algorithm = algorithm_name(metadata["checksum"]["algorithm"],
                                      metadata["checksum"]["value"])

    # Read file created if it defined for this file
    try:
        file_created = metadata["file_characteristics"]["file_created"]
    except KeyError:
        file_created = None

    # Build parameter list for import_objects script
    import_object_parameters = [
        metax_filepath.strip('/'),
        '--base_path', os.path.join(workspace, 'sip-in-progress'),
        '--workspace', os.path.join(workspace, 'sip-in-progress'),
        '--skip_inspection',
        '--format_name', metadata["file_characteristics"]["file_format"],
        '--digest_algorithm', digest_algorithm,
        '--message_digest', metadata["checksum"]["value"],
        '--format_version', formatversion
    ]
    if charset is not None:
        import_object_parameters += ['--charset', charset]

    if file_created is not None:
        import_object_parameters += ['--date_created', file_created]

    # Create PREMIS file metadata XML
    siptools.scripts.import_object.main(import_object_parameters)

    # Create technical attributes XML files
    create_technical_attributes(config, workspace, file_id, metax_filepath)


def create_technical_attributes(config, workspace, file_id, filepath):
    """Read technical attribute XML from Metax. Create METS TechMD files for
    each metadata type, if it is available in Metax.
    """
    config_object = Configuration(config)
    xmls = Metax(config_object.get('metax_url'),
                 config_object.get('metax_user'),
                 config_object.get('metax_password')).get_xml('files', file_id)
    for type_ in TECH_ATTR_TYPES:
        if type_["namespace"] in xmls:

            # Create METS TechMD file
            techmd_id = siptools.utils.create_techmdfile(
                os.path.join(workspace, 'sip-in-progress'),
                xmls[type_['namespace']].getroot(),
                type_['mdtype'],
                type_['mdtypeversion'],
                type_['othermdtype']
            )

            # Add reference from fileSec to TechMD
            siptools.utils.add_techmdreference(
                os.path.join(workspace, 'sip-in-progress'),
                techmd_id,
                filepath
            )


def create_addml(workspace, metax_filepath, charset, metadata):
    """Creates addml metadata and writes it to file.

    :workspace: workspace directory where file is written
    :metax_filepath: path of CSV file
    :charset: CSV file charset
    :metadata: dict that contains basic information of CSV file
    :returns: None
    """
    csv_delimiter = metadata["file_characteristics"]["csv_delimiter"]
    csv_record_separator \
        = metadata["file_characteristics"]["csv_record_separator"]
    csv_quoting_char = metadata["file_characteristics"]["csv_quoting_char"]
    csv_isheader = metadata["file_characteristics"]["csv_has_header"]

    # Create addml metadata
    mdtype = 'ADDML'
    mdtypeversion = '8.3'
    mddata = siptools_research.utils.create_addml.create_addml(
        os.path.join(workspace, 'sip-in-progress'),
        metax_filepath.strip('/'),
        csv_delimiter,
        csv_isheader,
        charset,
        csv_record_separator,
        csv_quoting_char
    )

    # Write addml metadata to a file
    siptools_research.utils.create_addml.create_techmdfile(
        os.path.join(workspace, 'sip-in-progress'),
        mdtype,
        mdtypeversion,
        mddata,
        metax_filepath.strip('/')
    )


def import_objects(dataset_id, workspace, config):
    """Main function of import_objects script """
    config_object = Configuration(config)
    metax_client = Metax(config_object.get('metax_url'),
                         config_object.get('metax_user'),
                         config_object.get('metax_password'))
    file_metadata = metax_client.get_dataset_files(dataset_id)
    for file_ in file_metadata:

        # Read file identifier
        file_id = file_["identifier"]

        # Read file path from dataset file metadata
        file_metadata = metax_client.get_dataset_files
        metax_filepath = file_['file_path'].strip('/')
        create_objects(file_id, metax_filepath, workspace, config)


def algorithm_name(algorithm, value):
    """Guess the checksum algorithm. The name of checksum algorithm in Metax is
    either 'md5' or 'sha2'. If it is 'sha2' the exact algorithm has to be
    deduced from the lenght of checksum value.

    :returns: 'MD5', 'SHA-224', 'SHA-256', 'SHA-384', or 'SHA-512'
    """
    sha2_bit_lengths \
        = {224: 'SHA-224', 256: 'SHA-256', 384: 'SHA-384', 512: 'SHA-512'}
    hash_bit_length = len(value) * 4

    if algorithm == 'md5':
        algorithm_key = 'MD5'
    elif algorithm == 'sha2':
        algorithm_key = sha2_bit_lengths[hash_bit_length]

    return algorithm_key
