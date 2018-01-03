"""Luigi task that creates technical metadata"""
# encoding=utf8

import os
import urllib
import lxml
from luigi import LocalTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.get_files import GetFiles
import siptools.scripts.import_object
from siptools.xml.mets import NAMESPACES


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


# pylint: disable=too-many-arguments
def create_premis_object(digital_object, formatname, creation_date,
                         hashalgorithm, hashvalue, format_version, workspace):
    """Calls import_object from siptools to create
    PREMIS file metadata.
    """
    siptools.scripts.import_object.main([digital_object,
                                         '--base_path', workspace,
                                         '--workspace', workspace,
                                         '--skip_inspection',
                                         '--format_name', formatname,
                                         '--digest_algorithm', hashalgorithm,
                                         '--message_digest', hashvalue,
                                         '--date_created', creation_date,
                                         '--format_version', format_version])


# pylint: disable=too-many-locals
def create_objects(file_id=None, metax_filepath=None, workspace=None,
                   config=None):
    """Gets file metadata from Metax and calls create_premis_object function"""

    metadata = Metax(config).get_data('files', file_id)
    hashalgorithm = metadata["checksum"]["algorithm"]
    hashvalue = metadata["checksum"]["value"]
    creation_date = metadata["file_characteristics"]["file_created"]
    formatname = metadata["file_format"]
    # formatversion hardcoded. Not in METAX yet. could be retrieved from file:
    #    formatname = formatdesignation(filepath, datatype='name')
    #    formatversion = formatdesignation(filepath, datatype='version')
    formatversion = "1.0"

    # Picks name of hashalgorithm from its length if it's not valid
    allowed_hashs = {128: 'MD5', 160: 'SHA-1', 224: 'SHA-224',
                     256: 'SHA-256', 384: 'SHA-384', 512: 'SHA-512'}
    hash_bit_length = len(hashvalue) * 4

    if hashalgorithm in allowed_hashs.values():
        hashalgorithm = hashalgorithm
    elif hash_bit_length in allowed_hashs:
        hashalgorithm = allowed_hashs[hash_bit_length]
    else:
        hashalgorithm = 'ERROR'

    # Create PREMIS file metadata XML
    create_premis_object(metax_filepath.strip('/'), formatname, creation_date,
                         hashalgorithm, hashvalue, formatversion,
                         os.path.join(workspace, 'sip-in-progress'))

    # Copy additional metadata XML files from Metax if they exist
    xml = Metax(config).get_xml('files', file_id)
    for ns_url in xml:
        if ns_url not in NAMESPACES.values():
            raise TypeError("Invalid XML namespace: %s" % ns_url)
        xml_data = xml[ns_url]
        ns_key = next((key for key, url in NAMESPACES.items() if url\
                       == ns_url), None)
        target_filename = urllib.quote_plus(metax_filepath + ns_key\
                                            + 'file.xml')
        output_file = os.path.join(workspace, 'sip-in-progress',
                                   target_filename)
        with open(output_file, 'w+') as outfile:
            # pylint: disable=no-member
            outfile.write(lxml.etree.tostring(xml_data))


def import_objects(dataset_id, workspace, config):
    """Main function of import_objects script """

    metax_client = Metax(config)
    file_metadata = metax_client.get_dataset_files(dataset_id)
    for file_ in file_metadata:

        # Read file identifier
        file_id = file_["identifier"]

        # Read file path from dataset file metadata
        file_metadata = metax_client.get_dataset_files
        metax_filepath = file_['file_path'].strip('/')
        create_objects(file_id, metax_filepath, workspace, config)
