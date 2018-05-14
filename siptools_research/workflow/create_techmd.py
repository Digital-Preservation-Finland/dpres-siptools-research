"""Luigi task that creates technical metadata"""
# encoding=utf8

import os
import urllib
import lxml.etree as ET
import xml_helpers.utils as h
from luigi import LocalTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.workflowtask import WorkflowTask, InvalidMetadataError
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.get_files import GetFiles
import siptools.scripts.import_object
from siptools.xml.mets import NAMESPACES
from siptools_research.utils.create_addml import create_addml, create_techmdfile
from siptools.utils import encode_path

ALLOWED_HASHS = {128: 'MD5', 160: 'SHA-1', 224: 'SHA-224',
                 256: 'SHA-256', 384: 'SHA-384', 512: 'SHA-512'}

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


# pylint: disable=too-many-locals
def create_objects(file_id=None, metax_filepath=None, workspace=None,
                   config=None):
    """Gets file metadata from Metax and calls create_premis_object function"""

    metadata = Metax(config).get_file(file_id)
    hashalgorithm = metadata["checksum"]["algorithm"]
    hashvalue = metadata["checksum"]["value"]
    creation_date = metadata["file_characteristics"]["file_created"]

    # Read character set if it defined for this file
    try:
        charset = metadata["file_characteristics"]["encoding"]
    except KeyError:
        charset = None

    if charset is not None:
        # TODO: Should we allow sligthly different spelling of charsets names
        # than those allowed in PAS? What are allowed charset names in Metax?
        allowed_charsets = ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']
        # convert charset string to uppercase
        charset = charset.upper()
        if charset not in allowed_charsets:
            raise InvalidMetadataError(
                'Character set: %s is not allowed. \
                The allowed character sets are %s and %s.',
                charset,
                ", ".join(allowed_charsets[:-1]),
                allowed_charsets[-1]
            )

    formatname = metadata["file_characteristics"]["file_format"]
    #formatversion = metadata["file_characteristics"]["formatversion"]
    # Read format version if it defined for this file
    try:
        formatversion = metadata["file_characteristics"]["formatversion"]
    except KeyError:
        formatversion = None

    # Picks name of hashalgorithm from its length if it's not valid
    hash_bit_length = len(hashvalue) * 4

    if hashalgorithm in ALLOWED_HASHS.values():
        hashalgorithm = hashalgorithm
    elif hash_bit_length in ALLOWED_HASHS:
        hashalgorithm = ALLOWED_HASHS[hash_bit_length]
    else:
        raise InvalidMetadataError(
            'Invalid checksum data (algorithm: %s, value: %s) for file: %s' %
            (hashalgorithm, hashvalue, file_id))

    # create ADDML if formatname = 'text/csv
    if formatname == 'text/csv':
        #csv_file_path = os.path.join (file_path,
        #        file_name).strip('/')

        # hardcoded. Not in METAX yet
        #csv_delimiter = ";"
        #csv_record_separator = "CR+LF"
        #csv_quoting_char = '"'
        #csv_isheader = 'False'
        csv_delimiter = metadata["file_characteristics"]["csv_delimiter"]
        csv_record_separator = metadata["file_characteristics"]["csv_record_separator"]
        csv_quoting_char = metadata["file_characteristics"]["csv_quoting_char"]
        csv_isheader = metadata["file_characteristics"]["csv_isheader"]

        # Metadata type and version
        mdtype = 'ADDML'
        mdtypeversion = '8.3'
        mddata = create_addml(os.path.join(workspace, 'sip-in-progress'),
                              metax_filepath.strip('/'), csv_delimiter,
                              csv_isheader, charset, csv_record_separator,
                              csv_quoting_char)

        create_techmdfile(os.path.join(workspace, 'sip-in-progress'), mdtype,
                          mdtypeversion, mddata, metax_filepath.strip('/'))

    # Build parameter list for import_objects script
    import_object_parameters = [
        metax_filepath.strip('/'),
        '--base_path', os.path.join(workspace, 'sip-in-progress'),
        '--workspace', os.path.join(workspace, 'sip-in-progress'),
        '--skip_inspection',
        '--format_name', formatname,
        '--digest_algorithm', hashalgorithm,
        '--message_digest', hashvalue,
        '--date_created', creation_date,
        '--format_version', formatversion
    ]
    if charset is not None:
        import_object_parameters += ['--charset', charset]

    # Create PREMIS file metadata XML
    siptools.scripts.import_object.main(import_object_parameters)

    # tempfile to pair metadata files
    tempfile_root = ET.Element('contents')

    # Copy additional metadata XML files from Metax if they exist
    xml = Metax(config).get_xml('files', file_id)
    for ns_url in xml:
        if ns_url not in NAMESPACES.values():
            raise TypeError("Invalid XML namespace: %s" % ns_url)
        xml_data = xml[ns_url]
        ns_key = next((key for key, url in NAMESPACES.items() if url\
                       == ns_url), None)
        #add_to_tempfile(tempfile_root, metax_filepath, metax_filepath, '')
        target_filename = urllib.quote_plus(metax_filepath + ns_key\
                                            + '-othermd.xml')
        output_file = os.path.join(workspace, 'sip-in-progress',
                                   target_filename)
        with open(output_file, 'w+') as outfile:
            # pylint: disable=no-member
            #outfile.write(ET.tostring(xml_data))
            outfile.write(h.serialize(xml_data))

        techmd_id = xml_data.xpath("/mets:mets/mets:amdSec/mets:techMD/@ID", namespaces=NAMESPACES)
        fileid = ET.Element('fileid')
        tempfile_root.append(fileid)
        fileid.text = techmd_id[0]
        fileid.set('path', metax_filepath)

        tempfilename = encode_path(ns_key, suffix='file.xml')
        with open(os.path.join(workspace, 'sip-in-progress', tempfilename), 'w+') as outfile:
            outfile.write(h.serialize(tempfile_root))
            print "Wrote md pairings to tempfile %s" % outfile.name


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
