# encoding=utf8
"""Luigi task that creates structure map."""

import os
import lxml.etree as ET
import mets
import luigi
from siptools.scripts import compile_structmap
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata


class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.

    :returns: dict
    """
    success_message = "Structure map created"
    failure_message = "Structure map could not be created"

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                CreateDescriptiveMetadata(workspace=self.workspace,
                                          dataset_id=self.dataset_id,
                                          config=self.config),
                "Create provenance information completed":
                CreateProvenanceInformation(workspace=self.workspace,
                                            dataset_id=self.dataset_id,
                                            config=self.config),
                "Create technical metadata completed":
                CreateTechnicalMetadata(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)}

    def output(self):
        """Writes log to ``logs/create-struct-map`` file

        :returns: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.logs_path,
                                              'create-struct-map'))

    def run(self):
        """Creates a METS structural map file with two distinct structural
        maps. Physical structural map is created based on a folder structure.
        Logical structural map is based on dataset metada retrieved from Metax.

        :returns: None

        """
        # Redirect stdout to logfile
        with self.output().open('w') as log:
            with redirect_stdout(log):

                # Create physical structmap using siptools script
                compile_structmap.main(['--workspace',
                                        self.sip_creation_path])

                # Read the generated physical structmap from file
                # pylint: disable=no-member
                structmap = ET.parse(os.path.join(self.sip_creation_path,
                                                  'structmap.xml'))

                # Add 'TYPE' attribute to physical structmap
                structmap.getroot()[0].attrib['TYPE'] \
                    = 'Fairdata-physical'

                # Get dmdsec id from physical_structmap
                dmdsec_id = structmap.getroot()[0][0].attrib['DMDID']

                # Init logical structmap
                logical_structmap = \
                    mets.structmap(type_attr='Fairdata-logical')

                # Create logical structmap
                categories = self.find_file_categories()
                wrapper_div = mets.div(type_attr='logical', dmdid=[dmdsec_id])
                for category in categories:
                    div = mets.div(type_attr=category, dmdid=[dmdsec_id])
                    for filename in categories.get(category):
                        fileid = self.get_fileid(filename)
                        div.append(mets.fptr(fileid))
                    wrapper_div.append(div)
                logical_structmap.append(wrapper_div)

                # Logical structmap element must be reparsed by lxml.etree to
                # make "pretty_printing" work
                logical_structmap = ET.fromstring(
                    ET.tostring(logical_structmap, pretty_print=True)
                )

                # Append the logical structmap into the element tree after
                # physical struct map
                structmap.getroot().append(logical_structmap)

                # Write new structmap to file
                structmap.write(os.path.join(self.sip_creation_path,
                                             'structmap.xml'),
                                encoding='UTF-8')

    def find_file_categories(self):
        """Creates logical structure map of dataset files. Returns dictionary
        with filecategories as keys and filepaths as values.

        :returns: dict
        """
        metax_client = Metax(self.config)
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_data('datasets', self.dataset_id)\
            ['research_dataset']
        locical_struct = dict()

        for dataset_file in dataset_files:

            file_id = dataset_file['identifier']

            # Get file's use category. The path to the file in logical
            # structmap is stored in 'use_category' in metax.
            filecategory = None
            if 'files' in dataset_metadata:
                for file_ in dataset_metadata['files']:
                    if file_id == file_['identifier']:
                        filecategory = file_['use_category']['pref_label']\
                            ['en']
                    break

            # If file listed in datasets/<id>/files is not listed in 'files'
            # section of dataset metadata, look for parent_directory of the
            # file from  'directories' section. The "use_category" of file is
            # the "use_category" of the parent directory.
            if filecategory is None:
                file_directory = dataset_file['parent_directory']['identifier']
                for directory in dataset_metadata['directories']:
                    if file_directory == directory['identifier']:
                        filecategory = directory['use_category']['pref_label']\
                                       ['en']
                        break

            # Append path to logical_struct[filecategory] list. Create list if
            # it does not exist already
            if filecategory not in locical_struct.keys():
                locical_struct[filecategory] = []
            locical_struct[filecategory].append(dataset_file['file_path'])

        return locical_struct


    def get_fileid(self, filename):
        """get file id from filesec by filename"""

        # pylint: disable=no-member
        filesec_xml = ET.parse(os.path.join(self.sip_creation_path,
                                            'filesec.xml'))

        fileid = None
        root = filesec_xml.getroot()

        files = root[0][0]
        for file_ in files:
            for file__ in file_:
                if str(file__.get('{http://www.w3.org/1999/xlink}href'))[7:] \
                        == filename:
                    print "file found %s " % file_.get('ID')
                    fileid = file_.get('ID')
                    break

        return fileid
