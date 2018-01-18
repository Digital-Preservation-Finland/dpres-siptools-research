# encoding=utf8
"""Luigi task that creates structure map."""

import os
import lxml.etree as ET
import mets
import xml_helpers.utils as h
import luigi
from siptools.scripts import compile_structmap
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools.xml.mets import NAMESPACES


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
                                        self.sip_creation_path,
                                       '--type_attr', 'Fairdata-physical'])
                # Read the generated physical structmap from file
                # pylint: disable=no-member
                structmap = ET.parse(os.path.join(self.sip_creation_path,
                                                  'structmap.xml'))

                # Get dmdsec id from physical_structmap
                dmdsec_id = structmap.getroot()[0][0].attrib['DMDID']

                # Get provenance id's
                provenance_ids = self.get_provenance_ids()

                # Init logical structmap
                logical_structmap = \
                    mets.structmap(type_attr='Fairdata-logical')

                mets_structmap = mets.mets(child_elements=[logical_structmap])

                # Create logical structmap
                categories = self.find_file_categories()
                wrapper_div = mets.div(type_attr='logical', dmdid=[dmdsec_id],
                        admid=provenance_ids)
                for category in categories:
                    div = mets.div(type_attr=category)
                    for filename in categories.get(category):
                        fileid = self.get_fileid(filename)
                        div.append(mets.fptr(fileid))
                    wrapper_div.append(div)
                logical_structmap.append(wrapper_div)

                with open(os.path.join(self.sip_creation_path, 'logical_structmap.xml'), 'w+') as outfile:
                    outfile.write(h.serialize(mets_structmap))


    def get_provenance_ids (self):
        metax_client = Metax(self.config)
        metadata = metax_client.get_data('datasets',
                self.dataset_id)
        provenance_ids = []
        for provenance in metadata["research_dataset"]["provenance"]:
            event_type = provenance["type"]["pref_label"]["en"]
            prov_file = '%s-event.xml' % event_type
            prov_xml = ET.parse(os.path.join(self.sip_creation_path,
                                                  prov_file))
            root = prov_xml.getroot()
            provenance_ids += root.xpath("//mets:digiprovMD/@ID",
                    namespaces=NAMESPACES)
        return provenance_ids


    def find_file_categories(self):
        """Creates logical structure map of dataset files. Returns dictionary
        with filecategories as keys and filepaths as values.

        :returns: dict
        """
        metax_client = Metax(self.config)
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_data('datasets', self.dataset_id)
        logical_struct = dict()

        for dataset_file in dataset_files:

            file_id = dataset_file['identifier']

            # Get the use category of file. The path to the file in logical
            # structmap is stored in 'use_category' in metax.
            filecategory = find_file_use_category(file_id, dataset_metadata)

            # If file listed in datasets/<id>/files is not listed in 'files'
            # section of dataset metadata, look for parent_directory of the
            # file from  'directories' section. The "use_category" of file is
            # the "use_category" of the parent directory.
            if filecategory is None:
                filecategory = find_dir_use_category(
                    dataset_file['parent_directory']['identifier'],
                    dataset_metadata
                )

            # Append path to logical_struct[filecategory] list. Create list if
            # it does not exist already
            if filecategory not in logical_struct.keys():
                logical_struct[filecategory] = []
            logical_struct[filecategory].append(dataset_file['file_path'])

        return logical_struct


    def get_fileid(self, filename):
        """get file id from filesec.xml by filename"""

        # pylint: disable=no-member
        filesec_xml = ET.parse(os.path.join(self.sip_creation_path,
                                            'filesec.xml'))

        root = filesec_xml.getroot()

        files = root[0][0]
        for file_ in files:
            for file__ in file_:
                if str(file__.get('{http://www.w3.org/1999/xlink}href'))[7:] \
                        == filename:
                    return file_.get('ID')


def find_file_use_category(identifier, dataset_metadata):
    """Looks for file with identifier from dataset metadata. Returns the
    "use_category" of file if it is found. If file is not found from list,
    return None.

    :identifier (string): File ID
    :dataset_metadata (dict): Dataset metadata from Metax
    :returns (string): Use category attribute
    """
    if 'files' in dataset_metadata['research_dataset']:
        for file_ in dataset_metadata['research_dataset']['files']:
            if file_['identifier'] == identifier:
                return file_['use_category']['pref_label']['en']

    # Nothing found
    return None


def find_dir_use_category(identifier, dataset_metadata):
    """Looks for file with identifier from dataset metadata. Returns the
    "use_category" of file if it is found. If file is not found from list,
    return None.
    """
    for directory in dataset_metadata['research_dataset']['directories']:
        if directory['identifier'] == identifier:
            return directory['use_category']['pref_label']['en']

    # Nothing found
    return None
