"""Luigi task that creates logical structure map."""

import os

from luigi import LocalTarget
import lxml.etree as ET
import mets
from metax_access import Metax
import xml_helpers.utils as h
from siptools.utils import encode_path
from siptools.xml.mets import NAMESPACES
from siptools_research.config import Configuration
from siptools_research.utils.locale import \
    get_dataset_languages, get_localized_value
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools_research.workflow.create_digiprov \
    import CreateProvenanceInformation


class CreateLogicalStructMap(WorkflowTask):
    """Create METS document that contains logical structMap. File is written to
    `<sip_creation_path>/logical_structmap.xml`

    Task requires that physical structure map, fileSec and provenance
    information are created.
    """
    success_message = "Logical structure map created"
    failure_message = "Logical structure map could not be created"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of tasks
        """
        return [
            CreateStructMap(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config),
            CreateProvenanceInformation(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)
        ]

    def output(self):
        """The output that this Task produces.

        :returns: local target: ``sip-in-progress/logical_structmap.xml`` ,
        :rtype: LocalTarget
        """
        return LocalTarget(os.path.join(self.sip_creation_path,
                                        'logical_structmap.xml'))

    def run(self):
        """Creates a METS document that contains logical structural map.
        Logical structural map is based on dataset metada retrieved from Metax.

        :returns: ``None``
        """

        # Read the generated physical structmap from file
        # pylint: disable=no-member
        physical_structmap = ET.parse(os.path.join(self.sip_creation_path,
                                                   'structmap.xml'))

        # Get dmdsec id from physical_structmap
        dmdsec_id = physical_structmap.getroot()[0][0].attrib['DMDID']

        # Get provenance id's
        provenance_ids = self.get_provenance_ids()

        # Init logical structmap
        logical_structmap \
            = mets.structmap(type_attr='Fairdata-logical')
        mets_structmap = mets.mets(child_elements=[logical_structmap])

        # Create logical structmap
        categories = self.find_file_categories()
        wrapper_div = mets.div(type_attr='logical',
                               dmdid=[dmdsec_id],
                               admid=provenance_ids)
        for category in categories:
            div = mets.div(type_attr=category)
            for filename in categories.get(category):
                fileid = self.get_fileid(encode_path(filename,
                                                     safe='/'))
                div.append(mets.fptr(fileid))
            wrapper_div.append(div)
        logical_structmap.append(wrapper_div)

        with self.output().open('w') as output:
            output.write(h.serialize(mets_structmap))

    def get_provenance_ids(self):
        """Gets list of dataset provenance events from Metax, and reads
        provenance IDs of the events from event.xml files found in the
        workspace directory.

        :returns: list of provenance IDs
        """
        config_object = Configuration(self.config)
        metax_client = Metax(config_object.get('metax_url'),
                             config_object.get('metax_user'),
                             config_object.get('metax_password'))
        metadata = metax_client.get_dataset(self.dataset_id)
        languages = get_dataset_languages(metadata)

        if 'provenance' not in metadata['research_dataset']:
            return []

        provenance_ids = []
        for provenance in metadata["research_dataset"]["provenance"]:
            event_type = get_localized_value(
                provenance["preservation_event"]["pref_label"],
                languages=languages
            )
            prov_file = '%s-event.xml' % event_type
            prov_file = encode_path(os.path.join(self.sip_creation_path,
                                                 prov_file))
            prov_xml = ET.parse(prov_file)
            root = prov_xml.getroot()
            provenance_ids += root.xpath("//mets:digiprovMD/@ID",
                                         namespaces=NAMESPACES)
        return provenance_ids

    def find_file_categories(self):
        """Creates logical structure map of dataset files. Returns dictionary
        with filecategories as keys and filepaths as values.

        :returns: logical structure map dictionary
        """
        config_object = Configuration(self.config)
        metax_client = Metax(config_object.get('metax_url'),
                             config_object.get('metax_user'),
                             config_object.get('metax_password'))
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_dataset(self.dataset_id)
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
        """Get file id from filesec.xml by filename.

        :param filename: filename
        :returns: file identifier
        """

        # pylint: disable=no-member
        filesec_xml = ET.parse(os.path.join(self.sip_creation_path,
                                            'filesec.xml'))

        root = filesec_xml.getroot()

        files = root[0][0]
        for file_ in files:
            for file__ in file_:
                if str(file__.get('{http://www.w3.org/1999/xlink}href'))[7:] \
                        == filename.strip('/'):
                    return file_.get('ID')

        raise Exception("File ID for file %s not found from fileSec: %s" %
                        (filename, filesec_xml))


def find_file_use_category(identifier, dataset_metadata):
    """Looks for file with identifier from dataset metadata. Returns the
    `use_category` of file if it is found. If file is not found from list,
    return None.

    :param identifier: file identifier
    :param dataset_metadata: dataset metadata dictionary
    :returns: `use_category` attribute of file
    """
    languages = get_dataset_languages(dataset_metadata)

    if 'files' in dataset_metadata['research_dataset']:
        for file_ in dataset_metadata['research_dataset']['files']:
            if file_['identifier'] == identifier:
                return get_localized_value(
                    file_['use_category']['pref_label'],
                    languages=languages)

    # Nothing found
    return None


def find_dir_use_category(identifier, dataset_metadata):
    """Looks for directory with identifier from dataset metadata. Returns the
    `use_category` of directory if it is found. If directory is not found from
    list, return ``None``.

    :param identifier: directory identifier
    :param dataset_metadata: dataset metadata dictionary
    :returns: `use_category` attribute of directory
    """
    languages = get_dataset_languages(dataset_metadata)

    for directory in dataset_metadata['research_dataset']['directories']:
        if directory['identifier'] == identifier:
            return get_localized_value(
                directory['use_category']['pref_label'],
                languages=languages
            )

    # Nothing found
    return None
