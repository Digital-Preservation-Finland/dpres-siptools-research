"""Luigi task that creates logical structure map."""

import os

import luigi.format
from luigi import LocalTarget
import lxml.etree as ET

import mets
from metax_access import Metax
import xml_helpers.utils as h
from siptools.utils import encode_path, read_md_references, get_md_references
from siptools.xml.mets import NAMESPACES

from siptools_research.config import Configuration
from siptools_research.utils.locale import \
    get_dataset_languages, get_localized_value
from siptools_research.workflowtask import WorkflowTask
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools_research.workflow.create_digiprov \
    import CreateProvenanceInformation


class CreateLogicalStructMap(WorkflowTask):
    """Create METS document that contains logical structMap.

    File is written to `<sip_creation_path>/logical_structmap.xml`

    Task requires that physical structure map, fileSec and provenance
    information are created.
    """

    success_message = "Logical structure map created"
    failure_message = "Logical structure map could not be created"

    def requires(self):
        """List the the Tasks that this Task depends on.

        Provenance reference path from CreateProvenanceInformation is
        required.

        :returns: list of tasks
        """
        return {
            "structmap": CreateStructMap(workspace=self.workspace,
                                         dataset_id=self.dataset_id,
                                         config=self.config),
            "provenance": CreateProvenanceInformation(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            )
        }

    def output(self):
        """List the output targets of this Task.

        :returns: local target: `sip-in-progress/logical_structmap.xml`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(self.sip_creation_path, 'logical_structmap.xml'),
            format=luigi.format.Nop
        )

    def run(self):
        """Create a METS document that contains logical structural map.

        Logical structural map is based on dataset metada retrieved from
        Metax.

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
        logical_structmap = mets.structmap(type_attr='Fairdata-logical')
        mets_structmap = mets.mets(child_elements=[logical_structmap])

        # Create logical structmap
        categories = self.find_file_categories()
        wrapper_div = mets.div(type_attr='logical',
                               dmdid=[dmdsec_id],
                               admid=provenance_ids)
        for category in categories:
            div = mets.div(type_attr=category)
            for filename in categories.get(category):
                fileid = self.get_fileid(encode_path(filename, safe='/'))
                div.append(mets.fptr(fileid))
            wrapper_div.append(div)
        logical_structmap.append(wrapper_div)

        with self.output().open('wb') as output:
            output.write(h.serialize(mets_structmap))

    def get_provenance_ids(self):
        """List identifiers of provenance events.

        Gets list of dataset provenance events from Metax, and reads
        provenance IDs of the events from event.xml files found in the
        workspace directory.

        :returns: list of provenance IDs
        """
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        metadata = metax_client.get_dataset(self.dataset_id)
        languages = get_dataset_languages(metadata)

        # Get the reference file path from Luigi task input
        # It already contains the workspace path.
        event_ids = get_md_references(read_md_references(
            self.sip_creation_path, self.input()["provenance"].path
        ))

        event_type_ids = {}
        for event_id in event_ids:
            event_file = event_id[1:] + "-PREMIS%3AEVENT-amd.xml"
            event_file_path = os.path.join(
                self.sip_creation_path, event_file)
            if not os.path.exists(event_file_path):
                continue
            root = ET.parse(encode_path(event_file_path)).getroot()
            event_type = root.xpath("//premis:eventType",
                                    namespaces=NAMESPACES)[0].text
            event_type_ids[event_type] = event_id

        provenance_ids = []
        for provenance in metadata["research_dataset"]["provenance"]:
            event_type = get_localized_value(
                provenance["preservation_event"]["pref_label"],
                languages=languages
            )
            provenance_ids += [event_type_ids[event_type]]

        return provenance_ids

    def find_file_categories(self):
        """Create logical structure map of dataset files.

        Returns dictionary with filecategories as keys and filepaths as
        values.

        :returns: logical structure map dictionary
        """
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_dataset(self.dataset_id)
        languages = get_dataset_languages(dataset_metadata)
        dirpath2usecategory = get_dirpath_dict(metax_client, dataset_metadata)
        logical_struct = dict()

        for dataset_file in dataset_files:

            file_id = dataset_file['identifier']

            # Get the use category of file. The path to the file in
            # logical structmap is stored in 'use_category' in metax.
            filecategory = find_file_use_category(file_id, dataset_metadata)

            # If file listed in datasets/<id>/files is not listed in
            # 'files' section of dataset metadata, look for
            # parent_directory of the file from  'directories' section.
            # The "use_category" of file is the "use_category" of the
            # parent directory.
            if filecategory is None:
                name_len = len(dataset_file["file_name"])

                filecategory = find_dir_use_category(
                    dataset_file["file_path"][:-name_len],
                    dirpath2usecategory, languages
                )

            # If file category was not found even for the parent
            # directory, raise error
            if filecategory is None:
                raise InvalidDatasetMetadataError(
                    "File category for file {} was not found".format(file_id)
                )

            # Append path to logical_struct[filecategory] list. Create
            # list if it does not exist already
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

        raise ValueError(
            "File ID for file %s not found from fileSec: %s" % (
                filename, filesec_xml
            )
        )


def find_file_use_category(identifier, dataset_metadata):
    """Look for file with identifier from dataset metadata.

    Returns the `use_category` of file if it is found. If file is not
    found from list, return None.

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


def _match_paths(parent_path, dir_path):
    """Retun the depth to which the two paths match.

    Returns 0 if dir_path is deeper than parent_path since we don't want
    to consider directories, which are lower in the directory tree than
    the parent directory.
    """
    parent_path = parent_path[1:] if parent_path[0] == "/" else parent_path
    dir_path = dir_path[1:] if dir_path[0] == "/" else dir_path
    parent_list = parent_path.split("/")
    dir_list = dir_path.split("/")

    if len(dir_list) > len(parent_list):
        return 0

    matches = 0
    for i, _dir in enumerate(dir_list):
        if parent_list[i] == _dir:
            matches += 1
        else:
            break

    return matches


def get_dirpath_dict(metax_client, dataset_metadata):
    """Map directory paths to use categories.

    Returns a dict, which maps all research_dataset directory paths to
    the correcponding use_category values.

    :param metax_client: metax access
    :dataset_metadata: dataset metadata dictionary
    :returns: Dict {dirpath: use_category}
    """
    dirpath_dict = {}
    research_dataset = dataset_metadata["research_dataset"]

    if "directories" in research_dataset:
        for _dir in research_dataset["directories"]:
            use_category = _dir["use_category"]
            directory = metax_client.get_directory(_dir["identifier"])
            dirpath = directory["directory_path"]

            dirpath_dict[dirpath] = use_category

    return dirpath_dict


def find_dir_use_category(parent_path, dirpath2usecategory, languages):
    """Find use category of path.

    Find use_category of the closest parent directory listed in the
    research_dataset. This is done by checking how well the directory
    paths in the research_dataset match with the parent directory path.

    :param parent_path: path to the parent directory of the file
    :param dirpath2usecategory: Dictionary, which maps research_dataset
                                directory paths to the corresponding
                                use_categories.
    :param languages: A list of ISO 639-1 formatted language codes of
                      the dataset
    :returns: `use_category` attribute of directory
    """
    max_matches = 0
    use_category = None

    for dirpath in dirpath2usecategory:
        matches = _match_paths(parent_path, dirpath)

        if matches > max_matches:
            max_matches = matches
            use_category = dirpath2usecategory[dirpath]

    if use_category:
        return get_localized_value(
            use_category["pref_label"],
            languages=languages
        )

    return None
