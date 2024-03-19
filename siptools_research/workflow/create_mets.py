"""Luigi task that creates METS document."""
import datetime
import shutil
import os

import luigi.format
from luigi import LocalTarget
import lxml.etree as ET
import mets
import siptools.mdcreator
from siptools.utils import encode_path, read_md_references, get_md_references
from siptools.scripts import (compile_mets, premis_event, import_description,
                              compile_structmap)
import siptools.scripts.import_object
from siptools.xml.mets import NAMESPACES
import xml_helpers.utils as h

from siptools_research.utils.locale import (
    get_dataset_languages, get_localized_value
)
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.copy_dataset_to_pas_data_catalog\
    import CopyToPasDataCatalog
from siptools_research.workflow.get_files import GetFiles
from siptools_research.xml_metadata import (TECH_ATTR_TYPES,
                                            XMLMetadataGenerator)


class CreateMets(WorkflowTask):
    """Creates the METS document.

    Requires that dataset is copied to PAS data catalog, as the DOI of
    the PAS version of the dataset will be included in METS.

    Writes mets.xml to preservation workspace.
    """

    success_message = "METS document created"
    failure_message = "Creating METS document failed"

    def requires(self):
        # TODO: Currently this task will download metadata from Metax
        # while METS document is build using old siptools. The plan is
        # to move metadata download to separate Task, which is required
        # by CreateMets task.
        return [
            GetFiles(
                dataset_id=self.dataset_id, config=self.config
            ),
            CopyToPasDataCatalog(
                dataset_id=self.dataset_id, config=self.config
            ),
        ]

    def output(self):
        return LocalTarget(
            str(self.dataset.preservation_workspace / 'mets.xml'),
            format=luigi.format.Nop
        )

    def run(self):
        # TODO: Rewrite this whole function using siptools-ng/mets-builder.

        # These methods are the run methods of old METS part creation
        # tasks. They were copied to this task without changes, which is
        # stupid, but this way we don't have to try to understand what
        # each these tasks does, and why. All of them will be removed
        # when we migrate to siptools-ng anyway.
        self.create_provenance_information()
        self.create_descriptive_metadata()
        self.create_technical_metadata()
        self.create_struct_map()
        self.create_logical_struct_map()

        # Get dataset metadata from Metax
        metax_client = get_metax_client(self.config)
        metadata = metax_client.get_dataset(self.dataset_id)

        # Get contract data from Metax
        contract_id = metadata["contract"]["identifier"]
        contract_metadata = metax_client.get_contract(contract_id)
        contract_identifier = contract_metadata["contract_json"]["identifier"]
        contract_org_name \
            = contract_metadata["contract_json"]["organization"]["name"]

        # Compile METS
        mets = compile_mets.create_mets(
            workspace=str(self.dataset.sip_creation_path),
            mets_profile='tpas',
            contractid=contract_identifier,
            objid=self.dataset.sip_identifier,
            contentid=self.dataset.sip_identifier,
            organization_name=contract_org_name,
            packagingservice='Packaging Service'
        )

        with self.output().open('wb') as outputfile:
            mets.write(outputfile,
                       pretty_print=True,
                       xml_declaration=True,
                       encoding='UTF-8')

    def create_provenance_information(self):
        """Create provenance information.

        Creates METS documents that contain PREMIS event element for
        each provenance event. Each document contains a digiprovMD
        element identified by hash generated from the document filename.
        The METS documents are written to
        `<sip_creation_path>/<uuid>-PREMIS%3AEVENT-amd.xml`. Premis
        event reference is written to
        <sip_creation_path>/premis-event-md-references.jsonl.

        The premis-event-md-references.jsonl file is copied to
        `<workspace>/preservation/create-provenance-information.jsonl`,
        because logical structuremap creation needs a reference file
        that contains only provenance events. Other events created by
        siptools-scripts would break the logical structuremap.
        """
        metadata = get_metax_client(self.config).get_dataset(self.dataset_id)
        dataset_languages = get_dataset_languages(metadata)
        provenances = metadata["research_dataset"].get("provenance", [])

        for provenance in provenances:

            # Although it shouldn't happen, theoretically both
            # 'preservation_event' and 'lifecycle_event' could exist in
            # the same provenance metadata. 'preservation_event' is used
            # as the overriding value if both exist.
            if "preservation_event" in provenance:
                event_type = get_localized_value(
                    provenance["preservation_event"]["pref_label"],
                    languages=dataset_languages
                )
            elif "lifecycle_event" in provenance:
                event_type = get_localized_value(
                    provenance["lifecycle_event"]["pref_label"],
                    languages=dataset_languages
                )
            else:
                raise InvalidDatasetMetadataError(
                    "Provenance metadata does not have key "
                    "'preservation_event' or 'lifecycle_event'. "
                    f"Invalid provenance: {provenance}"
                )

            try:
                event_datetime = provenance["temporal"]["start_date"]
            except KeyError:
                event_datetime = 'OPEN'

            # Add provenance title and description to eventDetail
            # element text, if present. If both are present, format as
            # "title: description". Our JSON schema validates that at
            # least one is present.
            event_detail_items = []
            if "title" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["title"],
                        languages=dataset_languages
                    )
                )
            if "description" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["description"],
                        languages=dataset_languages
                    )
                )
            event_detail = ": ".join(event_detail_items)

            if "event_outcome" in provenance:
                event_outcome = get_localized_value(
                    provenance["event_outcome"]["pref_label"],
                    languages=dataset_languages
                )
            else:
                event_outcome = "unknown"

            event_outcome_detail = provenance.get("outcome_description", None)
            if event_outcome_detail is not None:
                event_outcome_detail = get_localized_value(
                    provenance["outcome_description"],
                    languages=dataset_languages
                )

            premis_event.premis_event(
                workspace=self.dataset.sip_creation_path,
                event_type=event_type,
                event_datetime=event_datetime, event_detail=event_detail,
                event_outcome=event_outcome,
                event_outcome_detail=event_outcome_detail
            )

        # Create a copy of premis-event-md-references.jsonl before other
        # siptools-scripts will pollute it. It is needed for logical
        # structuremap creation.
        if provenances:
            shutil.copy(
                self.dataset.sip_creation_path
                / 'premis-event-md-references.jsonl',
                self.dataset.preservation_workspace
                / 'create-provenance-information.jsonl'
            )

    def create_descriptive_metadata(self):
        """Create METS dmdSec document.

        Descriptive metadata is read from Metax in DataCite format.
        Descriptive metadata is written to <sip_creation_path>/dmdsec.xml.
        Descriptive metadata references are written to
        <sip_creation_path>/import-description-md-references.jsonl.
        Premis event is written to
        <sip_creation_path>/<event_identifier>-PREMIS%3AEVENT-amd.xml.
        Premis event reference is written to
        <sip_creation_path>/premis-event-md-references.jsonl.
        """
        # Get datacite.xml from Metax
        metax_client = get_metax_client(self.config)
        dataset = metax_client.get_dataset(self.dataset_id)
        datacite = metax_client.get_datacite(dataset['identifier'])

        # Write datacite.xml to file
        datacite_path \
            = str(self.dataset.preservation_workspace / 'datacite.xml')
        datacite.write(datacite_path)

        # Create output files with siptools
        import_description.import_description(
            dmdsec_location=datacite_path,
            workspace=self.dataset.sip_creation_path,
            without_uuid=True
        )

    @property
    def dataset_files_directory(self):
        """Directory where files have been downloaded to."""
        return self.dataset.metadata_generation_workspace / 'dataset_files'

    def create_technical_metadata(self):
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

    def create_struct_map(self):
        """Creates structural map and file section.

        Structural map and file section are written to separate METS
        documents: `<sip_creation_path>/structmap.xml` and
        `<sip_creation_path>/filesec.xml`
        """
        compile_structmap.compile_structmap(
            workspace=self.dataset.sip_creation_path,
            structmap_type='Fairdata-physical',
            stdout=False
        )

    def create_logical_struct_map(self):
        """Create METS document that contains logical structMap.

        The file is written to
        `<sip_creation_path>/logical_structmap.xml`.
        """
        # Read the generated physical structmap from file
        # TODO: The path is converted to string because old versions of
        # lxml do not support pathlib Paths. The conversion can
        # probably be removed when Centos7 support is not required..
        physical_structmap \
            = ET.parse(str(self.dataset.sip_creation_path / 'structmap.xml'))

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

        output_path = self.dataset.sip_creation_path / 'logical_structmap.xml'
        with output_path.open('wb') as output:
            output.write(h.serialize(mets_structmap))

    def get_provenance_ids(self):
        """List identifiers of provenance events.

        Gets list of dataset provenance events from Metax, and reads
        provenance IDs of the events from event.xml files found in the
        workspace directory.

        :returns: list of provenance IDs
        """
        metadata = get_metax_client(self.config).get_dataset(self.dataset_id)
        languages = get_dataset_languages(metadata)

        # Read the reference file
        event_ids = get_md_references(read_md_references(
            str(self.dataset.preservation_workspace),
            'create-provenance-information.jsonl'
        ))
        if not event_ids:
            event_ids = []

        event_type_ids = {}
        for event_id in event_ids:
            event_file = event_id[1:] + "-PREMIS%3AEVENT-amd.xml"
            event_file_path = self.dataset.sip_creation_path / event_file
            if not os.path.exists(event_file_path):
                continue
            root = ET.parse(encode_path(str(event_file_path))).getroot()
            event_type = root.xpath("//premis:eventType",
                                    namespaces=NAMESPACES)[0].text
            event_type_ids[event_type] = event_id

        provenance_ids = []
        provenances = metadata["research_dataset"].get("provenance", [])
        for provenance in provenances:
            # Although it shouldn't happen, theoretically both
            # 'preservation_event' and 'lifecycle_event' could exist in
            # the same provenance metadata.  'preservation_event' is
            # used as the overriding value if both exist.
            if "preservation_event" in provenance:
                event_type = get_localized_value(
                    provenance["preservation_event"]["pref_label"],
                    languages=languages
                )
            elif "lifecycle_event" in provenance:
                event_type = get_localized_value(
                    provenance["lifecycle_event"]["pref_label"],
                    languages=languages
                )
            else:
                raise InvalidDatasetMetadataError(
                    "Provenance metadata does not have key "
                    "'preservation_event' or 'lifecycle_event'. Invalid "
                    f"provenance: {provenance}"
                )
            provenance_ids += [event_type_ids[event_type]]

        return provenance_ids

    def find_file_categories(self):
        """Create logical structure map of dataset files.

        Returns dictionary with filecategories as keys and filepaths as
        values.

        :returns: logical structure map dictionary
        """
        metax_client = get_metax_client(self.config)
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_dataset(self.dataset_id)
        languages = get_dataset_languages(dataset_metadata)
        dirpath2usecategory = get_dirpath_dict(metax_client, dataset_metadata)
        logical_struct = {}

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
                    f"File category for file {file_id} was not found"
                )

            # Append path to logical_struct[filecategory] list. Create
            # list if it does not exist already
            if filecategory not in logical_struct.keys():
                logical_struct[filecategory] = []
            logical_struct[filecategory].append(dataset_file['file_path'])

        return logical_struct

    def get_fileid(self, filepath):
        """Get file id from filesec.xml by filepath.

        :param filepath: file path
        :returns: file identifier
        """
        # pylint: disable=no-member
        filesec_xml \
            = ET.parse(str(self.dataset.sip_creation_path / 'filesec.xml'))

        root = filesec_xml.getroot()

        files = root[0][0]
        for file_ in files:
            for file__ in file_:
                if str(file__.get('{http://www.w3.org/1999/xlink}href'))[7:] \
                        == os.path.join('dataset_files', filepath.strip('/')):
                    return file_.get('ID')

        raise ValueError(
            f"File ID for file {filepath} not found from fileSec: "
            f"{filesec_xml}"
        )


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
