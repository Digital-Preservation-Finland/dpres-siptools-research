"""Luigi task that creates METS document."""
import luigi.format
from luigi import LocalTarget
from mets_builder import (METS, MetsProfile, StructuralMap, AgentType,
                          AgentRole, StructuralMapDiv)
from mets_builder.metadata import (DigitalProvenanceEventMetadata,
                                   ImportedMetadata)
from siptools_ng.sip_digital_object import SIPDigitalObject
from siptools_ng.sip import structural_map_from_directory_structure

from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.metax import get_metax_client
from siptools_research.utils.locale import (get_dataset_languages,
                                            get_localized_value)
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.copy_dataset_to_pas_data_catalog\
    import CopyToPasDataCatalog
from siptools_research.workflow.get_files import GetFiles

# Map event_outcome URI to a valid event outcome
EVENT_OUTCOME = {
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success":
    "success",
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/failure":
    "failure",
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/unknown":
    "(:unkn)",
}


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
        # while METS document is build. The plan is to move metadata
        # download to separate Task, which is required by CreateMets
        # task.
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
        # Get dataset metadata from Metax
        metax_client = get_metax_client(self.config)
        metadata = metax_client.get_dataset(self.dataset_id)
        contract_identifier = metadata["contract"]["identifier"]
        contract_metadata = metax_client.get_contract(contract_identifier)
        files = metax_client.get_dataset_files(self.dataset_id)
        datacite = metax_client.get_datacite(self.dataset_id)

        # Create METS
        mets = METS(
            mets_profile=MetsProfile.RESEARCH_DATA,
            contract_id=contract_identifier,
            package_id=self.dataset.sip_identifier,
            content_id=self.dataset.sip_identifier,
            creator_name="Packaging Service",
            creator_type='OTHER',
            creator_other_type="SOFTWARE"
        )

        # Add user organization as agent
        mets.add_agent(
            name=contract_metadata["contract_json"]["organization"]["name"],
            agent_role=AgentRole.ARCHIVIST,
            agent_type=AgentType.ORGANIZATION
        )

        # Create digital objects
        digital_objects = self._create_digital_objects(files)

        # Create physical structural map
        physical_structural_map \
            = structural_map_from_directory_structure(digital_objects)
        physical_structural_map.structural_map_type = 'Fairdata-physical'
        mets.add_structural_map(physical_structural_map)

        # Create logical structural map
        logical_structural_map \
            = self._create_logical_structmap(digital_objects)
        mets.add_structural_map(logical_structural_map)

        # Add provenance metadata to structural maps
        provenance_metadatas = self._create_provenance_metadata(metadata)
        for provenance_metadata in provenance_metadatas:
            physical_structural_map.root_div.add_metadata(provenance_metadata)
            logical_structural_map.root_div.add_metadata(provenance_metadata)

        # Add descriptive metadata to structural map
        descriptive_metadata = ImportedMetadata(
            data_string=datacite,
            metadata_type="descriptive",
            metadata_format="OTHER",
            other_format="DATACITE",
            # TODO: Version 4.1 is chosen because
            # old siptools is using it. Is it
            # correct?
            format_version="4.1"
        )
        physical_structural_map.root_div.add_metadata(descriptive_metadata)

        # Write METS to file
        mets.generate_file_references()
        mets.write(self.output().path)

    def _create_digital_objects(self, files):
        digital_objects = []
        for file_ in files:
            filepath = file_['file_path'].strip('/')
            source_filepath = (self.dataset.metadata_generation_workspace
                               / "dataset_files" / filepath)
            sip_filepath = "dataset_files/" + filepath
            fc = file_["file_characteristics"]
            digital_object = SIPDigitalObject(
                source_filepath=source_filepath,
                sip_filepath=sip_filepath,
            )
            digital_object.generate_technical_metadata(
                csv_has_header=fc.get("csv_has_header"),
                csv_delimiter=fc.get("csv_delimiter"),
                csv_record_separator=fc.get("csv_record_separator"),
                csv_quoting_character=fc.get("csv_quoting_char"),
                file_format=fc["file_format"],
                file_format_version=fc.get("format_version"),
                charset=fc.get("encoding"),
                file_created_date=fc.get("file_created"),
                checksum_algorithm=file_["checksum"]["algorithm"],
                checksum=file_["checksum"]["value"],
            )
            digital_objects.append(digital_object)

        return digital_objects

    def _create_logical_structmap(self, digital_objects):
        divs = []
        for category, filepaths in self._find_file_categories().items():
            sip_filepaths = ["dataset_files/" + path.strip("/")
                             for path in filepaths]
            category_digital_objects = [
                digital_object
                for digital_object
                in digital_objects
                if digital_object.sip_filepath in sip_filepaths
            ]
            divs.append(
                StructuralMapDiv(div_type=category,
                                 digital_objects=category_digital_objects)
            )

        root_div = StructuralMapDiv(div_type="logical")
        root_div.add_divs(divs)

        return StructuralMap(root_div=root_div,
                             structural_map_type='Fairdata-logical')

    def _create_provenance_metadata(self, metadata):
        provenance_metadatas = []
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
                # TODO: Invalid metadata should be found in metadata
                # validation. So it should be unnecessary to raise
                # InvalidDatasetMetadataError here!
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
                uri = provenance["event_outcome"]["identifier"]
                event_outcome = EVENT_OUTCOME[uri.lower()]
            else:
                event_outcome = "(:unav)"

            event_outcome_detail = provenance.get("outcome_description", None)
            if event_outcome_detail is not None:
                event_outcome_detail = get_localized_value(
                    provenance["outcome_description"],
                    languages=dataset_languages
                )

            provenance_metadatas.append(
                DigitalProvenanceEventMetadata(
                    event_type=event_type,
                    event_datetime=event_datetime,
                    event_detail=event_detail,
                    event_outcome=event_outcome,
                    event_outcome_detail=event_outcome_detail
                )
            )

        return provenance_metadatas

    # TODO: This method might be unnecessary: TPASPKT-1107
    def _find_file_categories(self):
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


# TODO: This function might be unnecessary: TPASPKT-1107
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


# TODO: This function might be unnecessary: TPASPKT-1107
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


# TODO: This function might be unnecessary: TPASPKT-1107
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
