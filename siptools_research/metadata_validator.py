"""Dataset metadata validation tools."""
import copy
import os

import jsonschema
from file_scraper.defaults import (BIT_LEVEL_WITH_RECOMMENDED, RECOMMENDED,
                                   UNACCEPTABLE)

import siptools_research.schemas
from siptools_research.exceptions import (InvalidDatasetFileError,
                                          InvalidDatasetMetadataError,
                                          InvalidFileMetadataError)
from siptools_research.metax import get_metax_client


def validate_metadata(
    dataset_id, config="/etc/siptools_research.conf", dummy_doi="false"
):
    """Validate dataset.

    Reads dataset metadata and file metadata from Metax and validates
    them against schemas. Raises error if dataset is not valid. Raises
    InvalidDatasetError if dataset is invalid.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :param: dummy_doi: 'true' if dummy preservation identifier is to be
                       used
    :returns: ``True``, if dataset metadata is valid.
    """
    metax_client = get_metax_client(config)
    # Get dataset metadata from Metax
    dataset_metadata = metax_client.get_dataset(dataset_id)
    _validate_dataset_metadata(dataset_metadata, dummy_doi=dummy_doi)

    # Validate file metadata for each file in dataset files
    _validate_file_metadata(dataset_metadata, metax_client)

    return True


def _validate_dataset_metadata(dataset_metadata, dummy_doi="false"):
    """Validate dataset metadata.

    :param dataset_metadata: dataset metadata dictionary
    :returns: ``None``
    """
    schema = copy.deepcopy(siptools_research.schemas.DATASET_METADATA_SCHEMA)
    # If dummy DOI is used, drop preeservation_identifier from schema
    if dummy_doi == "true":
        schema["properties"]["preservation"]["required"] = ["contract"]
        del schema["properties"]["preservation"]["properties"]["id"]

    try:
        jsonschema.validate(dataset_metadata, schema)
    except jsonschema.ValidationError as exc:
        raise InvalidDatasetMetadataError(str(exc)) from exc


def _validate_file_metadata(dataset, metax_client):
    """Validate file metadata.

    :param dataset: dataset
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    dataset_files = metax_client.get_dataset_files(dataset["id"])
    if not dataset_files:
        raise InvalidDatasetMetadataError(
            "Dataset must contain at least one file"
        )

    # Validate that every DPRES compatible and bit-level file pair
    # is included in the dataset
    _validate_file_dpres_links(dataset_files)

    for file_metadata in dataset_files:
        file_identifier = file_metadata["id"]
        file_path = file_metadata["pathname"]

        is_linked_bitlevel = bool(file_metadata["pas_compatible_file"])
        is_linked_pas_compatible = bool(
            file_metadata["non_pas_compatible_file"]
        )

        # Validate metadata against JSON schema. The schema contains
        # properties introduced in JSON schema draft 7. Using
        # Draft7Validator ensures that older validators that do not
        # support draft 7 are not used, in which case part of the schema
        # would be ignored without any warning.

        characteristics = file_metadata["characteristics"] or {}
        char_ext = file_metadata["characteristics_extension"] or {}
        file_format_version = characteristics.get("file_format_version")
        if not file_format_version and not is_linked_bitlevel:
            raise InvalidFileMetadataError(
                f"Non bit-level file must have `file_format_version` set: "
                f"{file_path}",
                files=[file_metadata]
            )

        try:
            jsonschema.Draft7Validator(
                siptools_research.schemas.FILE_METADATA_SCHEMA
            ).validate(file_metadata)
        except jsonschema.ValidationError as exc:
            raise InvalidFileMetadataError(
                f"Validation error in metadata of {file_path}: {str(exc)}",
                files=[file_metadata]
            ) from exc

        # Check that file path does not point outside SIP
        normalised_path = os.path.normpath(file_path.strip("/"))
        if normalised_path.startswith(".."):
            raise InvalidFileMetadataError(
                f"The file path of file {file_identifier} is invalid:"
                f" {file_path}",
                files=[file_metadata]
            )

        # Check that files with "bit-level-with-recommended" or "unacceptable"
        # grade have a PAS compatible file
        is_incomplete_bitlevel = (
            char_ext["grade"] in (BIT_LEVEL_WITH_RECOMMENDED, UNACCEPTABLE)
            and not is_linked_bitlevel
        )
        if is_incomplete_bitlevel:
            raise InvalidFileMetadataError(
                f"File {file_identifier} with '{char_ext['grade']}' "
                f"grade is not linked to a PAS compatible file",
                files=[file_metadata]
            )

        # Check that PAS compatible file has a good enough grade
        if is_linked_pas_compatible and char_ext["grade"] != RECOMMENDED:
            raise InvalidFileMetadataError(
                f"File {file_identifier} with grade '{char_ext['grade']}' "
                f"marked as PAS compatible does not have the required "
                f"'{RECOMMENDED}' grade",
                files=[file_metadata]
            )


def _validate_file_dpres_links(dataset_files):
    """
    Validate that dataset files contain every DPRES & non DPRES compatible file
    pair if any exist

    :param dataset_files: List of datasets

    :raises InvalidDatasetFileError: If any linked files are missing their
        DPRES and/or bit-level counterparts
    """
    dpres2non_dpres_file_id = {}
    non_dpres2dpres_file_id = {}

    # Create DPRES compatibility links
    for file in dataset_files:
        if id_ := file["non_pas_compatible_file"]:
            non_dpres2dpres_file_id[id_] = file["id"]

        if id_ := file["pas_compatible_file"]:
            dpres2non_dpres_file_id[id_] = file["id"]

    lone_non_dpres_ids = [
        id_ for id_ in dpres2non_dpres_file_id.values()
        if id_ not in non_dpres2dpres_file_id
    ]

    lone_dpres_ids = [
        id_ for id_ in non_dpres2dpres_file_id.values()
        if id_ not in dpres2non_dpres_file_id
    ]

    if lone_dpres_ids or lone_non_dpres_ids:
        error = "File linkings for DPRES compatible files are incomplete. "
        all_lone_ids = lone_non_dpres_ids + lone_dpres_ids

        if lone_dpres_ids and not lone_non_dpres_ids:
            error += (
                "Dataset contains DPRES compatible files without "
                "bit-level counterparts."
            )
        elif not lone_dpres_ids and lone_non_dpres_ids:
            error += (
                "Dataset contains bit-level files without "
                "DPRES compatible counterparts."
            )
        else:
            error += (
                "Dataset contains both bit-level and DPRES compatible "
                "files without DPRES compatible / bit-level counterparts."
            )

        lone_files = [
            file_ for file_ in dataset_files
            if file_["id"] in all_lone_ids
        ]

        raise InvalidDatasetFileError(error, files=lone_files)
