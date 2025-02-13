"""Dataset metadata validation tools."""
import copy
import os

import jsonschema

import siptools_research.schemas
from siptools_research.exceptions import (
    InvalidDatasetMetadataError,
    InvalidFileMetadataError,
)
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

    Validates dataset metadata from /rest/v2/datasets/<dataset_id>

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
    """Validate file metadata found from /rest/v2/datasets/<dataset_id>/files.

    :param dataset: dataset
    :param metax_client: metax_access.Metax instance
    :param conf: siptools_research Configuration object
    :returns: ``None``
    """
    dataset_files = metax_client.get_dataset_files(dataset["id"])
    if not dataset_files:
        raise InvalidDatasetMetadataError(
            "Dataset must contain at least one file"
        )
    for file_metadata in dataset_files:
        file_identifier = file_metadata["id"]
        file_path = file_metadata["pathname"]

        # Validate metadata against JSON schema. The schema contains
        # properties introduced in JSON schema draft 7. Using
        # Draft7Validator ensures that older validators that do not
        # support draft 7 are not used, in which case part of the schema
        # would be ignored without any warning.

        try:
            jsonschema.Draft7Validator(
                siptools_research.schemas.FILE_METADATA_SCHEMA
            ).validate(file_metadata)
        except jsonschema.ValidationError as exc:
            raise InvalidFileMetadataError(
                f"Validation error in metadata of {file_path}: {str(exc)}"
            ) from exc

        # Check that file path does not point outside SIP
        normalised_path = os.path.normpath(file_path.strip("/"))
        if normalised_path.startswith(".."):
            raise InvalidFileMetadataError(
                f"The file path of file {file_identifier} is invalid:"
                f" {file_path}"
            )
