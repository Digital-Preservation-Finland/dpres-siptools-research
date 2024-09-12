"""Dataset metadata validation tools."""

import copy
import os

import lxml
import lxml.isoschematron
from metax_access import DataciteGenerationError
from siptools_research.metax import get_metax_client

import jsonschema
import siptools_research.schemas
from siptools_research.exceptions import (InvalidContractMetadataError,
                                          InvalidDatasetMetadataError,
                                          InvalidFileMetadataError)
from siptools_research.utils import mimetypes

DATACITE_SCHEMA = ('/etc/xml/dpres-xml-schemas/schema_catalogs'
                   '/schemas_external/datacite/4.1/metadata.xsd')


DATACITE_VALIDATION_ERROR = 'Datacite metadata is invalid: %s'


def validate_metadata(
        dataset_id,
        config="/etc/siptools_research.conf",
        dummy_doi="false"
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

    # Validate dataset metadata
    _validate_dataset_metadata(dataset_metadata, dummy_doi=dummy_doi)

    # Validate contract metadata
    _validate_contract_metadata(dataset_metadata['contract']['identifier'],
                                metax_client)

    # Validate file metadata for each file in dataset files
    _validate_file_metadata(dataset_metadata, metax_client)

    # Validate datacite provided by Metax
    _validate_datacite(dataset_id, metax_client, dummy_doi=dummy_doi)

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
        schema["required"] = ["research_dataset", "contract"]
        del schema["properties"]["preservation_identifier"]

    try:
        jsonschema.validate(dataset_metadata, schema)
    except jsonschema.ValidationError as exc:
        raise InvalidDatasetMetadataError(str(exc)) from exc


def _validate_contract_metadata(contract_id, metax_client):
    """Validate contract metadata from /rest/v2/contracts/<contract_id>.

    :param contract_id: contract identifier
    :param metax_clien: metax_access.Metax instance
    :returns: ``None``
    """
    contract_metadata = metax_client.get_contract(contract_id)
    try:
        jsonschema.validate(contract_metadata,
                            siptools_research.schemas.CONTRACT_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidContractMetadataError(str(exc)) from exc


def _check_mimetype(file_metadata):
    """Check that file format is supported.

    :param file_metadata: file metadata dictionary
    :returns: ``None``
    """
    file_format = file_metadata["file_characteristics"]["file_format"]
    try:
        format_version \
            = file_metadata["file_characteristics"]["format_version"]
    except KeyError:
        format_version = ""

    if not mimetypes.is_supported(file_format, format_version):
        message = (
            f"Validation error in file {file_metadata['file_path']}: "
            f"Incorrect file format: {file_format}"
        )

        if format_version:
            message += f", version {format_version}"

        raise InvalidFileMetadataError(message)


def _validate_file_metadata(dataset, metax_client):
    """Validate file metadata found from /rest/v2/datasets/<dataset_id>/files.

    :param dataset: dataset
    :param metax_client: metax_access.Metax instance
    :param conf: siptools_research Configuration object
    :returns: ``None``
    """
    dataset_files = metax_client.get_dataset_files(dataset['identifier'])
    if not dataset_files:
        raise InvalidDatasetMetadataError(
            "Dataset must contain at least one file"
        )
    for file_metadata in dataset_files:

        file_identifier = file_metadata["identifier"]
        file_path = file_metadata["file_path"]

        # Validate metadata against JSON schema. The schema contains
        # properties introduced in JSON schema draft 7. Using
        # Draft7Validator ensures that older validators that do not
        # support draft 7 are not used, in which case part of the schema
        # would be ignored without any warning.
        try:
            jsonschema.Draft7Validator(
                siptools_research.schemas.FILE_METADATA_SCHEMA
            ).validate(
                file_metadata
            )
        except jsonschema.ValidationError as exc:
            raise InvalidFileMetadataError(
                f"Validation error in metadata of {file_path}: {str(exc)}"
            ) from exc

        # Check that mimetype is supported
        _check_mimetype(file_metadata)

        # Check that file path does not point outside SIP
        normalised_path = os.path.normpath(file_path.strip('/'))
        if normalised_path.startswith('..'):
            raise InvalidFileMetadataError(
                f'The file path of file {file_identifier} is invalid:'
                f' {file_path}'
            )


def _validate_datacite(dataset_id, metax_client, dummy_doi="false"):
    """Validate datacite.

    :param dataset_id: dataset identifier
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    try:
        datacite = metax_client.get_datacite(dataset_id, dummy_doi=dummy_doi)
    except DataciteGenerationError as exception:
        raise InvalidDatasetMetadataError(str(exception)) from exception

    datacite_etree = lxml.etree.fromstring(datacite)
    schema = lxml.etree.XMLSchema(lxml.etree.parse(DATACITE_SCHEMA))
    if schema.validate(datacite_etree) is False:
        # pylint: disable=not-an-iterable
        errors = [error.message for error in schema.error_log]
        raise InvalidDatasetMetadataError(
            DATACITE_VALIDATION_ERROR % (_format_error_list(errors))
        )


def _format_error_list(errors):
    """Format list of errors as one error message.

    :param errors: list of strings
    :returns: error message string
    """
    if len(errors) == 1:
        message = errors[0]
    elif len(errors) > 1:
        message = 'The following errors were detected:\n'
        for error in enumerate(errors, 1):
            message += f'\n{error[0]}. {error[1]}'
    else:
        raise TypeError("Can not format empty list")

    return message
