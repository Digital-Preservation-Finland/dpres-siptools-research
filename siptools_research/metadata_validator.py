"""Dataset metadata validation tools."""

import os
import copy
import jsonschema
import lxml
import lxml.isoschematron
from iso639 import languages
from requests.exceptions import HTTPError

from siptools.xml.mets import NAMESPACES

from metax_access import (Metax, MetaxError, DatasetNotFoundError,
                          DataciteGenerationError, DS_STATE_VALIDATING_METADATA,
                          DS_STATE_INVALID_METADATA, DS_STATE_VALID_METADATA,
                          DS_STATE_METADATA_VALIDATION_FAILED)
import siptools_research.schemas
from siptools_research.utils import mimetypes
from siptools_research.utils.dataset_consistency import DatasetConsistency
from siptools_research.utils.directory_validation import DirectoryValidation
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration


# SCHEMATRONS is a dictionary that contains mapping:
#    file_format_prefix=>xml_metadata_namespace_and_schematron_file
SCHEMATRONS = {
    'image': {'ns': 'http://www.loc.gov/mix/v20', 'schema':
              '/usr/share/dpres-xml-schemas/schematron/mets_mix.sch'},
    'audio': {'ns': 'http://www.loc.gov/audioMD/', 'schema':
              '/usr/share/dpres-xml-schemas/schematron/mets_audiomd.sch'},
    'video': {'ns': 'http://www.loc.gov/videoMD/', 'schema':
              '/usr/share/dpres-xml-schemas/schematron/mets_videomd.sch'}
}
DATACITE_SCHEMA = ('/etc/xml/dpres-xml-schemas/schema_catalogs'
                   '/schemas_external/datacite/4.1/metadata.xsd')


TECHMD_XML_VALIDATION_ERROR \
    = "Technical metadata XML of file %s is invalid: %s"
DATACITE_VALIDATION_ERROR = 'Datacite metadata is invalid: %s'
INVALID_NS_ERROR = "Invalid XML namespace: %s"
MISSING_XML_METADATA_ERROR = "Missing technical metadata XML for file: %s"


def validate_metadata(
        dataset_id,
        config="/etc/siptools_research.conf",
        dummy_doi="false",
        set_preservation_state=False,
        set_intermediate_state=True
):
    """Validate dataset.

    Reads dataset metadata, file metadata, and additional techMD XML from
    Metax and validates them against schemas. Raises error if dataset is not
    valid.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :param: dummy_doi: 'true' if dummy preservation identifier is to be used
    :param: set_preservation_state: ``True`` if dataset preservation_state is
        to be set
    :returns: ``True``, if dataset metadata is valid.
    """
    conf = Configuration(config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    try:
        success = False
        # set default values
        status_code = DS_STATE_METADATA_VALIDATION_FAILED
        message = "Metadata validation failed"

        if set_intermediate_state:
            metax_client.set_preservation_state(
                dataset_id,
                state=DS_STATE_VALIDATING_METADATA
            )

        # Get dataset metadata from Metax
        dataset_metadata = metax_client.get_dataset(dataset_id)

        # Validate dataset metadata
        _validate_dataset_metadata(dataset_metadata, dummy_doi=dummy_doi)

        # Validate dataset localization
        _validate_dataset_localization(dataset_metadata)

        # Validate contract metadata
        _validate_contract_metadata(dataset_metadata['contract']['identifier'],
                                    metax_client)

        # Validate file metadata for each file in dataset files
        _validate_file_metadata(dataset_metadata, metax_client, conf)

        # Validate techMD XML for each file in dataset files
        _validate_xml_file_metadata(dataset_id, metax_client)

        # Validate datacite provided by Metax
        _validate_datacite(dataset_id, metax_client, dummy_doi=dummy_doi)
    except DatasetNotFoundError:
        # Skip setting preservation state if validation failed because dataset
        # was not found in Metax.
        status_code = None
        raise
    except (InvalidMetadataError, MetaxError) as exc:
        detailed_error = str(exc)
        if isinstance(exc, InvalidMetadataError):
            status_code = DS_STATE_INVALID_METADATA
            message = ("Metadata did not pass validation: %s" % detailed_error)
        else:
            status_code = DS_STATE_METADATA_VALIDATION_FAILED
            message = "Metadata validation failed: %s" % detailed_error
        raise
    except HTTPError:
        message = "Internal error"
        status_code = DS_STATE_METADATA_VALIDATION_FAILED
        raise
    else:
        success = True
        status_code = DS_STATE_VALID_METADATA
        message = "Metadata passed validation"
    finally:
        if set_preservation_state:
            message = message[:199] if len(message) > 200 else message
            metax_client.set_preservation_state(dataset_id, state=status_code,
                                                system_description=message)
    return success


def _validate_dataset_metadata(dataset_metadata, dummy_doi="false"):
    """Validate dataset metadata.

    Validates dataset metadata from /rest/v1/datasets/<dataset_id>

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
        raise InvalidMetadataError(str(exc))


def _validate_dataset_localization(dataset_metadata):
    """Validate dataset localization.

    Validates that all required translations are provided with valid ISO
    639-1 language codes.
    """
    research_dataset = dataset_metadata["research_dataset"]
    provenance_list = research_dataset["provenance"]

    # Provenance translations
    for provenance in provenance_list:
        _validate_localization(
            provenance["description"], "provenance/description"
        )
        _validate_localization(
            provenance["preservation_event"]["pref_label"],
            "provenance/preservation_event/pref_label"
        )
        _validate_localization(
            provenance["event_outcome"]["pref_label"],
            "provenance/event_outcome/pref_label"
        )
        _validate_localization(
            provenance["outcome_description"], "provenance/outcome_description"
        )

    # Files translations
    if "files" in research_dataset:
        for _file in research_dataset["files"]:
            _validate_localization(
                _file["use_category"]["pref_label"],
                "files/use_category/pref_label"
            )

    # Directories translations
    if "directories" in research_dataset:
        for _dir in research_dataset["directories"]:
            _validate_localization(
                _dir["use_category"]["pref_label"],
                "directories/user_category/pref_label"
            )


def _validate_localization(localization_dict, field):
    """Validate languages.

    Check that the localization dict is not empty and all the keys are valid
    ISO 639-1 language codes.
    """
    if not localization_dict:
        raise InvalidMetadataError(
            "No localization provided in field: 'research_dataset/%s'" % field
        )

    for language in localization_dict:
        # Per MetaX schema, 'und' and 'zxx' are fallbacks for content that
        # can't be localized
        if language in ("und", "zxx"):
            continue

        try:
            languages.get(part1=language)
        except KeyError:
            message = (
                "Invalid language code: '%s' in field: 'research_dataset/%s'"
            ) % (language, field)

            raise InvalidMetadataError(message)


def _validate_contract_metadata(contract_id, metax_client):
    """Validate dataset metadata from /rest/v1/datasets/<dataset_id>.

    :param contract_id: contract identifier
    :param metax_clien: metax_access.Metax instance
    :returns: ``None``
    """
    contract_metadata = metax_client.get_contract(contract_id)
    try:
        jsonschema.validate(contract_metadata,
                            siptools_research.schemas.CONTRACT_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidMetadataError(str(exc))


def _check_mimetype(file_metadata, conf):
    """Check that file format is supported.

    :param file_metadata: file metadata dictionary
    :param conf: siptools_research Configuration object
    :returns: ``None``
    """
    file_format = file_metadata["file_characteristics"]["file_format"]
    try:
        format_version \
            = file_metadata["file_characteristics"]["format_version"]
    except KeyError:
        format_version = ""

    mimetypes_conf = conf.get("mimetypes_conf")
    if not mimetypes.is_supported(file_format, format_version, mimetypes_conf):
        message = (
            "Validation error in file {file_path}: Incorrect file "
            "format: {file_format}"
        ).format(
            file_path=file_metadata["file_path"],
            file_format=file_format,
        )

        if format_version:
            message += ", version %s" % format_version

        raise InvalidMetadataError(message)


def _validate_file_metadata(dataset, metax_client, conf):
    """Validate file metadata found from /rest/v1/datasets/<dataset_id>/files.

    :param dataset: dataset
    :param metax_client: metax_access.Metax instance
    :param conf: siptools_research Configuration object
    :returns: ``None``
    """
    # DatasetConsistency is used to verify file consistency within the dataset
    # i.e. every file returned by Metax API /datasets/datasetid/files
    # can be found from dataset.file or dataset.directories properties
    consistency = DatasetConsistency(metax_client, dataset)
    directory_validation = DirectoryValidation(metax_client)
    for file_metadata in metax_client.get_dataset_files(dataset['identifier']):
        file_identifier = file_metadata["identifier"]
        file_path = file_metadata["file_path"]
        # Validate metadata against JSON schema
        try:
            jsonschema.validate(file_metadata,
                                siptools_research.schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            raise InvalidMetadataError(
                "Validation error in metadata of {file_path}: {error}"
                .format(file_path=file_path, error=str(exc))
            )
        directory_validation.is_valid_for_file(file_metadata)
        # Check that mimetype is supported
        _check_mimetype(file_metadata, conf)

        # Check that file path does not point outside SIP
        normalised_path = os.path.normpath(file_path.strip('/'))
        if normalised_path.startswith('..'):
            raise InvalidMetadataError(
                'The file path of file %s is invalid: %s' % (file_identifier,
                                                             file_path)
            )
        consistency.is_consistent_for_file(file_metadata)


def _validate_xml_file_metadata(dataset_id, metax_client):
    """Validate additional techMD XML.

    XML file metadata found from /rest/v1/files/<file_id>/xml.

    :param dataset_id: identifier of file described by XML
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        file_format_prefix = file_metadata['file_characteristics'][
            'file_format'].split('/')[0]

        if file_format_prefix in SCHEMATRONS:
            file_id = file_metadata['identifier']
            try:
                xmls = metax_client.get_xml('files', file_id)
            except lxml.etree.XMLSyntaxError as exception:
                raise InvalidMetadataError(
                    TECHMD_XML_VALIDATION_ERROR % (file_id, exception)
                )

            for namespace in xmls:
                if namespace not in NAMESPACES.values():
                    raise TypeError(INVALID_NS_ERROR % namespace)

            if SCHEMATRONS[file_format_prefix]['ns'] not in xmls:
                raise InvalidMetadataError(
                    MISSING_XML_METADATA_ERROR % file_id
                )

            _validate_with_schematron(
                file_format_prefix,
                xmls[SCHEMATRONS[file_format_prefix]['ns']],
                file_id
            )


def _validate_with_schematron(filetype, xml, file_id):
    """Validate XML with schematron.

    Parses validation error from schematron output and raises
    InvalidMetadataError with clear error message.

    :param filetype: Type of file described by XML (image, video, or audio)
    :param xml: XML element
    :param file_id: identifier of file described by XML
    :returns: ``None``
    """
    schema_file = SCHEMATRONS[filetype]['schema']
    schema = lxml.isoschematron.Schematron(lxml.etree.parse(schema_file))
    if schema.validate(xml) is False:

        # Parse error messages from schematron output
        errors = []
        for error in schema.error_log:
            error_xml = lxml.etree.fromstring(error.message)
            error_string = error_xml.xpath(
                '//svrl:text',
                namespaces={'svrl': 'http://purl.oclc.org/dsdl/svrl'}
            )[0].text.strip()
            errors.append(error_string)

        raise InvalidMetadataError(
            TECHMD_XML_VALIDATION_ERROR % (file_id, _format_error_list(errors))
        )


def _validate_datacite(dataset_id, metax_client, dummy_doi="false"):
    """Validate datacite.

    :param dataset_id: dataset identifier
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    try:
        datacite = metax_client.get_datacite(dataset_id, dummy_doi=dummy_doi)
    except (lxml.etree.XMLSyntaxError, DataciteGenerationError) as exception:
        raise InvalidMetadataError(
            DATACITE_VALIDATION_ERROR % (exception)
        )

    schema = lxml.etree.XMLSchema(lxml.etree.parse(DATACITE_SCHEMA))
    if schema.validate(datacite) is False:
        # pylint: disable=not-an-iterable
        errors = [error.message for error in schema.error_log]
        raise InvalidMetadataError(
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
            message += '\n%s. %s' % (error)
    else:
        raise TypeError("Can not format empty list")

    return message
