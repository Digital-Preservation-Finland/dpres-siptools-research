"""Dataset metadata validation tools."""

import jsonschema
import lxml
import lxml.isoschematron
from metax_access import Metax
import siptools_research.utils.metax_schemas as metax_schemas
from siptools_research.utils import mimetypes
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
from siptools.xml.mets import NAMESPACES


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


def validate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Reads dataset metadata, file metadata, and additional techMD XML from
    Metax and validates them against schemas. Raises error if dataset is not
    valid.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True``, if dataset metada is valid.
    """
    conf = Configuration(config)
    metax_client = Metax(conf.get('metax_url'),
                         conf.get('metax_user'),
                         conf.get('metax_password'))

    # Get dataset metadata from Metax
    dataset_metadata = metax_client.get_dataset(dataset_id)

    # Validate dataset metadata
    _validate_dataset_metadata(dataset_metadata)

    # Validate contract metadata
    _validate_contract_metadata(dataset_metadata['contract']['identifier'],
                                metax_client)

    # Get dataset metadata for each listed file, and validates
    # file metadata
    _validate_dataset_files(dataset_metadata, metax_client)

    # Validate file metadata for each file in dataset files
    _validate_file_metadata(dataset_id, metax_client)

    # Validate techMD XML for each file in dataset files
    _validate_xml_file_metadata(dataset_id, metax_client)

    # Validate datacite provided by Metax
    _validate_datacite(dataset_id, metax_client)

    return True


def _validate_dataset_metadata(dataset_metadata):
    """Validates dataset metadata from /rest/v1/datasets/<dataset_id>

    :param dataset_metadata: dataset metadata dictionary
    :returns: ``None``
    """
    try:
        jsonschema.validate(dataset_metadata,
                            metax_schemas.DATASET_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidMetadataError(str(exc))


def _validate_contract_metadata(contract_id, metax_client):
    """Validates dataset metadata from /rest/v1/datasets/<dataset_id>

    :param contract_id: contract identifier
    :param metax_clien: metax_access.Metax instance
    :returns: ``None``
    """
    contract_metadata = metax_client.get_contract(contract_id)
    try:
        jsonschema.validate(contract_metadata,
                            metax_schemas.CONTRACT_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidMetadataError(str(exc))


def _validate_dataset_files(dataset_metadata, metax_client):
    """Reads file identifiers of each file listed in dataset metadata, and
    validates file metadata found by file identidier from
    /rest/v1/files/<file_id>.

    :param dataset_metadata: dataset metadata dictionary
    :param metax_clien: metax_access.Metax instance
    :returns: ``None``
    """
    for dataset_file in dataset_metadata['research_dataset']['files']:

        file_id = dataset_file['identifier']
        file_metadata = metax_client.get_file(file_id)

        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            message = (
                "Validation error in metadata of {file_path}: {error}"
            ).format(file_path=file_metadata["file_path"],
                     error=str(exc))
            raise InvalidMetadataError(message)


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
            "Validation error in file {file_path}: Incorrect file "
            "format: {file_format}, version {version}"
        ).format(
            file_path=file_metadata["file_path"],
            file_format=file_format,
            version=format_version
        )
        raise InvalidMetadataError(message)


def _validate_file_metadata(dataset_id, metax_client):
    """Validates file metadata found from /rest/v1/datasets/<dataset_id>/files.

    :param dataset_id: identifier of file
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            message = (
                "Validation error in metadata of {file_path}: {error}"
            ).format(file_path=file_metadata["file_path"],
                     error=str(exc))
            raise InvalidMetadataError(message)
        _check_mimetype(file_metadata)


def _validate_xml_file_metadata(dataset_id, metax_client):
    """Validates additional techMD XML file metadata found from
    /rest/v1/files/<file_id>/xml.

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
    """Validates XML with schematron. Parses validation error from schematron
    output and raises InvalidMetadataError with clear error message.

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


def _validate_datacite(dataset_id, metax_client):
    """Validates datacite.

    :param dataset_metadata: dataset metadata dictionary
    :param metax_client: metax_access.Metax instance
    :returns: ``None``
    """
    try:
        datacite = metax_client.get_datacite(dataset_id)
    except lxml.etree.XMLSyntaxError as exception:
        raise InvalidMetadataError(
            DATACITE_VALIDATION_ERROR % (exception)
        )

    schema = lxml.etree.XMLSchema(lxml.etree.parse(DATACITE_SCHEMA))
    if schema.validate(datacite) is False:
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
