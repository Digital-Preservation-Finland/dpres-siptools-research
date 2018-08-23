"""Dataset metadata validation tools."""

import tempfile
from subprocess import Popen, PIPE
import jsonschema
import siptools_research.utils.metax_schemas as metax_schemas
from siptools_research.utils import mimetypes
from siptools_research.utils.metax import Metax
from siptools_research.workflowtask import InvalidMetadataError
import lxml
from siptools.xml.mets import NAMESPACES


# SCHEMATRONS is a dictionary that contains mapping:
#    file_format_prefix=>xml_metadata_namespace_and_schematron_file
SCHEMATRONS = {
    'image': {'ns': 'http://www.loc.gov/mix/v20', 'schematron':
              '/usr/share/dpres-xml-schemas/schematron/mets_mix.sch'},
    'audio': {'ns': 'http://www.loc.gov/audioMD/', 'schematron':
              '/usr/share/dpres-xml-schemas/schematron/mets_audiomd.sch'},
    'video': {'ns': 'http://www.loc.gov/videoMD/', 'schematron':
              '/usr/share/dpres-xml-schemas/schematron/mets_videomd.sch'}
}

SCHEMATRON_ERROR = "Schematron metadata validation failed: %s. File: %s"
MISSING_XML_METADATA_ERROR = "Missing XML metadata for file: %s"
INVALID_NS_ERROR = "Invalid XML namespace: %s"
DATACITE_VALIDATION_ERROR = 'Datacite (id=%s) validation failed'


def validate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Reads dataset metadata, file metadata, and additional XML metadata
    from Metax and validates them against schemas. Raises error if dataset is
    not valid.

    :returns: ``True``, if dataset metada is valid.
    """
    metax_client = Metax(config)

    # Get dataset metadata from Metax
    dataset_metadata = metax_client.get_dataset(dataset_id)

    # Validate dataset metadata
    _validate_dataset_metadata(dataset_metadata)

    # Get dataset metadata for each listed file, and validates
    # file metadata
    _validate_dataset_metadata_files(dataset_metadata, metax_client)

    # Validate file metadata for each file in dataset files
    _validate_file_metadata(dataset_id, metax_client)

    # Validate XML metadata for each file in dataset files
    _validate_xml_file_metadata(dataset_id, metax_client)

    # Validate XML metadata for each file in dataset files
    _validate_datacite(dataset_id, metax_client)

    return True


def _validate_dataset_metadata(dataset_metadata):
    """Validates dataset metadata from /rest/v1/datasets/<dataset_id>

    :returns: None
    """
    try:
        jsonschema.validate(dataset_metadata,
                            metax_schemas.DATASET_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidMetadataError(exc.message)


# pylint: disable=invalid-name
def _validate_dataset_metadata_files(dataset_metadata, metax_client):
    """Reads file identifiers of each file listed in dataset metadata, and
    validates file metadata found by file identidier from
    /rest/v1/files/<file_id>.

    :returns: None
    """
    for dataset_file in dataset_metadata['research_dataset']['files']:

        file_id = dataset_file['identifier']
        file_metadata = metax_client.get_file(file_id)

        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            message = (
                "Validation error in file metadata of {file_path}: "
                "{message}".format(
                    file_path=file_metadata["file_path"], message=exc.message)
            )
            raise InvalidMetadataError(message)


def _check_mimetype(file_metadata):
    ''' Remove try-except as soon as Metax supports file_format and
    format_version attributes
    '''
    try:
        file_format = file_metadata["file_characteristics"]["file_format"]
        format_version = file_metadata["file_characteristics"]["format_version"]
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
    except KeyError:
        pass


def _validate_file_metadata(dataset_id, metax_client):
    """Validates file metadata found from /rest/v1/datasets/<dataset_id>/files.

    :returns: None
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        _check_mimetype(file_metadata)
        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            message = (
                "Validation error in file {file_path}: {message}"
            ).format(
                file_path=file_metadata["file_path"], message=exc.message
            )
            raise InvalidMetadataError(message)


def _validate_xml_file_metadata(dataset_id, metax_client):
    """Validates additional XML file metadata found from
    /rest/v1/files/<file_id>/xml.

    :returns: None
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        file_format_prefix = file_metadata['file_characteristics'][
            'file_format'].split('/')[0]

        if file_format_prefix in SCHEMATRONS:
            file_id = file_metadata['identifier']
            xmls = metax_client.get_xml('files', file_id)
            for ns_url in xmls:
                if ns_url not in NAMESPACES.values():
                    raise TypeError(INVALID_NS_ERROR % ns_url)
            if SCHEMATRONS[file_format_prefix]['ns'] not in xmls:
                raise InvalidMetadataError(
                    MISSING_XML_METADATA_ERROR % file_id
                )
            _validate_with_schematron(file_format_prefix, file_id, xmls)


def _validate_with_schematron(file_format_prefix, file_id, xmls):
    """Validates XML file with schematron.

    :returns: None
    """
    with tempfile.NamedTemporaryFile() as temp:
        namespace = xmls[SCHEMATRONS[file_format_prefix]['ns']]
        temp.write(lxml.etree.tostring(namespace).strip())
        temp.seek(0)
        schem = SCHEMATRONS[file_format_prefix]['schematron']
        proc = Popen(['check-xml-schematron-features', '-s', schem, temp.name],
                     stdout=PIPE,
                     stderr=PIPE,
                     shell=False,
                     cwd=None,
                     env=None)
        proc.communicate()
        if proc.returncode != 0:
            raise InvalidMetadataError(
                SCHEMATRON_ERROR % (proc.returncode, file_id)
            )


# pylint: disable=invalid-name
def _validate_datacite(dataset_metadata, metax_client):
    """Validates datacite.

    :returns: None
    """
    with tempfile.NamedTemporaryFile() as temp:
        datacite = metax_client.get_datacite(dataset_metadata)
        datacite.write(temp.name)
        temp.seek(0)
        schem = '/etc/xml/dpres-xml-schemas/schema_catalogs/' + \
                'schemas_external/datacite/4.1/metadata.xsd'
        proc = Popen(['check-xml-schema-features', '-s', schem, temp.name],
                     stdout=PIPE,
                     stderr=PIPE,
                     shell=False,
                     cwd=None,
                     env=None)
        proc.communicate()
        if proc.returncode != 0:
            raise InvalidMetadataError(DATACITE_VALIDATION_ERROR %
                                       (dataset_metadata))
