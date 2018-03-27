"""Dataset metadata validation tools."""

import tempfile
from subprocess import Popen, PIPE
import jsonschema
import siptools_research.utils.metax_schemas as metax_schemas
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
              '/usr/share/dpres-xml-schemas/schematron/mets_avmd.sch'},
    'video': {'ns': 'http://www.loc.gov/videoMD/', 'schematron':
              '/usr/share/dpres-xml-schemas/schematron/mets_avmd.sch'}
}

SHEM_ERR = "Schematron metadata validation failed: %s. File: %s"
MISS_XML_ERR = "Missing XML metadata for file: %s"
INV_NS_ERR = "Invalid XML namespace: %s"


def validate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Reads dataset metadata, file metadata, and additional XML metadata
    from Metax and validates them against schemas.

    :returns: True, if dataset metada is valid
    """
    # Get dataset metadata from Metax
    metax_client = Metax(config)
    dataset_metadata = metax_client.get_data('datasets',
                                             dataset_id)

    try:
        # Validate dataset metadata
        _validate_dataset_metadata(dataset_metadata)

        # Get dataset metadata for each listed file, and validates file metadata
        _validate_dataset_metadata_files(dataset_metadata, metax_client)

        # Validate file metadata for each file in dataset files
        _validate_file_metadata(dataset_id, metax_client)

        # Validate XML metadata for each file in dataset files
        _validate_xml_file_metadata(dataset_id, metax_client)
    except InvalidMetadataError as exc:
        return exc.message

    return True


def _validate_dataset_metadata(dataset_metadata):
    """Validates dataset metadata from /rest/v1/datasets/<dataset_id>

    :returns: None
    """
    try:
        jsonschema.validate(dataset_metadata,
                            metax_schemas.DATASET_METADATA_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise InvalidMetadataError(exc)


# pylint: disable=invalid-name
def _validate_dataset_metadata_files(dataset_metadata, metax_client):
    """Reads file identifiers of each file listed in dataset metadata, and
    validates file metadata found by file identidier from
    /rest/v1/files/<file_id>.

    :returns: None
    """
    for dataset_file in dataset_metadata['research_dataset']['files']:

        file_id = dataset_file['identifier']
        file_metadata = metax_client.get_data('files', file_id)

        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            raise InvalidMetadataError(exc)


def _validate_file_metadata(dataset_id, metax_client):
    """Validates file metadata found from /rest/v1/datasets/<dataset_id>/files.

    :returns: None
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        try:
            jsonschema.validate(file_metadata,
                                metax_schemas.FILE_METADATA_SCHEMA)
        except jsonschema.ValidationError as exc:
            raise InvalidMetadataError(exc)


def _validate_xml_file_metadata(dataset_id, metax_client):
    """Validates additional XML file metadata found from
    /rest/v1/files/<file_id>/xml.

    :returns: None
    """
    for file_metadata in metax_client.get_dataset_files(dataset_id):
        file_format_prefix = file_metadata['file_format'].split('/')[0]
        if file_format_prefix in SCHEMATRONS:
            file_id = file_metadata['identifier']
            xmls = metax_client.get_xml('files', file_id)
            for ns_url in xmls:
                if ns_url not in NAMESPACES.values():
                    raise TypeError(INV_NS_ERR % ns_url)
            if SCHEMATRONS[file_format_prefix]['ns'] not in xmls:
                raise InvalidMetadataError(MISS_XML_ERR % file_id)
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
            raise InvalidMetadataError(SHEM_ERR % (proc.returncode, file_id))
