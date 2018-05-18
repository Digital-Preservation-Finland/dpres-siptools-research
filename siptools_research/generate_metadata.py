"""Module that generates technical metadata for dataset files and writes it to
Metax.
"""
import uuid
import siptools.scripts.import_object
import siptools_research.utils.metax
import siptools_research.utils.ida

def generate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Generates technical metadata for all files of a given dataset and
    updates relevant fields in Metax. If a field already has a value (set by
    user) it will not be updated.
    """
    metax_client = siptools_research.utils.metax.Metax(config)
    for file_ in metax_client.get_dataset_files(dataset_id):
        file_id = file_['identifier']

        # Download file to tmp directory
        tmpfile = '/tmp/%s_%s' % (file_id, uuid.uuid4())
        siptools_research.utils.ida.download_file(file_id, tmpfile, config)

        # Read technical metadata
        metadata = siptools.scripts.import_object.metadata_info(tmpfile)

        # Create file_characteristics
        file_characteristics = {}
        file_characteristics['file_format'] = metadata['format']['mimetype']
        file_characteristics['format_version'] = metadata['format']['version']
        file_characteristics['encoding'] = metadata['format']['charset']

        # Merge generated file_characteristics with original data from Metax.
        # If a field was already defined in original data, it will override the
        # generated value.
        file_characteristics.update(
            metax_client.get_file(file_id)['file_characteristics']
        )

        # Update file_characteristics
        metax_client.set_file_characteristics(file_id, file_characteristics)
