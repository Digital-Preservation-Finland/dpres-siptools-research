"""Module that generates technical metadata for dataset files and writes it to
Metax.
"""
import os
import shutil
import tempfile
import siptools.scripts.import_object
import siptools.scripts.create_mix
import siptools_research.utils.metax
import siptools_research.utils.ida

def generate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Generates technical metadata and mix metadata for all files of a given
    dataset and updates relevant fields in file metadata.
    """
    metax_client = siptools_research.utils.metax.Metax(config)
    tmpdir = tempfile.mkdtemp(prefix='generate_metadata-')
    try:
        for file_ in metax_client.get_dataset_files(dataset_id):

            # Get file info
            file_id = file_['identifier']
            file_metadata = metax_client.get_file(file_id)

            # Download file to tmp directory
            tmpfile = os.path.join(tmpdir, file_id)
            siptools_research.utils.ida.download_file_header(file_id, tmpfile,
                                                             config)

            # Generate and update file_characteristics
            file_characteristics = _generate_techmd(tmpfile, file_metadata)
            metax_client.set_file_characteristics(file_id,
                                                  file_characteristics)

            # Generate and post mix metadata
            if file_characteristics['file_format'].startswith('image'):
                mix_element = siptools.scripts.create_mix.write_mix(tmpfile)
                metax_client.set_xml(file_id, mix_element)
    finally:
        shutil.rmtree(tmpdir)

def _generate_techmd(tmpfile, original_metadata):
    """Reads file and generates technical metadata. Saves metada to
    "file_characteristics" object in Metax. If a field already has a value
    (set by user) it will not be updated.
    """

    # Generate technical metadata from file
    metadata = siptools.scripts.import_object.metadata_info(tmpfile)

    # Create file_characteristics object
    file_characteristics = {}
    file_characteristics['file_format'] = metadata['format']['mimetype']
    file_characteristics['format_version'] = metadata['format']['version']
    if 'charset' in metadata['format'].keys():
        file_characteristics['encoding'] = metadata['format']['charset']

    # Merge generated file_characteristics with original data from Metax.
    # If a field was already defined in original data, it will override the
    # generated value.
    file_characteristics.update(
        original_metadata['file_characteristics']
    )

    return file_characteristics
