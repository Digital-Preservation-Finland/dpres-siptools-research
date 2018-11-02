"""Module that generates technical metadata for dataset files and writes it to
Metax.
"""
import os
import shutil
import tempfile
from requests.exceptions import HTTPError

from metax_access import Metax
from siptools.scripts import import_object, create_mix
from siptools.scripts import create_addml, create_audiomd
from siptools_research.utils import ida
from siptools_research.config import Configuration


def generate_metadata(dataset_id, config="/etc/siptools_research.conf"):
    """Generates technical metadata and mix metadata for all files of a given
    dataset and updates relevant fields in file metadata.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    config_object = Configuration(config)
    metax_client = Metax(config_object.get('metax_url'),
                         config_object.get('metax_user'),
                         config_object.get('metax_password'))
    tmpdir = tempfile.mkdtemp(prefix='generate_metadata-')

    try:
        for file_ in metax_client.get_dataset_files(dataset_id):

            # Get file info
            file_id = file_['identifier']
            file_metadata = metax_client.get_file(file_id)

            # Download file to tmp directory
            tmpfile = os.path.join(tmpdir, file_id)
            try:
                ida.download_file_header(file_id, tmpfile, config)
            except HTTPError as error:
                file_path = file_['file_path']
                status_code = error.response.status_code
                if status_code == 404:
                    raise Exception("File %s not found in Ida." % file_path)
                elif status_code == 403:
                    raise Exception("Access to file %s forbidden." % file_path)
                else:
                    raise Exception("File %s could not be retrieved." %
                                    file_path)

            # Generate and update file_characteristics
            file_characteristics = _generate_techmd(tmpfile, file_metadata)
            metax_client.set_file_characteristics(file_id,
                                                  file_characteristics)

            # Generate and post mix metadata
            if file_characteristics['file_format'].startswith('image'):
                ida.download_file(file_id, tmpfile, config)
                mix_element = create_mix.create_mix(tmpfile)
                metax_client.set_xml(file_id, mix_element)

            # Generate and post ADDML metadata
            elif file_characteristics['file_format'] == 'text/csv':

                addml_element = create_addml.create_addml(
                    tmpfile,
                    file_characteristics['csv_delimiter'],
                    file_characteristics['csv_has_header'],
                    file_characteristics['encoding'],
                    file_characteristics['csv_record_separator'],
                    file_characteristics['csv_quoting_char'],
                    flatfile_name=file_metadata['file_path']
                )
                metax_client.set_xml(file_id, addml_element)

            # Generate and post AudioMD metadata
            elif file_characteristics['file_format'] == 'audio/x-wav':

                # Try generating metadata based on the 512 byte header
                # returned by ida.download_file_header
                try:
                    audiomd_element = create_audiomd.create_audiomd(tmpfile)

                # Download the whole file if metadata generation fails using
                # only the first 512 bytes
                except ValueError:
                    ida.download_file(file_id, tmpfile, config)
                    audiomd_element = create_audiomd.create_audiomd(tmpfile)

                metax_client.set_xml(file_id, audiomd_element)

    finally:
        shutil.rmtree(tmpdir)


def _generate_techmd(filepath, original_metadata):
    """Reads file and generates technical metadata. `file_characteristics`
    object is read from original meta.  Generated metadata is appended
    `file_characteristics` object. If a field already has a value (set by
    user) it will not be updated.

    :param filepath: path to file for which the metadata is generated
    :param original_metadata: full original metadata dictionary
    :returns: New `file_characteristics` dictionary
    """

    # Generate technical metadata from file
    metadata = import_object.metadata_info(filepath)

    # Create file_characteristics object
    file_characteristics = {}
    file_characteristics['file_format'] = metadata['format']['mimetype']
    file_characteristics['format_version'] = metadata['format']['version']
    if 'charset' in metadata['format'].keys():
        file_characteristics['encoding'] = metadata['format']['charset']

    # Merge generated file_characteristics with original data from Metax.
    # If a field was already defined in original data, it will override the
    # generated value.
    if 'file_characteristics' in original_metadata:
        file_characteristics.update(
            original_metadata['file_characteristics']
            )

    return file_characteristics
