"""Generates metadata required to create SIP."""
import file_scraper
import file_scraper.scraper
from metax_access import Metax

from siptools_research.config import Configuration
from siptools_research.exceptions import InvalidFileError


def generate_metadata(dataset_id,
                      root_directory,
                      config="/etc/siptools_research.conf"):
    """Generate dataset metadata.

    Generates metadata required for creating SIP:
    - techincal metadata for all dataset files
    - file format specific metadata for files

    Raises InvalidDatasetError if metadata can not be generated due to
    invalid files.

    :param dataset_id: identifier of dataset
    :param root_directory: directory where files are found
    :param config: path to configuration file
    :returns: ``None``
    """
    config_object = Configuration(config)
    metax_client = Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification')
    )

    for file_ in metax_client.get_dataset_files(dataset_id):
        # Get file information from Metax
        file_id = file_['identifier']
        # TODO: Could we fetch metadata of all files at once?
        file_metadata = metax_client.get_file(file_id)
        original_file_characteristics = file_metadata.get(
            "file_characteristics", {}
        )

        # Detect file using scraper. Use mimetype, charset, and version
        # defined by user.
        file_path = root_directory / file_['file_path'].strip('/')
        mimetype = original_file_characteristics.get("file_format", None)
        charset = original_file_characteristics.get("encoding", None)
        version = original_file_characteristics.get("format_version", None)
        scraper = file_scraper.scraper.Scraper(
            str(file_path), mimetype=mimetype, charset=charset, version=version
        )
        scraper.scrape(check_wellformed=False)

        # Create file_characteristics
        new_file_characteristics = {
            'file_format': scraper.mimetype,
        }
        if scraper.version != file_scraper.defaults.UNAP:
            new_file_characteristics['format_version'] = scraper.version
        if 'charset' in scraper.streams[0]:
            new_file_characteristics['encoding'] \
                = scraper.streams[0]['charset']

        # Scraper output will be saved to file_characteristics_extension
        # for later use
        file_characteristics_extension = {
            'streams': scraper.streams
        }

        del scraper

        # Merge generated file_characteristics with original data from
        # Metax. If a field was already defined in original data, it
        # will override the generated value.
        new_file_characteristics.update(original_file_characteristics)

        if '(:unav)' in new_file_characteristics.values():
            raise InvalidFileError("File format was not recognized",
                                   [file_id])

        metax_client.patch_file(
            file_id,
            {
                'file_characteristics': new_file_characteristics,
                'file_characteristics_extension':
                file_characteristics_extension
            }
        )
