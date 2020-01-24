"""File validation tools."""
import os

from file_scraper.scraper import Scraper
from metax_access.metax import Metax

from siptools_research.config import Configuration
from siptools_research.utils.ida import download_file


def _download_files(dataset_id, config="/etc/siptools_research.conf"):
    """Download all dataset files from IDA

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: A list of the metadata of all downloaded files
    """
    conf = Configuration(config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )

    dataset_files = metax_client.get_dataset_files(dataset_id)
    for dataset_file in dataset_files:
        download_file(dataset_file["identifier"], config)

    return dataset_files


class FileValidationError(Exception):
    """Raised when file validation fails."""
    pass


def validate_files(dataset_id, config="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    conf = Configuration(config)
    ida_path = os.path.join(conf.get("workspace_root"), "ida_files")

    dataset_files = _download_files(dataset_id, config=config)
    for dataset_file in dataset_files:
        identifier = dataset_file["identifier"]
        filepath = os.path.join(ida_path, identifier)
        file_chars = dataset_file["file_characteristics"]
        mimetype = file_chars["mimetype"]
        encoding = file_chars["encoding"]

        scraper = Scraper(filepath, mimetype=mimetype)
        scraper.scrape(check_wellformed=True)

        # TODO: Iterate over all files and raise FileValidationError at the end
        #       if any files were not valid.
        assert scraper.well_formed
        if scraper.streams[0]["stream_type"] == "text":
            assert scraper.streams[0]["charset"] == encoding

        del scraper

    return True
