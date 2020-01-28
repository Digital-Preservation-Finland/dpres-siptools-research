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
        download_file(dataset_file["identifier"], config_file=config)

    return dataset_files


class FileValidationError(Exception):
    """Raised when file validation fails."""

    def __init__(self, message, identifiers):
        super(FileValidationError, self).__init__(message)
        self.identifiers = identifiers

    def __str__(self):
        message = super(FileValidationError, self).__str__() + "\n"
        for identifier in self.identifiers:
            message += (identifier + "\n")

        return message



def validate_files(dataset_id, config="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    conf = Configuration(config)
    ida_path = os.path.join(conf.get("workspace_root"), "ida_files")
    errors = []

    dataset_files = _download_files(dataset_id, config=config)
    for dataset_file in dataset_files:
        identifier = dataset_file["identifier"]
        filepath = os.path.join(ida_path, identifier)
        file_chars = dataset_file["file_characteristics"]
        mimetype = file_chars["file_format"]
        encoding = file_chars.get("encoding", None)

        scraper = Scraper(filepath, mimetype=mimetype, charset=encoding)
        scraper.scrape(check_wellformed=True)
        if not scraper.well_formed:
            errors.append(identifier)

        del scraper

    if errors:
        raise FileValidationError(
            "Following files are not well-formed:",
            errors
        )

    return True
