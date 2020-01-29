"""File validation tools."""
import os

from file_scraper.scraper import Scraper
from metax_access.metax import Metax

from requests.exceptions import HTTPError

from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from siptools_research.utils.ida import download_file, IdaError


def _download_files(dataset_id, config="/etc/siptools_research.conf"):
    """Download all dataset files from IDA

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: A list of the metadata of all downloaded files
    """
    conf = Configuration(config)
    pas_storage_id = conf.get("pas_storage_id")
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )

    dataset_files = metax_client.get_dataset_files(dataset_id)
    for dataset_file in dataset_files:
        identifier = dataset_file["identifier"]
        path = dataset_file["file_path"]
        if not dataset_file["file_storage"]["identifier"] == pas_storage_id:
            try:
                download_file(identifier, config_file=config)
            except (HTTPError, IdaError):
                raise FileValidationError(
                    "Could not download file '%s' from IDA" % path, []
                )


    return dataset_files


class FileValidationError(Exception):
    """Raised when file validation fails."""

    def __init__(self, message, paths):
        super(FileValidationError, self).__init__(message)
        self.paths = paths

    def __str__(self):
        message = super(FileValidationError, self).__str__()
        for path in self.paths:
            message += ("\n" + path)

        return message



def validate_files(dataset_id, config="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    :param dataset_id: dataset identifier
    :param config: configuration file path
    :returns: ``True`` if all files are well-formed.
    """
    conf = Configuration(config)
    storage_id = conf.get("pas_storage_id")
    mongo_files = Database(config).client.upload.files
    ida_path = os.path.join(conf.get("workspace_root"), "ida_files")
    errors = []

    dataset_files = _download_files(dataset_id, config=config)
    for dataset_file in dataset_files:
        identifier = dataset_file["identifier"]
        path = dataset_file["file_path"]
        file_chars = dataset_file["file_characteristics"]
        mimetype = file_chars["file_format"]
        encoding = file_chars.get("encoding", None)

        if dataset_file["file_storage"]["identifier"] == storage_id:
            mongo_file = mongo_files.find_one({"_id": identifier})
            if not mongo_file:
                raise FileValidationError(
                    "File '%s' not found in pre-ingest file storage" % path, []
                )
            filepath = mongo_file["file_path"]
        else:
            filepath = os.path.join(ida_path, identifier)

        scraper = Scraper(filepath, mimetype=mimetype, charset=encoding)
        scraper.scrape(check_wellformed=True)
        if not scraper.well_formed:
            errors.append(path)

        del scraper

    if errors:
        raise FileValidationError(
            "Following files are not well-formed:",
            errors
        )

    return True
