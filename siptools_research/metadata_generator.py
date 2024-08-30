"""Generates metadata required to create SIP."""

import file_scraper
import file_scraper.scraper
from siptools_research.metax import get_metax_client

from siptools_research.exceptions import InvalidFileError


def generate_metadata(
    dataset_id, root_directory, config="/etc/siptools_research.conf"
):
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
    metax_client = get_metax_client(config)

    for file_ in metax_client.get_dataset_files(dataset_id):
        # Get file information from Metax
        file_id = file_["id"]
        # TODO: Could we fetch metadata of all files at once?
        file_metadata = metax_client.get_file(file_id)
        original_file_characteristics = file_metadata.get(
            "characteristics", {}
        )

        # Detect file using scraper. Use mimetype, charset, and version
        # defined by user.
        file_path = root_directory / file_["pathname"].strip("/")
        mimetype = original_file_characteristics.get(
            "file_format_version", {}
        ).get("file_format", None)
        charset = original_file_characteristics.get("encoding", None)
        version = original_file_characteristics.get(
            "file_format_version", {}
        ).get("format_version", None)
        scraper = file_scraper.scraper.Scraper(
            str(file_path),
            mimetype=mimetype,
            charset=charset,
            version=version
        )
        scraper.scrape(check_wellformed=False)
        # This dict is in Metax V2 format as
        # patch does not support Metax V3 yet.
        # When normalizing path request change the format
        scraper_file_characteristics = {}

        if mimetype and mimetype != scraper.mimetype:
            raise InvalidFileError(
                "File scraper detects a different file type",
                [file_id]
            )
        scraper_file_characteristics['file_format'] = scraper.mimetype

        if "charset" in scraper.streams[0]:
            if charset and charset != scraper.streams[0]["charset"]:
                raise InvalidFileError(
                    "File scraper detects a different encoding type",
                    [file_id]
                )
            scraper_file_characteristics['encoding']\
                = scraper.streams[0]["charset"]

        if scraper.version != file_scraper.defaults.UNAP:
            if version and version != scraper.version:
                raise InvalidFileError(
                    "File scraper detects a different file version",
                    [file_id]
                )
            scraper_file_characteristics['format_version'] = scraper.version

        # Scraper output will be saved to file_characteristics_extension
        # for later use
        # TODO: Currently it is not possible to use previosly scraped
        # metadata to create METS with siptools-ng. Therefore,
        # file_characteristics_extension in Metax is useless.
        # Siptools-ng will scrape the file again in CreateMets task.
        # See TPASPKT-1326.
        file_characteristics_extension = {"streams": scraper.streams}

        if "(:unav)" in scraper_file_characteristics.values():
            raise InvalidFileError(
                "File format was not recognized",
                [file_id]
            )

        metax_client.patch_file(
            file_id,
            {
                "file_characteristics": scraper_file_characteristics,
                "file_characteristics_extension":
                    file_characteristics_extension
             }
        )
