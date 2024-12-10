"""Generates metadata required to create SIP."""
import file_scraper
import file_scraper.scraper
from metax_access.response import MetaxFileCharacteristics

from siptools_research.exceptions import (InvalidFileError,
                                          InvalidFileMetadataError)
from siptools_research.metax import get_metax_client


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
            "characteristics") or {}

        # Detect file using scraper. Use metadata defined by user.
        file_path = root_directory / file_["pathname"].strip("/")
        mimetype = original_file_characteristics.get(
            "file_format_version", {}
        ).get("file_format")
        charset = original_file_characteristics.get("encoding")
        version = original_file_characteristics.get(
            "file_format_version", {}
        ).get("format_version")
        delimiter = original_file_characteristics.get("csv_delimiter")
        separator = original_file_characteristics.get("csv_record_separator")
        quotechar = original_file_characteristics.get("csv_quoting_char")
        scraper = file_scraper.scraper.Scraper(
            str(file_path),
            mimetype=mimetype,
            charset=charset,
            version=version,
            delimiter=delimiter,
            separator=separator,
            quotechar=quotechar,
        )
        scraper.scrape(check_wellformed=False)

        # Create new file_characteristics based on scraping results
        scraper_file_characteristics: MetaxFileCharacteristics = {
            "file_format_version": {
                "file_format": scraper.mimetype,
                "format_version": scraper.version
            },
            "encoding": scraper.streams[0].get("charset"),
            # TODO: ensure that user-defined csv paramters are not
            # overwritten!
            "csv_delimiter": scraper.streams[0].get("delimiter"),
            "csv_record_separator": scraper.streams[0].get("separator"),
            "csv_quoting_char": scraper.streams[0].get("quotechar"),
        }

        compare_file_chars = [
            (
                scraper_file_characteristics["file_format_version"],
                {
                    "file_format": mimetype,
                    "format_version": version
                }
            ),
            (
                scraper_file_characteristics,
                {
                    "encoding": charset,
                    "csv_delimiter": delimiter,
                    "csv_record_separator": separator,
                    "csv_quoting_char": quotechar
                }
            )
        ]

        # Check that user defined metadata is not ignored by
        # file-scraper
        # TODO: This is necessary until TPASPKT-381 is resolved
        for scraper_metadata, user_metadata in compare_file_chars:
            for key, value in user_metadata.items():
                if value is None:
                    continue
                if scraper_metadata[key] != value:
                    error_message = f"File scraper detects a different {key}"
                    raise InvalidFileMetadataError(error_message, [file_id])

            if "(:unav)" in scraper_metadata.values():
                raise InvalidFileError(
                    "File format was not recognized",
                    [file_id]
                )

            # Remove "(:unap)" and None values from scraper metadata
            keys_to_remove = [
                key for key, value in scraper_metadata.items()
                if value in (file_scraper.defaults.UNAP, None)
            ]
            for key in keys_to_remove:
                del scraper_metadata[key]

        # Scraper output will be saved to file_characteristics_extension
        # for later use
        file_characteristics_extension = {
            "streams": scraper.streams,
            "info": scraper.info,
            "mimetype": scraper.mimetype,
            "version": scraper.version,
            "grade": scraper.grade(),
        }

        metax_client.patch_file_characteristics(
            file_id,
            {
                "characteristics": scraper_file_characteristics,
                "characteristics_extension": file_characteristics_extension
             }
        )
