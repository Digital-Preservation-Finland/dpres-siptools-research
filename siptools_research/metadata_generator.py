"""Generates metadata required to create SIP."""
import file_scraper
import file_scraper.scraper
from metax_access.response import MetaxFileCharacteristics

from siptools_research.exceptions import (InvalidFileError,
                                          InvalidFileMetadataError)
from siptools_research.metax import (CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL,
                                     CSV_RECORD_SEPARATOR_LITERAL_TO_ENUM,
                                     get_metax_client)


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

    # Map file_format and format_version to url
    reference_data = {
        (
            entry["file_format"],
            # Empty string as format_version seems to mean "(:unap)"
            entry["format_version"] or file_scraper.defaults.UNAP
        ): entry["url"]
        for entry in metax_client.get_file_format_versions()
    }

    for file_metadata in metax_client.get_dataset_files(dataset_id):
        # If this file is linked to a PAS compatible file, it must mean
        # this file is a bit-level file. If so, be lenient during
        # file metadata generation and allow scraping failures to pass.
        is_linked_bitlevel = bool(file_metadata["pas_compatible_file"])

        original_file_characteristics = file_metadata.get(
            "characteristics") or {}

        # Detect file using scraper. Use metadata defined by user.
        file_path = root_directory / file_metadata["pathname"].strip("/")
        file_format_version \
            = original_file_characteristics.get("file_format_version") or {}
        mimetype = file_format_version.get("file_format")
        version = file_format_version.get("format_version")
        charset = original_file_characteristics.get("encoding")
        delimiter = original_file_characteristics.get("csv_delimiter")
        separator = original_file_characteristics.get("csv_record_separator")
        quotechar = original_file_characteristics.get("csv_quoting_char")
        scraper = file_scraper.scraper.Scraper(
            str(file_path),
            mimetype=mimetype,
            charset=charset,
            version=version,
            delimiter=delimiter,
            separator=CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL[separator],
            quotechar=quotechar,
        )
        scraper.scrape(check_wellformed=False)

        scraper_file_characteristics: MetaxFileCharacteristics = {
            "file_format_version": None,
            "encoding": scraper.streams[0].get("charset"),
            "csv_delimiter": scraper.streams[0].get("delimiter"),
            "csv_record_separator": scraper.streams[0].get("separator"),
            "csv_quoting_char": scraper.streams[0].get("quotechar"),
        }

        is_unrecognized = \
            "(:unav)" in (scraper.mimetype, scraper.version)
        if is_unrecognized and not is_linked_bitlevel:
            # TODO: We probably should check all metadata that is required
            # for packaging, but we can not check all fields in
            # scraper.streams, because some of them will be "(:unav)" and it
            # seems to be OK (or is there a bug in file-scraper,
            # siptools-ng, or sample files of this repository?)
            error = "File format was not recognized"
            raise InvalidFileError(error, [file_metadata])

        try:
            url = reference_data[(scraper.mimetype, scraper.version)]
            scraper_file_characteristics["file_format_version"] = {
                "url": url
            }
        except KeyError as error:
            # Bit-level file formats don't need to be recognized by Metax V3;
            # we will store possible file format metadata in
            # 'file_characteristics_extension'.
            if not is_linked_bitlevel:
                message = (f"Detected file_format: '{scraper.mimetype}' and "
                           f"format_version: '{scraper.version}' are not "
                           "supported.")
                raise InvalidFileError(message, [file_metadata]) from error

        # Check that user defined metadata is not ignored by
        # file-scraper
        # TODO: This is necessary until TPASPKT-1418 is resolved
        compare_file_chars = [
            (
                "file_format",
                scraper.mimetype,
                mimetype,
            ),
            (
                "format_version",
                scraper.version,
                version,
            ),
            (
                "encoding",
                scraper_file_characteristics["encoding"],
                charset,
            ),
            (
                "csv_delimiter",
                scraper_file_characteristics["csv_delimiter"],
                delimiter,
            ),
            (
                "csv_record_separator",
                scraper_file_characteristics["csv_record_separator"],
                CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL[separator],
            ),
            (
                "csv_quoting_char",
                scraper_file_characteristics["csv_quoting_char"],
                quotechar,
            )
        ]
        for key, scraper_metadata, user_metadata in compare_file_chars:
            if user_metadata and scraper_metadata != user_metadata:
                error = (f"File scraper detects a different {key}: "
                         f"{scraper_metadata}")
                raise InvalidFileMetadataError(error, [file_metadata])

        # Map 'csv_record_separator' to the enum value expected by Metax V3
        scraper_file_characteristics["csv_record_separator"] = (
            CSV_RECORD_SEPARATOR_LITERAL_TO_ENUM[
                scraper_file_characteristics["csv_record_separator"]
            ]
        )

        # Remove "(:unap)" and None values from
        # scraper_file_characteristics
        scraper_file_characteristics = {
            key: value
            for key, value
            in scraper_file_characteristics.items()
            if value not in (file_scraper.defaults.UNAP, None)
        }

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
            file_metadata["id"],
            {
                "characteristics": scraper_file_characteristics,
                "characteristics_extension": file_characteristics_extension
             }
        )
