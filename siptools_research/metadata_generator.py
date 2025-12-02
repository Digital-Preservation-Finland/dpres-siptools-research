"""Generates metadata required to create SIP."""
import file_scraper
import file_scraper.scraper
from metax_access.response import MetaxFileCharacteristics

from siptools_research.database import connect_mongoengine
from siptools_research.exceptions import (InvalidFileError,
                                          InvalidFileMetadataError,
                                          file_error_collector)
from siptools_research.metax import (CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL,
                                     CSV_RECORD_SEPARATOR_LITERAL_TO_ENUM,
                                     get_metax_client)
from siptools_research.models.file_error import FileError
from pathlib import Path


def generate_metadata(
        dataset_id: str,
        root_directory: Path,
        config: str = "/etc/siptools_research.conf"
) -> None:
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

    dataset_files = metax_client.get_dataset_files(dataset_id)

    # Clear any existing file errors
    connect_mongoengine(config)
    FileError.objects.filter(
        file_id__in=[file["id"] for file in dataset_files],
        dataset_id__in=(None, dataset_id)
    ).delete()

    with file_error_collector() as collect_error:
        for file_metadata in dataset_files:
            # If this file is linked to a PAS compatible file, it must mean
            # this file is a bit-level file. If so, be lenient during
            # file metadata generation and allow scraping failures to pass.
            is_linked_bitlevel = bool(file_metadata["pas_compatible_file"])

            original_file_characteristics = file_metadata["characteristics"]

            # Detect file using scraper. Use metadata defined by user.
            file_path = root_directory / file_metadata["pathname"].strip("/")
            file_format_version \
                = original_file_characteristics["file_format_version"]
            mimetype = file_format_version["file_format"]
            version = file_format_version["format_version"]
            charset = original_file_characteristics["encoding"]
            delimiter = original_file_characteristics["csv_delimiter"]
            separator = original_file_characteristics["csv_record_separator"]
            quotechar = original_file_characteristics["csv_quoting_char"]
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

            # Check that scraping succeed
            # TODO: Why scraper does not raise Exception?
            if scraper.well_formed is False and not is_linked_bitlevel:
                # TODO: There probably should be easier way to get the
                # error messages from Scraper. It could be implemented
                # in TPASPKT-1622
                for info in scraper.info.values():
                    for error in info["errors"]:
                        collect_error(InvalidFileError(
                            f"{info['class']}: {error}",
                            [file_metadata]
                        ))
                continue

            scraper_file_characteristics: MetaxFileCharacteristics = {
                "file_format_version": None,
                "encoding": scraper.streams[0].get("charset"),
                "csv_delimiter": scraper.streams[0].get("delimiter"),
                "csv_record_separator": scraper.streams[0].get("separator"),
                "csv_quoting_char": scraper.streams[0].get("quotechar"),
            }

            try:
                url = reference_data[(scraper.mimetype, scraper.version)]
                scraper_file_characteristics["file_format_version"] = {
                    "url": url
                }
            except KeyError:
                # Bit-level file formats don't need to be recognized by Metax
                # V3; we will store possible file format metadata in
                # 'file_characteristics_extension'.
                if not is_linked_bitlevel:
                    message = (
                        f"Detected file_format: '{scraper.mimetype}' and "
                        f"format_version: '{scraper.version}' are not "
                        "supported."
                    )
                    collect_error(
                        InvalidFileError(message, [file_metadata])
                    )
                    continue

            # Check that user defined metadata is not ignored by
            # file-scraper
            # TODO: This is necessary until file-scraper does not ignore
            # predefined metadata (See TPASPKT-1621). The problem has
            # already been fixed for file_format and format_version.
            compare_file_chars = [
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
                    collect_error(
                        InvalidFileMetadataError(error, [file_metadata])
                    )

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
