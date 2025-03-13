"""File validation tools."""
from file_scraper.defaults import BIT_LEVEL, BIT_LEVEL_WITH_RECOMMENDED
from file_scraper.scraper import Scraper

from siptools_research.exceptions import InvalidFileError
from siptools_research.metax import (CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL,
                                     get_metax_client)


def validate_files(dataset_id, root_directory,
                   config_file="/etc/siptools_research.conf"):
    """Validate all files in a dataset.

    Raises InvalidFileError if any of the files are invalid.

    :param dataset_id: dataset identifier
    :param root_directory: directory where files are found
    :param config: configuration file path
    """

    metax_client = get_metax_client(config_file)

    invalid_files = []
    for file in metax_client.get_dataset_files(dataset_id):

        filepath = root_directory / file["pathname"].strip('/')
        if not filepath.is_file():
            # Scraper won't raise exception if file is missing, so it is
            # better to raise exception here to avoid misleading error
            # messages.
            raise ValueError("Trying to validate file that does not "
                             "exist, or is not a file.")

        characteristics = file["characteristics"]

        # If this file is linked to a PAS compatible file, it must mean
        # this file is a bit-level file.
        is_linked_bitlevel = bool(file["pas_compatible_file"])

        # Map Metax V3 record separator to file-scraper format
        separator = CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL[
            characteristics.get("csv_record_separator")
        ]

        file_format_version = \
            characteristics.get("file_format_version", {}) or {}
        scraper = Scraper(
            filename=str(filepath),
            mimetype=file_format_version.get("file_format"),
            charset=characteristics.get("encoding"),
            version=file_format_version.get("format_version"),
            delimiter=characteristics.get("csv_delimiter"),
            quotechar=characteristics.get("csv_quoting_char"),
            separator=separator
        )
        scraper.scrape(check_wellformed=True)

        allowed_grades = [BIT_LEVEL_WITH_RECOMMENDED, BIT_LEVEL]

        if is_linked_bitlevel:
            # File is a bit-level file with a DPRES compatible counterpart.
            # 'ValidateMetadata' ensures that both must be included in the same
            # dataset, so allow this file through, even if it could be complete
            # garbage for all we know. ;)
            pass

        elif scraper.well_formed is True:
            # File is valid
            pass

        elif scraper.well_formed is None \
                and scraper.grade() in allowed_grades:
            # File was not validated, but it will be preserved only bit
            # level, so it is ok
            pass

        else:
            # File is invalid or could not be validated
            invalid_files.append(file["id"])

    if invalid_files:
        raise InvalidFileError(
            f"{len(invalid_files)} files are not well-formed",
            invalid_files
        )
