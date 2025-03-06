"""Module that provides metax client."""
from metax_access import Metax

from siptools_research.config import Configuration


# Mapping used for converting between file-scraper and Metax V3 representations
# of the CSV record separator
# TODO: This mismatch between Metax V3 and file-scraper is annoying and
# requires conversions in various places. Should we rather update Metax V3 to
# accept the literals instead?
CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL = {
    "CRLF": "\r\n",
    "LF": "\n",
    "CR": "\r",
    None: None
}
CSV_RECORD_SEPARATOR_LITERAL_TO_ENUM = {
    value: key for key, value in CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL.items()
}


def get_metax_client(config):
    """Initialize Metax client."""
    config_object = Configuration(config)
    return Metax(
        url=config_object.get("metax_url"),
        token=config_object.get("metax_token"),
        verify=config_object.getboolean("metax_ssl_verification"),
    )
