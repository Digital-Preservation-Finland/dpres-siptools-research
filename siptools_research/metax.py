"""Module that provides metax client."""
from metax_access import Metax

from siptools_research.config import Configuration


def get_metax_client(config):
    """Initialize Metax client."""
    config_object = Configuration(config)
    return Metax(
        url=config_object.get("metax_url"),
        token=config_object.get("metax_token"),
        verify=config_object.getboolean("metax_ssl_verification"),
    )
