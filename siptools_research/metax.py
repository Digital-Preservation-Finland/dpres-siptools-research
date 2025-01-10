"""Module that provides metax client."""
from metax_access import Metax

from siptools_research.config import Configuration


def get_metax_client(config):
    """Initialize Metax client."""
    config_object = Configuration(config)
    return Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification'),
        api_version=config_object.get('metax_api_version'),
    )
