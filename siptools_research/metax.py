"""Module that provides metax client."""
from metax_access import Metax

from siptools_research.config import Configuration


def get_metax_client(config):
    """Initialize Metax client."""
    config_object = Configuration(config)
    return Metax(
        config_object.get("metax_url"),
        user=config_object.get("metax_user"),
        # TODO: Password is only required for Metax API V2 support
        password=config_object.get("metax_password"),
        token=(
            # Only use tokens for V3. Tokens are not supported
            # for V2.
            config_object.get("metax_token")
            if config_object.get("metax_api_version") == "v3"
            else None
        ),
        verify=config_object.getboolean("metax_ssl_verification"),
        api_version=config_object.get("metax_api_version"),
    )
