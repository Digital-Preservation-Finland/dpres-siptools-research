"""Digital Preservation Service client."""
import configparser

from dpres_access_rest_api_client import AccessClient
from flask import current_app


def get_dps(contract):
    """Create a Digital Preservation Service client.

    :param contract: Contract identifier
    :returns: DPS client
    """
    config = configparser.ConfigParser()
    config.add_section('dpres')
    config['dpres']['api_host'] \
        = current_app.config["ACCESS_REST_API_HOST"]
    config['dpres']['username'] \
        = current_app.config["ACCESS_REST_API_USER"]
    config['dpres']['password'] \
        = current_app.config["ACCESS_REST_API_PASSWORD"]
    config['dpres']['contract_id'] = contract
    config['dpres']['verify_ssl'] \
        = str(current_app.config["ACCESS_REST_API_SSL_VERIFICATION"])

    return AccessClient(config=config)
