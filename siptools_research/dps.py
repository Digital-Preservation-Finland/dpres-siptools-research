"""Digital Preservation Service client."""
import configparser

from dpres_rest_api_client import AccessClient
from siptools_research.config import Configuration


def get_dps(contract, config):
    """Create a Digital Preservation Service client.

    :param contract: Contract identifier
    :returns: DPS client
    """
    dps_config = configparser.ConfigParser()
    config_object = Configuration(config)
    dps_config.add_section('dpres')
    dps_config['dpres']['api_host'] \
        = config_object.get("access_rest_api_host")
    dps_config['dpres']['username'] \
        = config_object.get("access_rest_api_user")
    dps_config['dpres']['password'] \
        = config_object.get("access_rest_api_password")
    dps_config['dpres']['contract_id'] = contract
    dps_config['dpres']['verify_ssl'] \
        = str(config_object.get("access_rest_api_ssl_verification"))

    return AccessClient(config=dps_config)
