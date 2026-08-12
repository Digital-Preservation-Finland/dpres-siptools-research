"""Digital Preservation Service client."""
import configparser

from dpres_rest_api_client.v2.client import AccessClient
from dpres_rest_api_client.v3.client import RestClient
from siptools_research.config import Configuration


def _dps_config(contract, config):
    """Build configuration for a Digital Preservation Service client.

    :param contract: Contract identifier
    :param config: Path to configuration file
    :returns: ConfigParser object
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

    return dps_config


def get_dps(contract, config):
    """Create a Digital Preservation Service REST API v3 client.

    :param contract: Contract identifier
    :param config: Path to configuration file
    :returns: DPS client
    """
    return RestClient(config=_dps_config(contract, config))


def get_dps_v2(contract, config):
    """Create a Digital Preservation Service REST API v2 client.

    Used only for polling the status of transfers that were sent to
    DPS before SendSIPToDP was switched to use the REST API v3.

    :TODO: Remove once all datasets sent to DPS using the old
        SFTP/v2 based SendSIPToDP have been processed.

    :param contract: Contract identifier
    :param config: Path to configuration file
    :returns: DPS client
    """
    return AccessClient(config=_dps_config(contract, config))
