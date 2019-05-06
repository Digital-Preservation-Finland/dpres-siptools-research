"""IDA interface module"""
import requests
from siptools_research.config import Configuration


def _get_response(identifier, config_file, stream=False):
    """Send authenticated HTTP request to IDA.

    :param identifier: File identifier
    :param config_file: path to configuration file
    :param stream (bool): Stream the request content
    :returns: requests Response
    """
    conf = Configuration(config_file)
    user = conf.get('ida_user')
    password = conf.get('ida_password')
    baseurl = conf.get('ida_url')
    url = '%s/files/%s/download' % (baseurl, identifier)

    try:
        response = requests.get(url,
                                auth=(user, password),
                                verify=False,
                                stream=stream)
    except requests.exceptions.ConnectionError as exc:
        raise Exception("Could not connect to Ida: %s" % exc.message)

    response.raise_for_status()
    return response


def download_file(identifier, tmpfilepath, config_file):
    """Download file from IDA. Ida url, username, and password are read from
    configuration file.

    :param identifier: File identifier (for example "pid:urn:1")
    :param tmpfilepath: Path to save the file
    :param config_file: Configuration file
    :returns: ``None``
    """
    response = _get_response(identifier, config_file)

    with open(tmpfilepath, 'w') as new_file:
        new_file.write(response.content)
