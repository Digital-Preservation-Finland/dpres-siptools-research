"""IDA interface module"""
import os
import shutil

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


def download_file(identifier, linkpath, config_file):
    """Download file from IDA to workspace_root/ida_files and create a hard
    link to linkpath. Ida url, username, and password are read from
    configuration file.

    :param identifier: File identifier (for example "pid:urn:1")
    :param linkpath: Path where the hard link is created
    :param config_file: Configuration file
    :returns: ``None``
    """
    conf = Configuration(config_file)
    filepath = os.path.join(
        conf.get("workspace_root"), "ida_files", identifier
    )

    if not os.path.exists(filepath):
        response = _get_response(identifier, config_file, stream=True)

        with open(filepath, 'wb') as new_file:
            shutil.copyfileobj(response.raw, new_file)

    os.link(filepath, linkpath)
