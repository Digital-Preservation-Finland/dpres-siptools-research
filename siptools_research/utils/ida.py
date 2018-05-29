"""IDA interface module"""
import tempfile
import requests
from siptools_research.config import Configuration


def _get_response(identifier, config_file, stream=False):
    conf = Configuration(config_file)
    user = conf.get('ida_user')
    password = conf.get('ida_password')
    baseurl = conf.get('ida_url')
    url = '%s/files/%s/download' % (baseurl, identifier)
    try:
        response = requests.get(url, auth=(user, password), verify=False,
                                stream=stream)
    except requests.exceptions.ConnectionError as exc:
        raise Exception("Could not connect to Ida: %s" % exc.message)
    # Raise error if file is not found
    if response.status_code == 404:
        raise Exception("File not found in Ida.")
    if response.status_code == 403:
        raise Exception("Access to file forbidden.")
    elif not response.status_code == 200:
        raise Exception("File could not be retrieved.")
    return response


def download_file(identifier, filepath, config_file):
    """Download file from IDA. Ida url, username, and password are read from
    configuration file.

    Function arguments:
    :identifier: File identifier (for example "pid:urn:1")
    :filepath: Path to save the file
    :config_file: Configuration file
    :returns: None
    """
    response = _get_response(identifier, config_file)

    with open(filepath, 'w') as new_file:
        new_file.write(response.content)


def download_file_header(identifier, config_file):
    """Downloads only the header of the file in IDA and creates a temporary
    file the caller should delete if not needed anymore. Ida url, username,
    and password are read from configuration file.

    Function arguments:
    :identifier: File identifier (for example "pid:urn:1")
    :config_file: Configuration file
    :returns: Path to the temp file containing the header of the file in IDA
    """
    response = _get_response(identifier, config_file, stream=True)

    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    tmp.write(chunk)
                    break
    finally:
        response.close()
    return tmp.name
