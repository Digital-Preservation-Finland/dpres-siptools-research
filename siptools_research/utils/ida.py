"""IDA interface module"""
import requests
from siptools_research.config import Configuration

def download_file(identifier, filepath, config_file):
    """Download file from IDA. Ida url, username, and password are read from
    configuration file.

    Function arguments:
    :identifier: File identifier (for example "pid:urn:1")
    :filepath: Path to save the file
    :config_file: Configuration file
    :returns: None
    """
    conf = Configuration(config_file)
    user = conf.get('ida_user')
    password = conf.get('ida_password')
    baseurl = conf.get('ida_url')

    url = '%s/files/%s/download' % (baseurl, identifier)
    response = requests.get(url, auth=(user, password), verify=False)

    # Raise error if file is not found
    if response.status_code == 404:
        raise Exception("File not found in Ida.")
    if response.status_code == 403:
        raise Exception("Access to file forbidden.")
    elif not response.status_code == 200:
        raise Exception("File could not be retrieved.")

    with open(filepath, 'w') as new_file:
        new_file.write(response.content)
