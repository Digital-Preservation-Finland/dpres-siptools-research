"""IDA interface"""
import requests

BASEURL = 'https://86.50.169.61:4433'
USER = 'testuser_1'
PASSWORD = 'testuser'

def download_file(identifier, filepath, user=USER, password=PASSWORD):
    """Download file from IDA.

    :identifier: File identifier (for example "pid:urn:1")
    :filepath: Path to save the file
    :user: Username for authentication
    :password: Password for authentication
    :returns: None
    """

    url = '%s/files/%s/download' % (BASEURL, identifier)
    response = requests.get(url, auth=(user, password), verify=False)
    with open(filepath, 'w') as new_file:
        new_file.write(response.content)
