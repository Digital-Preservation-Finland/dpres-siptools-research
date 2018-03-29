"""Metax interface class."""

import logging
import requests
from requests.auth import HTTPBasicAuth
import lxml.etree
from siptools_research.config import Configuration


# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_ENTITIES = ['datasets', 'contracts', 'files']
PRINT_OUTPUT = ['json', 'xml', 'string']


class MetaxConnectionError(Exception):
    """Exception raised when Metax is not available"""
    message = 'No connection to Metax'


class Metax(object):
    """Get metadata from metax as dict object."""

    def __init__(self, config_file):
        """ Initialize Metax object.

        :config_file: Global configuration file
        """
        configuration = Configuration(config_file)
        self.metax_url = configuration.get('metax_url')
        self.username = configuration.get('metax_user')
        self.password = configuration.get('metax_password')
        self.baseurl = self.metax_url + '/rest/v1/'
        self.elasticsearch_url = self.metax_url + '/es/'

    def get_data(self, entity_url, entity_id):
        """Get metadata of dataset, contract or file with id from Metax.

        :entity_url: "datasets", "contracts" or "files"
        :entity_id: ID number of object
        :returns: dict"""
        url = self.baseurl + entity_url + '/' + entity_id

        response = _do_get_request(url)

        if not response.status_code == 200:
            raise Exception("Could not find metadata.")
        return response.json()

    def get_xml(self, entity_url, entity_id):
        """Get xml data of dataset, contract or file with id from Metax.

        :entity_url: "datasets", "contracts" or "files"
        :entity_id: ID number of object
        :returns: dict with XML namespace strings as keys and
                  lxml.etree.ElementTree objects as values
        """
        # Init result dict
        xml_dict = {}

        # Get list of xml namespaces
        ns_key_url = self.baseurl + entity_url + '/' + entity_id + '/xml'
        response = _do_get_request(ns_key_url)
        if not response.status_code == 200:
            raise Exception("Could not retrieve list of additional metadata "\
                            "XML for dataset %s: %s" % (entity_id, ns_key_url))
        ns_key_list = requests.get(ns_key_url).json()

        # For each listed namespace, download the xml, create ElementTree, and
        # add it to result dict
        for ns_key in ns_key_list:
            query = '?namespace=' + ns_key
            response = _do_get_request(ns_key_url + query)
            if not response.status_code == 200:
                raise Exception("Could not retrieve additional metadata XML "\
                                "for dataset %s: %s" % (entity_id,
                                                        ns_key_url + query))
            # pylint: disable=no-member
            xml_dict[ns_key] = lxml.etree.fromstring(response.content)\
                .getroottree()

        return xml_dict

    def get_elasticsearchdata(self):
        """Get elastic search data from Metax

        :returns: dict"""
        url = self.elasticsearch_url + "reference_data/use_category/_search?"\
                                       "pretty&size=100"
        response = _do_get_request(url)

        if not response.status_code == 200:
            raise Exception("Could not find elastic search data.")

        return response.json()

    def set_preservation_state(self, dataset_id, state, description):
        """Set values of attributes `preservation_state` and
        `preservation_state_description` for dataset in Metax

        :dataset_id: The ID of dataset in Metax
        :state (integer): The value for `preservation_state`
        :description (string): The value for `preservation_system_description`
        :returns: None

        """
        url = self.baseurl + 'datasets/' + dataset_id
        data = {"preservation_state": state,
                "preservation_state_description": description}
        request = requests.patch(
            url,
            json=data,
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if request.status_code == 503:
            raise MetaxConnectionError

        # Raise exception if request fails
        assert request.status_code == 200

    def get_datacite(self, dataset_id):
        """Get descriptive metadata in datacite xml format.

        :dataset_id: ID of dataset
        :returns: Datacite XML (lxml.etree.ElementTree object)
        """
        url = "%sdatasets/%s?dataset_format=datacite" % (self.baseurl,
                                                         dataset_id)
        response = _do_get_request(url)

        if not response.status_code == 200:
            raise Exception("Could not find descriptive metadata.")

        # pylint: disable=no-member
        return lxml.etree.fromstring(response.content).getroottree()

    def get_dataset_files(self, dataset_id):
        """Get file metadata of dataset from Metax.

        :dataset_id: ID number of object
        :returns: dict"""
        url = self.baseurl + 'datasets/' + dataset_id + '/files'

        response = _do_get_request(url)

        if not response.status_code == 200:
            raise Exception("Could not find dataset files metadata.")

        return response.json()


def _do_get_request(url):
    """Wrapper function for requests.get() function. Raises
    MetaxConnectionError if status code is 503 (Service unavailable

    :returns: requests response
    """
    response = requests.get(url)
    if response.status_code == 503:
        raise MetaxConnectionError
    return response
