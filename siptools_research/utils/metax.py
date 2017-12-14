"""Metax interface class."""

import argparse
import pprint                   # For printing dict
import logging
import requests
from requests.auth import HTTPBasicAuth
import lxml.etree
from siptools_research.config import Configuration


# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

METAX_ENTITIES = ['datasets', 'contracts', 'files']
PRINT_OUTPUT = ['json', 'xml', 'string']


def print_output(dataset, output_type=None):
    """Print dataset as json, xml or string"""
    if output_type == 'json':
        pprint.pprint(dataset)
    elif output_type == 'xml':
        # pylint: disable=no-member
        tree = lxml.etree.parse(dataset)
        root = tree.getroot()
        print lxml.etree.tostring(root)
    else:
        print dataset


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

        return requests.get(url).json()

    def get_xml(self, entity_url, entity_id):
        """Get xml data of dataset, contract or file with id from Metax.

        :entity_url: "datasets", "contracts" or "files"
        :entity_id: ID number of object
        :returns: dict with xml namespaces as keys and lxml.etree.ElementTree
                  objects as values
        """
        # Init result dict
        xml_dict = {}

        # Get list of xml namespaces
        ns_key_url = self.baseurl + entity_url + '/' + entity_id + '/xml'
        ns_key_list = requests.get(ns_key_url).json()

        # For each listed namespace, download the xml, create ElementTree, and
        # add it to result dict
        for ns_key in ns_key_list:
            query = '?namespace=' + ns_key
            response = requests.get(ns_key_url + query)
            # pylint: disable=no-member
            xml_dict[ns_key] = lxml.etree.fromstring(response.content)\
                .getroottree()

        return xml_dict

    def get_elasticsearchdata(self):
        """Get elastic search data from Metax

        :returns: dict"""
        url = self.elasticsearch_url + "reference_data/use_category/_search?"\
                                       "pretty&size=100"
        return requests.get(url).json()

    def set_preservation_state(self, dataset_id, state):
        """Set value of field `preservation_state` for dataset in Metax

        :dataset_id: The ID of dataset in Metax
        :state: The value for `preservation_state`
        :returns: None

        """
        url = self.baseurl + 'datasets/' + dataset_id
        data = {"id": dataset_id, "preservation_state": state}
        request = requests.patch(
            url,
            json=data,
            auth=HTTPBasicAuth(self.username, self.password)
        )

        # Raise exception if request fails
        assert request.status_code == 200

    def get_datacite(self, dataset_id):
        """Get descriptive metadata in datacite xml format.

        :dataset_id: ID of dataset
        :returns: Datacite XML (lxml.etree.ElementTree object)
        """
        url = "%sdatasets/%s?dataset_format=datacite" % (self.baseurl,
                                                         dataset_id)
        response = requests.get(url)
        # pylint: disable=no-member
        return lxml.etree.fromstring(response.content).getroottree()


def parse_arguments(arguments):
    """Create arguments parser and return parsed
    command line arguments.
    """
    parser = argparse.ArgumentParser(description="Print dataset, contract, or "
                                     "file metada from Metax in pretty "
                                     "format.")
    parser.add_argument('entity_url', type=str, choices=METAX_ENTITIES,
                        help='Entity url to be retrieved')
    parser.add_argument('entity_id',
                        metavar='entity_id',
                        help='Entity ID')
    parser.add_argument('--print_output',
                        metavar='print_output', default='json',
                        help='print output as json/xml/string')
    parser.add_argument('--config', required=True, help='Configuration file')
    return parser.parse_args(arguments)


def main(arguments=None):
    """Print metadata from Metax"""

    args = parse_arguments(arguments)

    metax = Metax(args.config)

    dataset = metax.get_data(args.entity_url, args.entity_id)
    print_output(dataset, args.print_output)


if __name__ == "__main__":
    main()
