"""Metax interface class."""

import argparse
import pprint                   # For printing dict
from json import loads, dumps   # For printing orderedDict
import getpass
import requests
from requests.auth import HTTPBasicAuth
import coreapi
import lxml.etree as ET

METAX_ENTITIES = ['datasets', 'contracts', 'files']
PRINT_OUTPUT = ['json', 'xml', 'string']
# TODO: Store username and password somewhere else
USER = 'tpas'


def print_output(dataset, print_output=None):
    """Print dataset as json, xml or string"""
    if print_output == 'json':
        pprint.PrettyPrinter(indent=4).pprint(loads(dumps(dataset)))
    elif print_output == 'xml':
        tree = ET.parse(dataset)
        root = tree.getroot()
        print ET.tostring(root)
    else:
        print dataset


class Metax(object):
    """Get metadata from metax as OrderedDict object."""
    baseurl = "https://metax-test.csc.fi/rest/v1/"

    def __init__(self):
        self.client = coreapi.Client()

    def get_data(self, entity_url, entity_id):
        """Get metadata of dataset, contract or file with id from Metax.
        :entity_url: "datasets", "contracts" or "files"
        :entity_id: ID number of object
        :returns: OrderedDict"""
        url = self.baseurl + entity_url + '/' + entity_id
        return self.client.get(url)

    def set_preservation_state(self, dataset_id, state):
        """Set value of field `preservation_state` for dataset in Metax

        :dataset_id: The ID of dataset in Metax
        :state: The value for `preservation_state`
        :returns: None

        """
        # TODO: To avoid saving Metax password to repository, it is prompted
        # here (just for testing purposes)
        password = getpass.getpass()

        url = self.baseurl + 'datasets/' + dataset_id
        data = {"id": dataset_id, "preservation_state": state}
        request = requests.patch(
            url,
            json=data,
            auth=HTTPBasicAuth(USER, password)
        )

        # Raise exception if request fails
        assert request.status_code == 200


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
    return parser.parse_args(arguments)


def main(arguments=None):
    """Print metadata from Metax"""

    args = parse_arguments(arguments)

    metax = Metax()

    dataset = metax.get_data(args.entity_url, args.entity_id)
    print_output(dataset,args.print_output)


if __name__ == "__main__":
    main()
