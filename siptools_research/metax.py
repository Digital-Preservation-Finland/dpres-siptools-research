"""Metax interface class"""

import argparse
import pprint                   # For printing dict
from json import loads, dumps   # For printing orderedDict
import coreapi

METAX_ENTITIES = ['datasets', 'contracts', 'files']


def pprint_ordereddict(input_ordered_dict):
    """Convert orderedDict to normal dict"""
    pprint.PrettyPrinter(indent=4).pprint(loads(dumps(input_ordered_dict)))


class Metax(object):
    """Metax interface class."""
    baseurl = "https://metax-test.csc.fi/rest/v1/"

    def __init__(self):
        self.client = coreapi.Client()

    def get_data(self, entity_url, entity_id):
        """Get dataset with id"""
        url = self.baseurl + entity_url + '/' + entity_id
        return self.client.get(url)


def parse_arguments(arguments):
    """ Create arguments parser and return parsed
    command line arguments.
    """
    parser = argparse.ArgumentParser(description="Print dataset or contract "
                                     "information from Metax.")
    parser.add_argument('entity_url', type=str, choices=METAX_ENTITIES,
                        help='Entity url to be retrieved: %s' % METAX_ENTITIES)
    parser.add_argument('entity_id',
                        metavar='entity_id',
                        help='Print entity data with ID: entity_id')
    return parser.parse_args(arguments)


def main(arguments=None):
    """Print metadata from Metax"""

    args = parse_arguments(arguments)

    metax = Metax()

    dataset = metax.get_data(args.entity_url, args.entity_id)
    pprint_ordereddict(dataset)


if __name__ == "__main__":
    main()
