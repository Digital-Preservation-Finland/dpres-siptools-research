"""Metax interface class"""

import pprint                   # For printing dict
from json import loads, dumps   # For printing orderedDict
import coreapi
import sys


def pprint_ordereddict(input_ordered_dict):
    """Convert orderedDict to normal dict"""
    pprint.PrettyPrinter(indent=4).pprint(loads(dumps(input_ordered_dict)))

class Metax(object):
    """Metax interface class"""
    baseurl = "https://metax-test.csc.fi/rest/v1/"
    datasetsurl = "datasets/"

    def __init__(self):
        self.client = coreapi.Client()

    def get_dataset(self, dataset_id):
        """Get dataset with id"""
        return self.client.get(self.baseurl + self.datasetsurl + dataset_id)

    def get_contract(self, contract_id):
        """Get contract with id"""
        return self.client.get(self.baseurl + self.datasetsurl + contract_id)



def main(arg):
    """Test extracting some data from Metax"""
    metax = Metax()
    dataset = metax.get_dataset(arg[1])
    pprint_ordereddict(dataset)
    print '\nTitle:', dataset['research_dataset']['title'][0]['en']
    print '\nDescription:', dataset['research_dataset']['description'][0]['en']


if __name__ == "__main__":
    main(sys.argv)
