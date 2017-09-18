"""Metax interface class"""

import argparse
import pprint                   # For printing dict
from json import loads, dumps   # For printing orderedDict
import coreapi


def pprint_ordereddict(input_ordered_dict):
    """Convert orderedDict to normal dict"""
    pprint.PrettyPrinter(indent=4).pprint(loads(dumps(input_ordered_dict)))


class Metax(object):
    """Metax interface class"""
    baseurl = "https://metax-test.csc.fi/rest/v1/"
    datasetsurl = "datasets/"
    contractsurl = "contracts/"

    def __init__(self):
        self.client = coreapi.Client()

    def get_dataset(self, dataset_id):
        """Get dataset with id"""
        return self.client.get(self.baseurl + self.datasetsurl + dataset_id)

    def get_contract(self, contract_id):
        """Get contract with id"""
        return self.client.get(self.baseurl + self.contractsurl + contract_id)


def main():
    """Print metadata from Metax"""

    parser = argparse.ArgumentParser(description="Print dataset or contract "
                                     "information from Metax.")
    parser.add_argument('--dataset',
                        metavar='dataset_id',
                        help='Print dataset with ID: dataset_id')
    parser.add_argument('--contract',
                        metavar='contract_id',
                        help='Print contract with ID: contract_id')
    args = vars(parser.parse_args())

    metax = Metax()

    if args['dataset'] and not args['contract']:
        dataset = metax.get_dataset(args['dataset'])
        pprint_ordereddict(dataset)
    elif args['contract'] and not args['dataset']:
        dataset = metax.get_dataset(args['contract'])
        pprint_ordereddict(dataset)
    elif args['contract'] and args['dataset']:
        print '--contract and  --dataset can not be user together.'
    else:
        print 'No dataset ID or contract ID given.'


if __name__ == "__main__":
    main()
