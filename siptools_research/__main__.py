"""Commandline interface to start the workflow.

To start the workflow for dataset 1234 (for example)::

   siptools_research --config /etc/siptools_research.conf 1234
"""

import argparse
from siptools_research.preserve_dataset import preserve_dataset
from siptools_research.validate_metadata import validate_metadata

def main():
    """Parse command line arguments and start the workflow.

    :returns: None
    """

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Send to dataset to digital'\
                                     'preservation service.')
    parser.add_argument('dataset_id', help="Metax dataset identifier")
    parser.add_argument('--config', default='/etc/siptools_research.conf',
                        help="Path to configuration file")
    parser.add_argument('--validate', action='store_true',
                        help="Just validate the dataset metadata")
    args = parser.parse_args()

    if args.validate:
        validate_metadata(args.dataset_id, args.config)
    else:
        preserve_dataset(args.dataset_id, args.config)


if __name__ == '__main__':
    main()

# TODO: There is not yet (2017-11-30) tests for this module.
