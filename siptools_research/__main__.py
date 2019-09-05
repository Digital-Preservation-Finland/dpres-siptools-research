"""Commandline interface for siptools_research package.

To start the workflow for dataset 1234 (for example)::

   siptools_research preserve --config /etc/siptools_research.conf 1234

To generate metadata::

   siptools_research generate --config /etc/siptools_research.conf 1234

To validate metadata::

   siptools_research validate --config /etc/siptools_research.conf 1234
"""

import argparse
from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflow_init import preserve_dataset
from siptools_research.metadata_validator import validate_metadata


def _parse_args():
    """Parse command line arguments.

    :returns: Parsed arguments
    """
    # Parse commandline arguments
    parser = argparse.ArgumentParser(
        description='Generate techincal metadata for a dataset in Metax, '
                    'validate Metax dataset metadata, or start digital '
                    'preservation workflow a Metax dataset.'
    )

    # Add three alternative commands. The command defines which function will
    # called.
    subparsers = parser.add_subparsers(title='Available commands')
    generate_parser = subparsers.add_parser(
        'generate', help='generate technical metadata for the dataset'
    )
    generate_parser.set_defaults(func=generate_metadata)
    validate_parser = subparsers.add_parser(
        'validate', help='validate dataset metadata'
    )
    validate_parser.set_defaults(func=validate_metadata)
    preserve_parser = subparsers.add_parser(
        'preserve', help='start preservation workflow'
    )
    preserve_parser.set_defaults(func=preserve_dataset)

    subparsers.add_parser(
        'get',
        help='Get a workflow document'
    )
    subparsers.add_parser(
        'status',
        help='Get workflow task results'
    )
    subparsers.add_parser(
        'disable',
        help='Disable workflow'
    )
    subparsers.add_parser(
        'enable',
        help='Enable workflow'
    )

    # Define arguments common to all commands
    parser.add_argument('dataset_id', help="Metax dataset identifier")
    parser.add_argument('--workflow_id', help="Luigi workflow identifier")
    parser.add_argument(
        '--config',
        default='/etc/siptools_research.conf',
        metavar='config_file',
        help="path to configuration file"
    )

    # Parse arguments and return the arguments
    return parser.parse_args()


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args.dataset_id, args.config)


if __name__ == '__main__':
    main()
