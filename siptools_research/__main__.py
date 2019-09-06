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

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')

    generate_parser = subparsers.add_parser(
        'generate', help='generate technical metadata for the dataset'
    )
    generate_parser.set_defaults(func=_generate)

    validate_parser = subparsers.add_parser(
        'validate', help='validate dataset metadata'
    )
    validate_parser.set_defaults(func=_validate)

    preserve_parser = subparsers.add_parser(
        'preserve', help='start preservation workflow'
    )
    preserve_parser.set_defaults(func=_preserve)

    get_parser = subparsers.add_parser(
        'get',
        help='Get a workflow document'
    )
    get_parser.set_defaults(func=_get)

    status_parser = subparsers.add_parser(
        'status',
        help='Get workflow task results'
    )
    status_parser.set_defaults(func=_status)

    disable_parser = subparsers.add_parser(
        'disable',
        help='Disable workflow'
    )
    disable_parser.set_defaults(func=_disable)

    enable_parser = subparsers.add_parser(
        'enable',
        help='Enable workflow'
    )
    enable_parser.set_defaults(func=_enable)

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


def _generate(args):
    """Generate technical metadata for the dataset"""
    generate_metadata(args.dataset_id, args.config)


def _validate(args):
    """Validate dataset metadata"""
    validate_metadata(args.dataset_id, args.config)


def _preserve(args):
    """Start preservation workflow"""
    preserve_dataset(args.dataset_id, args.config)


def _get(args):
    """Get a workflow document"""
    pass


def _status(args):
    """Get workflow task results"""
    pass


def _disable(args):
    """Disable workflow"""
    pass


def _enable(args):
    """Enable workflow"""
    pass


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
