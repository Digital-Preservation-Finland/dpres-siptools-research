"""Commandline interface for siptools_research package.

To start the workflow for dataset 1234 (for example)::

   siptools_research preserve --config /etc/siptools_research.conf 1234

To generate metadata::

   siptools_research generate --config /etc/siptools_research.conf 1234

To validate metadata::

   siptools_research validate --config /etc/siptools_research.conf 1234
"""
from __future__ import print_function

import argparse
import json

from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflow_init import preserve_dataset
from siptools_research.metadata_validator import validate_metadata
from siptools_research.utils.database import Database


# ANSI escape sequences for different colors
SUCCESSC = '\033[92m'
WARNINGC = '\033[93m'
FAILC = '\033[91m'
ENDC = '\033[0m'


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
    get_parser.add_argument(
        '--workflow_id',
        help="Luigi workflow identifier"
    )

    status_parser = subparsers.add_parser(
        'status',
        help='Get workflow task results'
    )
    status_parser.set_defaults(func=_status)
    status_parser.add_argument(
        '--workflow_id',
        help="Luigi workflow identifier"
    )

    disable_parser = subparsers.add_parser(
        'disable',
        help='Disable workflow'
    )
    disable_parser.set_defaults(func=_disable)
    disable_parser.add_argument(
        '--workflow_id',
        help="Luigi workflow identifier"
    )

    enable_parser = subparsers.add_parser(
        'enable',
        help='Enable workflow'
    )
    enable_parser.set_defaults(func=_enable)
    enable_parser.add_argument(
        '--workflow_id',
        help="Luigi workflow identifier"
    )

    # Define arguments common to all commands
    parser.add_argument('dataset_id', help="Metax dataset identifier")
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


def _get_workflow_document(args):
    """Get a workflow document dict using workflow identifier if provided.
    Otherwise, use the dataset identifier.
    """
    dataset_id = args.dataset_id
    workflow_id = args.workflow_id
    database = Database(args.config)
    document = None

    # If workflow_id is provided, search using it
    if workflow_id is not None:
        document = database.get_one_workflow(workflow_id)
        if not document:
            print(
                FAILC +
                "Could not find document "
                "with workflow identifier: %s" % workflow_id +
                ENDC
            )

    # If no workflow_id is provided, search using the dataset_id
    else:
        documents = Database(args.config).get_workflows(dataset_id)
        count = documents.count()

        if count == 0:
            print(
                FAILC +
                "Could not find documents "
                "with dataset identifier: %s" % dataset_id +
                ENDC
            )
        elif count > 1:
            print(WARNINGC + "Found multiple matches:")
            for doc in documents:
                print(doc["_id"])
            print(ENDC, end="")
        else:
            documents = documents[0]

    return document


def _get(args):
    """Get a workflow document"""
    document = _get_workflow_document(args)
    if document:
        print(json.dumps(document, indent=4))


def _status(args):
    """Get workflow task results"""
    document = _get_workflow_document(args)
    if document:
        success = []
        fail = []

        for task in document["workflow_tasks"]:
            if document["workflow_tasks"][task]["result"] == "success":
                success.append(task)
            else:
                fail.append([task, document["workflow_tasks"][task]])

        # Print tasks that were completed successfully
        if success:
            print("Ran successfully:")
            print(SUCCESSC)
            for task in success:
                print(task)
            print(ENDC, end="")

        # Print tasks that failed
        if fail:
            print("\nFailed:")
            print(FAILC)
            for task in fail:
                print(json.dumps(task, indent=4))
            print(ENDC, end="")


def _disable(args):
    """Disable workflow"""
    document = _get_workflow_document(args)
    if document:
        database = Database(args.config)
        _id = document["_id"]
        database.set_disabled(_id)
        print("Workflow %s disabled" % _id)


def _enable(args):
    """Enable workflow"""
    document = _get_workflow_document(args)
    if document:
        database = Database(args.config)
        _id = document["_id"]
        database.set_enabled(_id)
        print("Workflow %s enabled" % _id)


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
