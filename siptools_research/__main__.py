"""Commandline interface for siptools_research package.

To start the workflow for dataset 1234 (for example)::

   siptools-research preserve 1234

To generate metadata::

   siptools-research generate 1234

To validate metadata::

   siptools-research validate 1234

MongoDB can be queried and updated with commands: get, status, tasks,
workflows, disable and enable. For further information run::

   siptools-research --help

and::

   siptools-research command --help

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
FAILC = '\033[91m'
ENDC = '\033[0m'


def _parse_args():
    """Parse command line arguments.

    :returns: Parsed arguments
    """
    # Parse commandline arguments
    parser = argparse.ArgumentParser(
        description='Generate technical metadata for a dataset in Metax, '
                    'validate Metax dataset metadata, start digital '
                    'preservation workflow a Metax dataset, or query/edit '
                    'workflow documents in MongoDB.'
    )

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')
    _setup_generate_args(subparsers)
    _setup_validate_args(subparsers)
    _setup_preserve_args(subparsers)
    _setup_get_args(subparsers)
    _setup_status_args(subparsers)
    _setup_tasks_args(subparsers)
    _setup_workflows_args(subparsers)
    _setup_disable_args(subparsers)
    _setup_enable_args(subparsers)

    # Define arguments common to all commands
    parser.add_argument(
        '--config',
        default='/etc/siptools_research.conf',
        metavar='config_file',
        help="path to configuration file"
    )

    # Parse arguments and return the arguments
    return parser.parse_args()


def _setup_generate_args(subparsers):
    """Define generate subparser and its arguments."""
    generate_parser = subparsers.add_parser(
        'generate', help='Generate technical metadata for the dataset'
    )
    generate_parser.set_defaults(func=_generate)
    generate_parser.add_argument('dataset_id', help="Metax dataset identifier")


def _setup_validate_args(subparsers):
    """Define validate subparser and its arguments."""
    validate_parser = subparsers.add_parser(
        'validate', help='Validate dataset metadata'
    )
    validate_parser.set_defaults(func=_validate)
    validate_parser.add_argument('dataset_id', help="Metax dataset identifier")


def _setup_preserve_args(subparsers):
    """Define preserve subparser and its arguments."""
    preserve_parser = subparsers.add_parser(
        'preserve', help='Start preservation workflow'
    )
    preserve_parser.set_defaults(func=_preserve)
    preserve_parser.add_argument('dataset_id', help="Metax dataset identifier")


def _setup_get_args(subparsers):
    """Define get subparser and its arguments."""
    get_parser = subparsers.add_parser(
        'get',
        help='Get a workflow document'
    )
    get_parser.set_defaults(func=_get)
    get_parser.add_argument(
        'workflow_id',
        help="Luigi workflow identifier"
    )


def _setup_status_args(subparsers):
    """Define status subparser and its arguments."""
    status_parser = subparsers.add_parser(
        'status',
        help='Get workflow status'
    )
    status_parser.set_defaults(func=_status)
    status_parser.add_argument(
        'workflow_id',
        help="Luigi workflow identifier"
    )


def _setup_tasks_args(subparsers):
    """Define tasks subparser and its arguments."""
    status_parser = subparsers.add_parser(
        'tasks',
        help='Get workflow task results'
    )
    status_parser.set_defaults(func=_tasks)
    status_parser.add_argument(
        'workflow_id',
        help="Luigi workflow identifier"
    )


def _setup_workflows_args(subparsers):
    """Define tasks subparser and its arguments."""
    status_parser = subparsers.add_parser(
        'workflows',
        help='Get all tasks of a single workflow'
    )
    status_parser.set_defaults(func=_workflows)
    status_parser.add_argument(
        'dataset_id',
        help="Metax dataset identifier"
    )


def _setup_disable_args(subparsers):
    """Define disable subparser and its arguments."""
    disable_parser = subparsers.add_parser(
        'disable',
        help='Disable workflow'
    )
    disable_parser.set_defaults(func=_disable)
    disable_parser.add_argument(
        'workflow_id',
        help="Luigi workflow identifier"
    )


def _setup_enable_args(subparsers):
    """Define enable subparser and its arguments."""
    enable_parser = subparsers.add_parser(
        'enable',
        help='Enable workflow'
    )
    enable_parser.set_defaults(func=_enable)
    enable_parser.add_argument(
        'workflow_id',
        help="Luigi workflow identifier"
    )


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
    """Get a workflow document dict using workflow identifier.
    """
    workflow_id = args.workflow_id
    database = Database(args.config)

    document = database.get_one_workflow(workflow_id)
    if not document:
        print(
            FAILC +
            "Could not find document "
            "with workflow identifier: %s" % workflow_id +
            ENDC
        )

    return document


def _get(args):
    """Get a workflow document"""
    document = _get_workflow_document(args)
    if document:
        print(json.dumps(document, indent=4))


def _status(args):
    """Get workflow status"""
    document = _get_workflow_document(args)
    if document:
        print("Status: %s" % document["status"])
        print("Workflow is ", end="")
        print("completed" if document["completed"] else "incomplete", end="")
        print(" and ", end="")
        print("disabled" if document["disabled"] else "enabled")


def _tasks(args):
    """Get workflow task results"""
    document = _get_workflow_document(args)
    if document:
        success = []
        fail = []

        if "workflow_tasks" in document:
            for task in document["workflow_tasks"]:
                if document["workflow_tasks"][task]["result"] == "success":
                    success.append([task, document["workflow_tasks"][task]])
                else:
                    fail.append([task, document["workflow_tasks"][task]])
        else:
            print("Workflow %s has no workflow_tasks" % document["_id"])

        # Sort and print tasks that were completed successfully
        if success:
            # Sort by timestamp
            success = sorted(success, key=lambda k: k[1]["timestamp"])

            # Print
            print("Ran successfully:")
            print(SUCCESSC)
            for task in success:
                print(task[0])
            print(ENDC, end="")

        # Sort and print tasks that failed
        if fail:
            # Sort by timestamp
            success = sorted(success, key=lambda k: k[1]["timestamp"])

            # Print
            print("\nFailed:")
            print(FAILC)
            for task in fail:
                print(json.dumps(task, indent=4))
            print(ENDC, end="")


def _workflows(args):
    """Get all workflow identifiers the correct dataset identifier"""
    dataset_id = args.dataset_id
    documents = Database(args.config).get_workflows(dataset_id)

    if documents.count() == 0:
        print(FAILC + "No workflows found" + ENDC)
    else:
        for doc in documents:
            print(doc["_id"])


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
