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
import argparse
import json

from metax_access import Metax
from metax_access import (
    DS_STATE_TECHNICAL_METADATA_GENERATED,
    DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED,
    DS_STATE_VALIDATING_METADATA,
    DS_STATE_METADATA_VALIDATION_FAILED,
    DS_STATE_VALID_METADATA
)

from siptools_research.config import Configuration
from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflow_init import preserve_dataset
from siptools_research.metadata_validator import validate_metadata
from siptools_research.utils.database import Database
from siptools_research.utils.download import clean_file_cache


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
                    'workflows in MongoDB.'
    )

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')
    _setup_generate_args(subparsers)
    _setup_validate_args(subparsers)
    _setup_preserve_args(subparsers)
    _setup_workflow_args(subparsers)
    _setup_workflows_args(subparsers)
    _setup_status_args(subparsers)
    _setup_tasks_args(subparsers)
    _setup_dataset_args(subparsers)
    _setup_disable_args(subparsers)
    _setup_enable_args(subparsers)
    _setup_clean_cache_args(subparsers)

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
    generate_parser.add_argument('dataset_id', help="Dataset identifier")
    generate_parser.add_argument(
        '--set-preservation-state',
        action="store_true", default=False,
        help="Set preservation state based on metadata generation results"
    )


def _setup_validate_args(subparsers):
    """Define validate subparser and its arguments."""
    validate_parser = subparsers.add_parser(
        'validate', help='Validate dataset metadata'
    )
    validate_parser.set_defaults(func=_validate)
    validate_parser.add_argument('dataset_id', help="Dataset identifier")
    validate_parser.add_argument(
        '--set-preservation-state',
        action="store_true", default=False,
        help="Set preservation state based on metadata validation results"
    )


def _setup_preserve_args(subparsers):
    """Define preserve subparser and its arguments."""
    preserve_parser = subparsers.add_parser(
        'preserve', help='Start preservation workflow'
    )
    preserve_parser.set_defaults(func=_preserve)
    preserve_parser.add_argument('dataset_id', help="Dataset identifier")


def _setup_workflow_args(subparsers):
    """Define workflow subparser and its arguments."""
    get_parser = subparsers.add_parser(
        'workflow',
        help='Get a workflow'
    )
    get_parser.set_defaults(func=_workflow)
    get_parser.add_argument(
        'workflow_id',
        help="Workflow identifier"
    )


def _setup_workflows_args(subparsers):
    """Define workflows subparser and its arguments."""
    parser = subparsers.add_parser(
        'workflows',
        help='Get workflow documents with specified filters'
    )
    parser.set_defaults(func=_workflows)
    parser.add_argument(
        '--dataset',
        help="Dataset identifier"
    )
    parser.add_argument(
        '--disabled',
        action="store_true", default=False,
        help="Filter by disabled == True"
    )
    parser.add_argument(
        '--enabled',
        action="store_true", default=False,
        help="Filter by disabled == False"
    )
    parser.add_argument(
        '--incomplete',
        action="store_true", default=False,
        help="Filter by completed == False"
    )
    parser.add_argument(
        '--completed',
        action="store_true", default=False,
        help="Filter by completed == True"
    )
    parser.add_argument(
        '--full',
        action="store_true", default=False,
        help="Print the full workflows"
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
        help="Workflow identifier"
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
        help="Workflow identifier"
    )


def _setup_dataset_args(subparsers):
    """Define tasks subparser and its arguments."""
    status_parser = subparsers.add_parser(
        'dataset',
        help='Get all workflows of a dataset'
    )
    status_parser.set_defaults(func=_dataset)
    status_parser.add_argument(
        'dataset_id',
        help="Dataset identifier"
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
        help="Workflow identifier"
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
        help="Workflow identifier"
    )


def _setup_clean_cache_args(subparsers):
    """Define clean-cache subparser and its arguments."""
    clean_cache_parser = subparsers.add_parser(
        'clean-cache',
        help='Clean old files from Ida file cache'
    )
    clean_cache_parser.set_defaults(func=_clean_cache)


def _generate(args):
    """Generate technical metadata for the dataset"""
    conf = Configuration(args.config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )

    try:
        generate_metadata(args.dataset_id, args.config)
    except Exception:
        if args.set_preservation_state:
            metax_client.set_preservation_state(
                args.dataset_id,
                state=DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED
            )
        raise

    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            state=DS_STATE_TECHNICAL_METADATA_GENERATED
        )


def _validate(args):
    """Validate dataset metadata"""
    conf = Configuration(args.config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            state=DS_STATE_VALIDATING_METADATA
        )

    try:
        validate_metadata(args.dataset_id, args.config)
    except Exception:
        if args.set_preservation_state:
            metax_client.set_preservation_state(
                args.dataset_id,
                state=DS_STATE_METADATA_VALIDATION_FAILED
            )
        raise

    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            state=DS_STATE_VALID_METADATA
        )


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


def _get_workflow_documents(args):
    """Get a workflow documents with filters.
    """
    if args.disabled and args.enabled:
        raise ValueError("Use either disabled or enabled")
    elif args.incomplete and args.completed:
        raise ValueError("Use either incomplete or completed")

    search = {}
    if args.dataset:
        search["dataset"] = args.dataset
    if args.disabled:
        search["disabled"] = True
    if args.enabled:
        search["disabled"] = False
    if args.incomplete:
        search["completed"] = False
    if args.completed:
        search["completed"] = True

    database = Database(args.config)
    documents = database.get(search)
    if documents.count() == 0:
        print(FAILC + "Could not find any workflows" + ENDC)

    return documents


def _workflow(args):
    """Get a workflow document"""
    document = _get_workflow_document(args)
    if document:
        print(json.dumps(document, indent=4))


def _workflows(args):
    """Get a workflow documents"""
    documents = _get_workflow_documents(args)
    if documents.count() > 0:
        for document in documents:
            if not args.full:
                print(document["_id"])
            else:
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


def _dataset(args):
    """Get all workflow identifiers with the correct dataset identifier"""
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


def _clean_cache(args):
    """Clean files from Ida file cache"""
    clean_file_cache(args.config)


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
