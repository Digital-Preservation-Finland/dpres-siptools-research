"""Commandline interface for siptools_research package.

To start the preservation workflow for dataset 1234 (for example)::

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
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_INVALID_METADATA,
    DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
)

from siptools_research.config import Configuration
from siptools_research.dataset import Dataset, find_datasets
from siptools_research.exceptions import (InvalidDatasetFileError,
                                          InvalidDatasetError)
from siptools_research.metadata_generator import generate_metadata
from siptools_research import preserve_dataset
from siptools_research.metadata_validator import validate_metadata
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
    validate_parser.add_argument(
        '--dummy-doi',
        action="store_true", default=False,
        help="Use dummy doi for metadata validation"
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
        'dataset_id',
        help="Dataset identifier"
    )


def _setup_workflows_args(subparsers):
    """Define workflows subparser and its arguments."""
    parser = subparsers.add_parser(
        'workflows',
        help='Get workflow documents with specified filters'
    )
    parser.set_defaults(func=_workflows)
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
        'dataset_id',
        help="Dataset identifier"
    )


def _setup_tasks_args(subparsers):
    """Define tasks subparser and its arguments."""
    status_parser = subparsers.add_parser(
        'tasks',
        help='Get workflow task results'
    )
    status_parser.set_defaults(func=_tasks)
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
        'dataset_id',
        help="Dataset identifier"
    )


def _setup_enable_args(subparsers):
    """Define enable subparser and its arguments."""
    enable_parser = subparsers.add_parser(
        'enable',
        help='Enable workflow'
    )
    enable_parser.set_defaults(func=_enable)
    enable_parser.add_argument(
        'dataset_id',
        help="Dataset identifier"
    )


def _setup_clean_cache_args(subparsers):
    """Define clean-cache subparser and its arguments."""
    clean_cache_parser = subparsers.add_parser(
        'clean-cache',
        help='Clean old files from Ida file cache'
    )
    clean_cache_parser.set_defaults(func=_clean_cache)


def _generate(args):
    """Generate technical metadata for the dataset."""
    conf = Configuration(args.config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )

    try:
        generate_metadata(args.dataset_id, args.config)
    except InvalidDatasetFileError as error:
        if args.set_preservation_state:
            metax_client.set_preservation_state(
                args.dataset_id,
                DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED,
                f'{error}: {error.files}'
            )
        raise

    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            DS_STATE_TECHNICAL_METADATA_GENERATED,
            "Technical metadata generated by system admin"
        )


def _validate(args):
    """Validate dataset metadata."""
    conf = Configuration(args.config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    dummy_doi = "true" if args.dummy_doi else "false"
    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            DS_STATE_VALIDATING_METADATA,
            "Validation started by system admin"
        )

    try:
        validate_metadata(args.dataset_id, args.config, dummy_doi=dummy_doi)
    except InvalidDatasetError as error:
        if args.set_preservation_state:
            metax_client.set_preservation_state(
                args.dataset_id,
                DS_STATE_INVALID_METADATA,
                f"Metadata is invalid: {error}"
            )
        raise
    except Exception:
        if args.set_preservation_state:
            metax_client.set_preservation_state(
                args.dataset_id,
                DS_STATE_METADATA_VALIDATION_FAILED,
                "Validation failed for unknown reason"
            )
        raise

    if args.set_preservation_state:
        metax_client.set_preservation_state(
            args.dataset_id,
            DS_STATE_METADATA_CONFIRMED,
            "Metadata is valid"
        )


def _preserve(args):
    """Start preservation workflow."""
    # Report preservation state to Metax
    conf = Configuration(args.config)
    metax_client = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    metax_client.set_preservation_state(
        args.dataset_id,
        DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        'In packaging service'
    )

    preserve_dataset(args.dataset_id, args.config)


def _get_dataset(args):
    """Get a dataset by identifier."""
    dataset = Dataset(args.dataset_id, config=args.config)
    if not dataset.target:
        print(
            f"{FAILC}Could not find dataset with identifier:"
            f" {args.dataset_id}{ENDC}"
        )

    return dataset


def _get_datasets(args):
    """Find datasets with filters."""
    if args.disabled and args.enabled:
        raise ValueError("Use either disabled or enabled")

    if args.disabled:
        enabled = False
    elif args.enabled:
        enabled = True
    else:
        enabled = None

    datasets = find_datasets(enabled=enabled, config=args.config)
    if not datasets:
        print(FAILC + "Could not find any workflows" + ENDC)

    return datasets


def _workflow(args):
    """Get a workflow document."""
    dataset = _get_dataset(args)
    if dataset.target:
        print(f'Dataset identifier: {dataset.identifier}\n'
              f'Target: {dataset.target}')


def _workflows(args):
    """Get a workflow documents."""
    datasets = _get_datasets(args)
    for dataset in datasets:
        if not args.full:
            print(dataset.identifier)
        else:
            print(f'Dataset identifier: {dataset.identifier}\n'
                  f'Target: {dataset.target}')


def _status(args):
    """Get workflow status."""
    dataset = _get_dataset(args)
    if dataset:
        status = "enabled" if dataset.enabled else "disabled"
        print(f"Workflow is {status}")


def _tasks(args):
    """Get workflow task results."""
    dataset = _get_dataset(args)
    if dataset:
        success = []
        fail = []

        tasks = dataset.get_tasks()
        if tasks:
            for task in tasks:
                if tasks[task]["result"] == "success":
                    success.append([task, tasks[task]])
                else:
                    fail.append([task, tasks[task]])
        else:
            print(f"Dataset {dataset.identifier} has no workflow_tasks")

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


def _disable(args):
    """Disable workflow."""
    dataset = _get_dataset(args)
    if dataset:
        dataset.disable()
        print(f"Workflow of dataset {dataset.identifier} disabled")


def _enable(args):
    """Enable workflow."""
    dataset = _get_dataset(args)
    if dataset:
        dataset.enable()
        print(f"Workflow of dataset {dataset.identifier} enabled")


def _clean_cache(args):
    """Clean files from Ida file cache."""
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
