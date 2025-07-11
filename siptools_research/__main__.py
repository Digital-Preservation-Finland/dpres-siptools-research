"""Commandline interface for siptools_research package.

Workflow status can be changed and queried with commands::

   siptools-research dataset {disable,enable,status} <dataset_id>
   siptools-research dataset list

and::

   siptools-research command --help

"""
import click
import pprint

from siptools_research.dataset import find_datasets
from siptools_research.database import connect_mongoengine


def _get_dataset(dataset_id, config):
    """Get a dataset by identifier."""
    datasets = find_datasets(identifier=dataset_id, config=config)

    return datasets[0] if datasets else None


def _get_datasets(enabled, disabled, config):
    """Find datasets with filters."""
    if disabled:
        enabled = False
    elif enabled:
        enabled = True
    else:
        enabled = None

    datasets = find_datasets(enabled=enabled, config=config)

    return datasets


class Context:
    """Context class for the Click application"""
    config: str = ""


@click.group()
@click.option(
    "--config",
    type=click.Path(file_okay=True, dir_okay=False, readable=True),
    default="/etc/siptools_research.conf",
    help="Path to the siptools-research configuration file"
)
@click.pass_obj
def cli(ctx, config):
    """
    CLI main entrypoint
    """
    ctx.config = config

    connect_mongoengine(config)


@cli.group()
def dataset():
    """Dataset related operations"""
    pass


@dataset.command("list", help="List datasets based on various filters")
@click.option("--enabled", is_flag=True, default=None)
@click.option("--disabled", is_flag=True, default=None)
@click.option("--show-target", is_flag=True, default=False)
@click.pass_obj
def list_datasets(ctx, enabled, disabled, show_target):
    if enabled and disabled:
        raise click.UsageError(
            "'enabled' and 'disabled' are mutually exclusive"
        )

    datasets = _get_datasets(
        enabled=enabled, disabled=disabled, config=ctx.config
    )
    if datasets:
        for dataset in datasets:
            if show_target:
                try:
                    target = dataset.target.value
                except AttributeError:
                    target = "None"

                click.echo(f"{dataset.identifier} (target: {target})")
            else:
                click.echo(dataset.identifier)
    else:
        dataset_not_found_error()


@dataset.command("disable", help="Disable dataset workflow")
@click.argument("dataset_id")
@click.pass_obj
def disable_workflow(ctx, dataset_id):
    """Disable dataset workflow"""
    dataset = _get_dataset(dataset_id, config=ctx.config)
    if dataset:
        dataset.disable()
        click.echo(f"Workflow of dataset {dataset.identifier} disabled")
    else:
        dataset_not_found_error()


@dataset.command("enable", help="Enable dataset workflow")
@click.argument("dataset_id")
@click.pass_obj
def enable_workflow(ctx, dataset_id):
    """Enable dataset workflow"""
    dataset = _get_dataset(dataset_id, config=ctx.config)
    if dataset:
        dataset.enable()
        click.echo(f"Workflow of dataset {dataset.identifier} enabled")
    else:
        dataset_not_found_error()


@dataset.command("status", help="Show dataset workflow status")
@click.argument("dataset_id")
@click.pass_obj
def workflow_status(ctx, dataset_id):
    """Show status for dataset workflow"""
    dataset = _get_dataset(dataset_id, config=ctx.config)
    if dataset:
        dataset_dict = dataset._document.to_mongo().to_dict()

        # Convert any timestamps to ISO 8601 timestamps before printing
        for task in dataset_dict["workflow_tasks"].values():
            task["timestamp"] = task["timestamp"].isoformat()

        click.echo(pprint.pformat(dataset_dict))
    else:
        dataset_not_found_error()


def dataset_not_found_error():
    click.echo(click.style("Dataset not found!", fg="red"))


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    cli(obj=Context())


if __name__ == '__main__':
    main()
