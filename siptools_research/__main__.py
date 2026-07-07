"""Commandline interface for siptools_research package.

Workflow status can be changed and queried with commands::

   siptools-research dataset pause <dataset_id>
   siptools-research dataset resume <dataset_id>
   siptools-research dataset status <dataset_id>
   siptools-research dataset list
"""
import pprint

import click

from siptools_research.config import Configuration
from siptools_research.database import connect_mongoengine
from siptools_research.dataset import Dataset
from siptools_research.workflow import find_workflows


def _get_workflow(dataset_id, config):
    """Get a workflow by identifier."""
    workflows = find_workflows(identifier=dataset_id, config=config)

    return workflows[0] if workflows else None


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

    configuration = Configuration(config)
    connect_mongoengine(
        host=configuration.get("mongodb_host"),
        port=configuration.get("mongodb_port"),
    )


@cli.group()
def dataset():
    """Dataset related operations"""
    pass


@dataset.command("list", help="List datasets")
@click.option(
    "--active",
    is_flag=True,
    default=False,
    help="List only active workflows. Workflow is active if it has not "
         "completed and errors have not been found from the "
         "dataset. Active workflows will be automatically restarted "
         "until they complete, unless they are manually paused."
)
@click.option(
    "--paused",
    is_flag=True,
    default=False,
    help="List only paused workflows."
)
@click.option("--show-target", is_flag=True, default=False)
@click.pass_obj
def list_datasets(ctx, active, paused, show_target) -> None:
    """List datasets."""
    workflows = find_workflows(
        active=active,
        paused=paused,
        config=ctx.config
    )
    if workflows:
        for workflow in workflows:
            if show_target:
                target = workflow.target

                click.echo(f"{workflow.dataset.identifier} (target: {target})")
            else:
                click.echo(workflow.dataset.identifier)
    else:
        dataset_not_found_echo()


@dataset.command("pause", help="Pause dataset workflow")
@click.argument("dataset_id")
@click.pass_obj
def pause_workflow(ctx, dataset_id):
    """Pause dataset workflow."""
    workflow = _get_workflow(dataset_id, config=ctx.config)
    if workflow:
        workflow.pause()
        click.echo(
            f"The Workflow of the dataset {workflow.dataset.identifier} was"
            " paused"
        )
    else:
        dataset_not_found_echo()


@dataset.command("resume", help="Resume paused dataset workflow")
@click.argument("dataset_id")
@click.pass_obj
def resume_workflow(ctx, dataset_id):
    """Resume dataset workflow."""
    workflow = _get_workflow(dataset_id, config=ctx.config)
    if workflow:
        workflow.resume()
        click.echo(
            f"The Workflow of the dataset {workflow.dataset.identifier} was "
            "resumed"
        )
    else:
        dataset_not_found_echo()


@dataset.command("status", help="Show dataset workflow status")
@click.argument("dataset_id")
@click.pass_obj
def workflow_status(ctx, dataset_id):
    """Show status for dataset workflow."""
    workflow = _get_workflow(dataset_id, config=ctx.config)
    if workflow:
        workflow_dict = workflow._document.to_mongo().to_dict()

        # TODO: These task logs are not required anymore for anything
        # else than debugging purposes. Is anyone really using them, or
        # could they be removed? The luigi logs contain more or less the
        # same information. And luigi also provides built-in Task
        # history:
        # https://luigi.readthedocs.io/en/stable/central_scheduler.html#enabling-task-history
        # If we really keep the task logs, they probably should not be
        # in Dataset class (single-responsibility principle), and using
        # them should not require accessing private members of the
        # class.
        dataset_dict = Dataset(
            dataset_id,
            config=ctx.config
        )._document.to_mongo().to_dict()
        for task in dataset_dict["workflow_tasks"].values():
            # Convert any timestamps to ISO 8601 timestamps before printing
            task["timestamp"] = task["timestamp"].isoformat()

        workflow_dict["workflow_tasks"] = dataset_dict["workflow_tasks"]

        click.echo(pprint.pformat(workflow_dict))
    else:
        dataset_not_found_echo()


def dataset_not_found_echo():
    """
    Helper function for common click output.
    """
    click.echo(click.style("Dataset not found!", fg="red"))


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    cli(obj=Context())


if __name__ == '__main__':
    main()
