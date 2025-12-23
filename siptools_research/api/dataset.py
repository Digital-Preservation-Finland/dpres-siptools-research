"""API for managing datasets."""
from flask import Blueprint, Response, current_app, jsonify, request, url_for

from siptools_research.exceptions import ActionNotAllowedError
from siptools_research.workflow import Workflow

dataset = Blueprint("dataset", "dataset")


@dataset.route("/<dataset_id>/status", methods=["GET"])
def status(dataset_id: str) -> Response:
    """Check status of dataset.

    :param dataset_id: Identifier of dataset
    """
    workflow = Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF"),
    )
    actions = []
    for action in [
            {
                "name": "reset",
                "endpoint": "dataset.reset",
                "precondition_method": workflow.require_reset_allowed,
            },
            {
                "name": "generate_metadata",
                "endpoint": "dataset.generate_metadata",
                "precondition_method":
                workflow.require_generate_metadata_allowed,
            },
            {
                "name": "confirm",
                "endpoint": "dataset.confirm",
                "precondition_method": workflow.require_confirm_allowed,
            },
            {
                "name": "propose",
                "endpoint": "dataset.propose_dataset",
                "precondition_method": workflow.require_propose_allowed,
            },
            {
                "name": "preserve",
                "endpoint": "dataset.preserve",
                "precondition_method": workflow.require_preserve_allowed,
            },
            {
                "name": "reject",
                "endpoint": "dataset.reject",
                "precondition_method": workflow.require_reject_allowed,
            },
    ]:
        try:
            action["precondition_method"]()
            allowed = True
            reason = None
        except ActionNotAllowedError as error:
            allowed = False
            reason = str(error)

        actions.append({
            "name": action["name"],
            "url": url_for(action["endpoint"], dataset_id=dataset_id),
            "allowed": allowed,
            "reason": reason,
        })

    return jsonify({
        "dataset_id": dataset_id,
        "enabled": workflow.enabled,
        "target": workflow.target,
        "metadata_confirmed": workflow.metadata_confirmed,
        "proposed": workflow.proposed,
        "actions": actions,
        "errors": workflow.dataset.errors,
    })


@dataset.route("/<dataset_id>/propose", methods=["POST"])
def propose_dataset(dataset_id: str) -> Response:
    """Propose dataset for preservation.

    Once dataset has been proposed for preservation, the user with
    approve-rights can accept the dataset for preservation. However, the
    dataset also has to be validated before it can be accepted.
    Therefore, validation workflow is initialized when dataset is
    proposed for preservation.

    :param dataset_id: Identifier of dataset
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).propose()

    response = jsonify({
        "dataset_id": dataset_id,
        "status": "validating dataset"
    })
    response.status_code = 202

    return response


@dataset.route("/<dataset_id>/preserve", methods=["POST"])
def preserve(dataset_id: str) -> Response:
    """Trigger preservation workflow for dataset.

    :param dataset_id: Identifier of dataset
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).preserve()

    response = jsonify({
        "dataset_id": dataset_id,
        "status": "preserving"
    })
    response.status_code = 202

    return response


@dataset.route("/<dataset_id>/generate-metadata", methods=["POST"])
def generate_metadata(dataset_id: str) -> Response:
    """Generate technical metadata and store it to Metax.

    :param dataset_id: Identifier of dataset
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).generate_metadata()

    response = jsonify({
        "dataset_id": dataset_id,
        "status": "generating metadata"
    })
    response.status_code = 202

    return response


@dataset.route("/<dataset_id>/confirm", methods=["POST"])
def confirm(dataset_id: str) -> Response:
    """Confirm dataset.

    :param dataset_id: Identifier of dataset
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).confirm()

    return jsonify({
        "dataset_id": dataset_id,
        "status": "dataset metadata has been confirmed"
    })


@dataset.route("/<dataset_id>/reset", methods=["POST"])
def reset(dataset_id: str) -> Response:
    """Reset dataset.

    :param dataset_id: Identifier of dataset
    """
    description = request.form["description"]
    reason_description = request.form["reason_description"]

    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).reset(
        description=description,
        reason_description=reason_description
    )

    return jsonify({
        "dataset_id": dataset_id,
        "status": "dataset has been reset"
    })


@dataset.route("/<dataset_id>/reject", methods=["POST"])
def reject(dataset_id: str) -> Response:
    """Reject dataset.

    :param dataset_id: Identifier of dataset
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).reject()

    return jsonify({
        "dataset_id": dataset_id,
        "status": "dataset has been rejected"
    })
