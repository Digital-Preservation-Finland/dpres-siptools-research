"""API for managing datasets."""
from flask import Blueprint, current_app, jsonify, request

from siptools_research.workflow import Workflow

dataset = Blueprint("dataset", "dataset")


@dataset.route("/<dataset_id>/propose", methods=["POST"])
def propose_dataset(dataset_id):
    """Propose dataset for preservation.

    Once dataset has been proposed for preservation, the user with
    approve-rights can accept the dataset for preservation. However, the
    dataset also has to be validated before it can be accepted.
    Therefore, validation workflow is initialized when dataset proposed
    for preservation.

    :returns: HTTP Response
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).propose()
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).validate()

    response = jsonify({'dataset_id': dataset_id,
                        'status': 'validating dataset'})
    response.status_code = 202

    return response


@dataset.route('/<dataset_id>/preserve', methods=['POST'])
def preserve(dataset_id):
    """Trigger preservation workflow for dataset.

    :returns: HTTP Response
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).preserve()

    response = jsonify({'dataset_id': dataset_id,
                        'status': 'preserving'})
    response.status_code = 202

    return response


@dataset.route('/<dataset_id>/generate-metadata', methods=['POST'])
def generate_metadata(dataset_id):
    """Generate technical metadata and store it to Metax.

    :returns: HTTP Response
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).generate_metadata()

    response = jsonify({'dataset_id': dataset_id,
                        'status': 'generating metadata'})
    response.status_code = 202

    return response


@dataset.route("/<dataset_id>/confirm", methods=["POST"])
def confirm(dataset_id: str):
    """Confirm dataset.

    :param dataset_id: Identifier of dataset
    :returns: HTTP response
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).confirm()

    return jsonify({
        "dataset_id": dataset_id,
        "status": "dataset metadata has been confirmed"
    })


@dataset.route('/<dataset_id>/reset', methods=['POST'])
def reset(dataset_id: str):
    """Reset dataset.

    :returns: HTTP response
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

    response = jsonify({
        'dataset_id': dataset_id,
        'status': 'dataset has been reset'
    })

    return response


@dataset.route("/<dataset_id>/reject", methods=["POST"])
def reject(dataset_id: str):
    """Reject dataset.

    :param dataset_id: Identifier of dataset
    :returns: HTTP response
    """
    Workflow(
        dataset_id=dataset_id,
        config=current_app.config.get("SIPTOOLS_RESEARCH_CONF")
    ).reject()

    return jsonify({
        "dataset_id": dataset_id,
        "status": "dataset has been rejected"
    })
