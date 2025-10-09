"""API for viewing file errors."""
from flask import Blueprint, jsonify, request

from siptools_research.models.file_error import FileError


file_errors = Blueprint("file_errors", "file_errors")


@file_errors.route(
    '/file-errors',
    # 'GET' only accepts 8 KB of query parameters, which would limit
    # the amount of entries we can retrieve at a time.
    # TODO: Maybe use QUERY HTTP method here if/once that becomes
    # possible?
    methods=['POST']
)
def get_file_errors():
    """Return file errors for the given files.

    Following parameters can be given to filter the results:
    * ids = comma-separated list of file ID
    * storage_service = storage service of the file
    * storage_identifiers = comma-separated list of file storage
      identifiers
    * dataset_id = optional dataset identifier
    """
    filter_kwargs = {}

    if ids := request.form.get("ids"):
        filter_kwargs["file_id__in"] = ids.split(",")

    if storage_service := request.form.get("storage_service"):
        filter_kwargs["storage_service"] = storage_service

    if storage_identifiers := request.form.get("storage_identifiers"):
        filter_kwargs["storage_identifier__in"] = \
            storage_identifiers.split(",")

    if dataset_id := request.form.get("dataset_id"):
        filter_kwargs["dataset_id"] = dataset_id

    file_errors = FileError.objects.filter(**filter_kwargs)

    return jsonify([
        {
            "id": error.file_id,
            "storage_identifier": error.storage_identifier,
            "storage_service": error.storage_service.value,
            "dataset_id": error.dataset_id,
            "errors": error.errors,
            "created_at": error.created_at.isoformat()
        }
        for error in file_errors
    ])
