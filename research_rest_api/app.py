"""Application instance factory."""
import logging

from flask import Flask, abort, current_app, jsonify, request
from metax_access import ResourceNotAvailableError

import siptools_research
from siptools_research.database import connect_mongoengine
from siptools_research.models.file_error import FileError

logging.basicConfig(level=logging.ERROR)
LOGGER = logging.getLogger(__name__)


SIPTOOLS_RESEARCH_CONFIG = "/etc/siptools_research.conf"


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()

    """
    app = Flask(__name__)
    app.config['CONF'] = SIPTOOLS_RESEARCH_CONFIG

    connect_mongoengine(app.config['CONF'])

    @app.route('/dataset/<dataset_id>/validate', methods=['POST'])
    def validate_dataset(dataset_id):
        """Validate dataset metadata and files.

        :returns: HTTP Response
        """
        siptools_research.validate_dataset(
            dataset_id, app.config.get('CONF')
        )

        response = jsonify({'dataset_id': dataset_id,
                            'status': 'validating dataset'})
        response.status_code = 202

        return response

    @app.route('/dataset/<dataset_id>/preserve', methods=['POST'])
    def preserve(dataset_id):
        """Trigger preservation workflow for dataset.

        :returns: HTTP Response
        """
        # Trigger dataset preservation using function provided by
        # siptools_research package.
        siptools_research.preserve_dataset(
            dataset_id, app.config.get('CONF')
        )

        response = jsonify({'dataset_id': dataset_id,
                            'status': 'preserving'})
        response.status_code = 202

        return response

    @app.route('/dataset/<dataset_id>/generate-metadata', methods=['POST'])
    def generate_metadata(dataset_id):
        """Generate technical metadata and store it to Metax.

        :returns: HTTP Response
        """
        siptools_research.generate_metadata(
            dataset_id, app.config.get('CONF')
        )

        response = jsonify({'dataset_id': dataset_id,
                            'status': 'generating metadata'})
        response.status_code = 202

        return response

    @app.route('/dataset/<dataset_id>/reset', methods=['POST'])
    def reset(dataset_id: str):
        """Reset dataset.

        :returns: HTTP response
        """
        description = request.form["description"]
        reason_description = request.form["reason_description"]

        siptools_research.reset_dataset(
            dataset_id,
            description=description,
            reason_description=reason_description,
            config=app.config.get('CONF')
        )

        response = jsonify({
            'dataset_id': dataset_id,
            'status': 'dataset has been reset'
        })

        return response

    @app.route(
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

    @app.route('/')
    def index():
        """Return a Bad Request error."""
        abort(400)

    @app.errorhandler(404)
    def page_not_found(error):
        """Handle the 404 - Not found errors.

        :returns: HTTP Response
        """
        response = jsonify({"code": 404, "error": str(error)})
        response.status_code = 404

        return response

    @app.errorhandler(400)
    def bad_request(error):
        """JSON response handler for the 400 - Bad request errors.

        :returns: HTTP Response
        """
        response = jsonify({"code": 400, "error": str(error)})
        response.status_code = 400

        return response

    @app.errorhandler(500)
    def internal_server_error(error):
        """Handle the 500 - Internal server error.

        :returns: HTTP Response
        """
        current_app.logger.error(error, exc_info=True)

        response = jsonify({"code": 500, "error": "Internal server error"})
        response.status_code = 500

        return response

    @app.errorhandler(ResourceNotAvailableError)
    def metax_error(error):
        """Handle ResourceNotAvailableError."""
        response = jsonify({"code": 404, "error": str(error)})
        response.status_code = 404
        return response

    return app
