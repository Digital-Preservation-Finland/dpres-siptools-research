"""Application instance factory."""
import logging
import os

from flask import Flask, abort, current_app, jsonify
from metax_access import ResourceNotAvailableError

import siptools_research.api.dataset
import siptools_research.api.file_errors
from siptools_research.config import Configuration
from siptools_research.database import connect_mongoengine
from siptools_research.exceptions import ActionNotAllowedError

logging.basicConfig(level=logging.ERROR)
LOGGER = logging.getLogger(__name__)


SIPTOOLS_RESEARCH_CONF = "/etc/siptools_research.conf"


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()

    """
    app = Flask(__name__)
    app.config["SIPTOOLS_RESEARCH_CONF"] = os.getenv("SIPTOOLS_RESEARCH_CONF",
                                                     SIPTOOLS_RESEARCH_CONF)
    app.register_blueprint(siptools_research.api.dataset.dataset,
                           url_prefix="/dataset")
    app.register_blueprint(siptools_research.api.file_errors.file_errors)

    configuration = Configuration(app.config["SIPTOOLS_RESEARCH_CONF"])
    connect_mongoengine(
        host=configuration.get("mongodb_host"),
        port=configuration.get("mongodb_port"),
    )

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

    @app.errorhandler(ActionNotAllowedError)
    def action_not_allowed_error(error):
        """Handle ActionNotAllowedError."""
        response = jsonify({"code": 409, "error": str(error)})
        response.status_code = 409
        return response

    return app
