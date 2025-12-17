"""Tests for ``siptools_research.app`` module."""
import flask
import pytest
from metax_access import ResourceNotAvailableError

from siptools_research.exceptions import ActionNotAllowedError


def test_index(client):
    """Test the application index page.

    :param app: Flask application
    """
    response = client.get("/")

    assert response.status_code == 400


@pytest.mark.parametrize(
    ("code", "expected_error_message"),
    [
        (404, "404 Not Found: foo"),
        (400, "400 Bad Request: foo"),
        (500, "Internal server error"),
    ]
)
def test_http_exception_handling(
    app, caplog, code, expected_error_message
):
    """Test HTTP error handling.

    Tests that API responds with correct error messages when HTTP errors
    occur.

    :param app: Flask application
    :param caplog: log capturing instance
    :param code: status code of the HTTP error
    :param expected_error_message: The error message that should be
                                   shown to the user
    """
    # Let Flask handle exceptions
    app.testing = False

    @app.route("/test")
    def _raise_exception():
        """Raise exception."""
        flask.abort(code, "foo")

    with app.test_client() as client:
        response = client.get("/test")

    assert response.json == {
        "code": code,
        "error": expected_error_message
    }

    # The error handlers should not log anything. Flask will
    # automatically log unhandled exceptions.
    assert not caplog.records


def test_unhandled_exception(app, caplog):
    """Test that error is logged when unhandled exception is raised.

    :param app: Flask application
    :param caplog: log capturing instance
    """
    # Let Flask handle Exception
    app.testing = False

    @app.route("/test")
    def _raise_exception():
        """Raise exception."""
        raise Exception("Unexpected error")

    with app.test_client() as client:
        response = client.get("/test")

    assert response.json == {
        "code": 500,
        "error": "Internal server error",
    }

    assert len(caplog.records) == 1
    assert caplog.records[0].message == "Exception on /test [GET]"
    assert "Unexpected error" in caplog.records[0].exc_text


def test_metax_error_handler(app, caplog):
    """Test Metax 404 error handling.

    Test that API responds correctly when resource is not available in
    Metax.

    :param app: Flask application
    :param caplog: log capturing instance
    """
    error_message = "Dataset not available."

    @app.route("/test")
    def _raise_exception():
        """Raise exception."""
        raise ResourceNotAvailableError(error_message)

    with app.test_client() as client:
        response = client.get("/test")

    assert response.json == {
        "code": 404,
        "error": error_message
    }

    assert len(caplog.records) == 0


def test_action_not_allowed_error_handler(app):
    """Test ActionNotAllowedError is handled.

    :param app: Flask application
    """
    error_message = "Dataset is not ready for this action!"

    @app.route("/test")
    def _raise_exception():
        """Raise exception."""
        raise ActionNotAllowedError(error_message)

    with app.test_client() as client:
        response = client.get("/test")

    assert response.json == {
        "code": 409,
        "error": error_message
    }
