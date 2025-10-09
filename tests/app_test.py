"""Tests for ``siptools_research.app`` module."""
import flask
import pytest
from metax_access import ResourceNotAvailableError


@pytest.fixture()
def client(app):
    """Create a test client."""
    with app.test_client() as client:
        yield client


def test_index(client):
    """Test the application index page.

    :param app: Flask application
    """
    response = client.get("/")

    assert response.status_code == 400


@pytest.mark.parametrize(
    ("code", "expected_error_message", "expected_log_message"),
    [
        (404, "404 Not Found: foo", "404 Not Found: foo"),
        (400, "400 Bad Request: foo", "400 Bad Request: foo"),
        (500, "Internal server error", "500 Internal Server Error: foo"),
    ]
)
def test_http_exception_handling(
    app, caplog, code, expected_error_message, expected_log_message
):
    """Test HTTP error handling.

    Tests that API responds with correct error messages when HTTP errors
    occur.

    :param app: Flask application
    :param caplog: log capturing instance
    :param code: status code of the HTTP error
    :param expected_error_message: The error message that should be
                                   shown to the user
    :param expected_log_message: The error message that should be
                                 written to the logs
    """
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

    if code > 499:
        assert len(caplog.records) == 1
        assert caplog.records[0].message == expected_log_message
    else:
        assert not caplog.records


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
