"""Tests for ``siptools_research.app`` module."""
import flask
import pytest
from metax_access import ResourceNotAvailableError

from siptools_research.models.file_error import FileError


@pytest.fixture()
def default_file_errors(app):
    """
    Default file errors to be added into the error database.

    Used in the test cases.
    """
    file_errors = []
    for i in range(0, 20):
        file_error = FileError(
            file_id=f"file-id-{i}",
            storage_identifier=f"storage-identifier-id-{i}",
            # Every file with even id is stored in IDA, odd files in PAS
            storage_service="ida" if i % 2 == 0 else "pas",
            # Every 5th file error has a dataset
            dataset_id=f"dataset-id-{i}" if i % 5 == 0 else None,
            errors=[f"error {i}"]
        )
        file_error.save()

        file_errors.append(file_error)

    return file_errors


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


def test_dataset_preserve(mocker, app, client):
    """Test preserving dataset.

    :param mocker: pytest-mock mocker
    :param app: Flask application
    :param client: Flask test client
    """
    mock_function = mocker.patch("siptools_research.preserve_dataset")

    response = client.post("/dataset/1/preserve")
    assert response.status_code == 202

    mock_function.assert_called_with(
        "1", app.config.get("CONF")
    )

    assert response.json == {
        "dataset_id": "1",
        "status": "preserving"
    }


def test_dataset_generate_metadata(mocker, app, client):
    """Test the generating metadata.

    :param mocker: pytest-mock mocker
    :param app: Flask application
    :param client: Flask test client
    """
    mock_function = mocker.patch("siptools_research.generate_metadata")

    response = client.post("/dataset/1/generate-metadata")
    assert response.status_code == 202

    mock_function.assert_called_with(
        "1", app.config.get("CONF")
    )

    assert response.json == {
        "dataset_id": "1",
        "status": "generating metadata"
    }


def test_dataset_reset(mocker, app, client):
    """Test resetting the dataset.

    :param mocker: pytest-mock mocker
    :param app: Flask application
    :param client: Flask test client
    """
    mock_function = mocker.patch("siptools_research.reset_dataset")

    response = client.post(
        "/dataset/1/reset",
        data={
            "description": "Reset by user",
            "reason_description": "File was incorrect"
        }
    )
    assert response.status_code == 200

    mock_function.assert_called_with(
        "1",
        description="Reset by user",
        reason_description="File was incorrect",
        config=app.config.get("CONF")
    )

    assert response.json == {
        "dataset_id": "1",
        "status": "dataset has been reset"
    }


def test_validate_dataset(mocker, app, client):
    """Test validating files and metadata.

    :param mocker: pytest-mock mocker
    :param app: Flask application
    :param client: Flask test client
    """
    mock_function = mocker.patch("siptools_research.validate_dataset")

    response = client.post("/dataset/1/validate")
    assert response.status_code == 202

    mock_function.assert_called_with(
        "1", app.config.get("CONF")
    )

    assert response.json == {"dataset_id": "1", "status": "validating dataset"}


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


@pytest.mark.usefixtures("default_file_errors")
def test_retrieve_file_errors(client):
    """
    Try retrieving two file errors and ensure they have the correct
    data
    """
    response = client.post(
        "/file-errors",
        data={"ids": "file-id-1,file-id-5"}
    )
    file_errors = response.json

    # Two files found
    assert len(response.json) == 2

    file_1 = next(
        file for file in file_errors
        if file["id"] == "file-id-1"
    )
    file_5 = next(
        file for file in file_errors
        if file["id"] == "file-id-5"
    )

    assert file_1["storage_service"] == "pas"
    assert file_1["storage_identifier"] == "storage-identifier-id-1"
    assert file_1["errors"] == ["error 1"]
    assert file_1["dataset_id"] is None

    assert file_5["storage_identifier"] == "storage-identifier-id-5"
    assert file_5["errors"] == ["error 5"]
    # Every fifth test file has an associated dataset
    assert file_5["dataset_id"] == "dataset-id-5"


@pytest.mark.usefixtures("default_file_errors")
@pytest.mark.parametrize(
    "params,expected_ids",
    [
        (
            {"ids": "file-id-1"},
            ["file-id-1"]
        ),
        (
            # Every odd test file is stored in PAS
            {"storage_service": "pas"},
            [
                "file-id-1", "file-id-3", "file-id-5", "file-id-7",
                "file-id-9", "file-id-11", "file-id-13", "file-id-15",
                "file-id-17", "file-id-19"
            ]
        ),
        (
            {
                "storage_identifiers":
                "storage-identifier-id-1,storage-identifier-id-5"
            },
            ["file-id-1", "file-id-5"]
        ),
        (
            {
                "dataset_id": "dataset-id-5"
            },
            ["file-id-5"]
        ),
        (
            # Query parameters result in an AND query
            {
                "ids": "file-id-0,file-id-1,file-id-2,file-id-3",
                "storage_service": "ida"
            },
            ["file-id-0", "file-id-2"]
        )
    ]
)
def test_file_error_filter(client, params, expected_ids):
    """
    Test retrieving file errors with given POST parameters.
    """
    response = client.post(
        "/file-errors",
        data=params
    )
    file_errors = response.json

    found_file_ids = {file["id"] for file in file_errors}

    assert set(expected_ids) == found_file_ids


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
