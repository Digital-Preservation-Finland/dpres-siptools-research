"""Tests for :mod:`siptools_research.file_validator` module"""
import pytest

from siptools_research.file_validator import validate_files, FileValidationError
import tests.conftest


@pytest.mark.usefixtures("testmetax", "testida", "mock_metax_access", "testpath")
def test_validate_files():
    """Test that validate_metadata function returns ``True`` for valid files.
    """
    assert validate_files(
        "validate_files_valid",
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )


@pytest.mark.usefixtures("testmetax", "testida", "mock_metax_access", "testpath")
def test_validate_invalid_files():
    """Test that validating files with wrong mimetype raises
    FileValidationError.
    """
    with pytest.raises(FileValidationError) as error:
        validate_files(
            "validate_files_invalid",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(error.value) == (
        "Following files are not well-formed:\n"
        "pid:urn:invalid_mimetype_1\n"
        "pid:urn:invalid_mimetype_2\n"
    )
