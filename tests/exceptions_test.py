"""Tests for `file_error_collector`"""

import pytest

from siptools_research.exceptions import (BulkInvalidDatasetFileError,
                                          InvalidDatasetFileError,
                                          file_error_collector)


def test_file_error_collector():
    """
    Test file error collector when the maximum amount of errors is not reached
    """
    validation_complete = False
    with pytest.raises(BulkInvalidDatasetFileError) as exc, \
        file_error_collector(max_count=500) as collect_error:
        # Fake 499 files all having errors. This does not reach
        # the max count, which means the entire block will be executed
        # to completion.
        for i in range(0, 499):
            collect_error(
                InvalidDatasetFileError(
                    f"File {i} was invalid",
                    files=[{"id": f"file-id-{i}"}]
                )
            )

        validation_complete = True

    assert validation_complete
    assert "499 errors in 499 files found during processing" in str(exc.value)
    assert len(exc.value.file_errors) == 499


def test_file_error_collector_max_count_reached():
    """
    Test file error collector when the maximum amount of errors is reached
    and the block execution is halted prematurely
    """
    validation_complete = False
    with pytest.raises(BulkInvalidDatasetFileError) as exc, \
        file_error_collector(max_count=500) as collect_error:
        # Fake one million files having 3 errors each. Because of the error
        # limit of 500, only 500 will be iterated and 1498 errors collected.
        for i in range(0, 1_000_000):
            for j in range(0, 3):
                collect_error(
                    InvalidDatasetFileError(
                        f"File {i} was invalid, error {j}",
                        files=[{"id": f"file-id-{i}"}]
                    )
                )

        validation_complete = True

    # Only 500 entries were iterated
    assert not validation_complete
    assert "1498 errors in 500 files found before processing was halted" \
        in str(exc.value)
    assert len(exc.value.file_errors) == 1498

    file_errors = exc.value.file_errors

    # First file has all three errors
    for i in range(0, 3):
        assert file_errors[i].files[0]["id"] == "file-id-0"
        assert str(file_errors[i]) == f"File 0 was invalid, error {i}"

    # Last file only has the first error detected because the execution was
    # halted after the 500 file limit was reached
    assert file_errors[-1].files[0]["id"] == "file-id-499"
    assert str(file_errors[-1]) == "File 499 was invalid, error 0"
