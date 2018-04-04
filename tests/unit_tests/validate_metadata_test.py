"""Tests for ``siptools_research.validate_metadata`` module"""

import pytest
from siptools_research import validate_metadata
from siptools_research.workflowtask import InvalidMetadataError

@pytest.mark.usefixtures('testmetax')
def test_validate_metadata():
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.
    """
    assert validate_metadata('validate_metadata_test_dataset_1',
                             pytest.TEST_CONFIG_FILE) is True

@pytest.mark.usefixtures('testmetax')
def test_validate_invalid():
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        result = validate_metadata('validate_metadata_test_dataset_2',
                                   pytest.TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith("'contract' is a required property")
