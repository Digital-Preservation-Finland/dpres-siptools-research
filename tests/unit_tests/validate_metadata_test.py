"""Tests for ``siptools_research.validate_metadata`` module"""

import pytest
from siptools_research import validate_metadata

@pytest.mark.usefixtures('testmetax')
def test_validate_metadata():
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.
    """
    assert validate_metadata('validate_metadata_test_dataset_1',
                             'tests/data/siptools_research.conf') is True

@pytest.mark.usefixtures('testmetax')
def test_validate_invalid():
    """Test that validate_metadata function returns error message for invalid
    dataset.
    """
    # with pytest.raises
    result = validate_metadata('validate_metadata_test_dataset_2',
                               'tests/data/siptools_research.conf')
    assert result is not True
    assert result.startswith("'contract' is a required property")
