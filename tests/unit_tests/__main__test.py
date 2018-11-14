"""Tests for ``siptools_research.__main__`` module"""

import sys
import siptools_research.__main__
import mock


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_generate(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "generate"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "generate" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools_research', 'generate', '1']):
        siptools_research.__main__.main()

    # The generate_metadata function should be called.
    mock_generate.assert_called_with('1', '/etc/siptools_research.conf')
    mock_validate.assert_not_called()
    mock_preserve.assert_not_called()


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_validate(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "validate"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "validate" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools_research', 'validate', '2']):
        siptools_research.__main__.main()

    # The validate_metadata function should be called.
    mock_validate.assert_called_with('2', '/etc/siptools_research.conf')
    mock_generate.assert_not_called()
    mock_preserve.assert_not_called()


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_preserve(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "preserve"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "preserve" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools_research', 'preserve', '3']):
        siptools_research.__main__.main()

    # The preserve_dataset function should be called.
    mock_preserve.assert_called_with('3', '/etc/siptools_research.conf')
    mock_generate.assert_not_called()
    mock_validate.assert_not_called()
