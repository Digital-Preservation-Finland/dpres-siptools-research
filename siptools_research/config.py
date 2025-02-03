"""Global configuration defaults and config file reader."""
import os
import logging
import configparser


DEFAULTS = {
    'packaging_root': '/var/spool/siptools-research',
    'mongodb_host': 'localhost',
    'mongodb_database': 'siptools-research',
    'mongodb_collection': 'workflow',
    'metax_url': 'https://metax-test.csc.fi',
    'metax_user': 'tpas',
    'metax_password': '',  # TODO: Only for Metax API V2 support
    'metax_token': '',
    'metax_ssl_verification': 'True',
    'metax_api_version': 'v2',
    'fd_download_service_authorize_url':
        'https://download.fd-test.csc.fi:4431',
    'fd_download_service_url': 'https://download.fd-test.csc.fi:4430',
    'fd_download_service_token': '',
    'fd_download_service_ssl_verification': 'True',
    'dp_host': '86.50.168.218',
    'dp_port': '22',
    'dp_user': 'tpas',
    'dp_ssh_key': '~/.ssh/id_rsa_tpas_pouta',
    'sip_sign_key': '~/sip_sign_pas.pem',
    'access_rest_api_host': "https://access.localhost",
    'access_rest_api_user': "organisaatio1",
    'access_rest_api_password': "csc123",
    'access_rest_api_base_path': "/",
    'access_rest_api_ssl_verification': 'True'
}


class Configuration:
    """Reads and stores configuration from configuration file."""

    config_section = 'siptools_research'
    config_file = None
    __shared_state = {}

    def __init__(self, config_file):

        # Share class state between instances (Borg design pattern)
        self.__dict__ = self.__shared_state

        # Read config file if it has not been read yet
        if self.config_file != config_file:
            self._parser = configparser.RawConfigParser()
            self.read_config_file(config_file)

    def read_config_file(self, config_file):
        """Reads config file and checks all options. If an option is missing, a
        default option value is added to options. The configuration file must
        be format fotmat that can be read by ConfigParser. Options under
        section "[siptools_research]" are read.

        https://docs.python.org/3/library/configparser.html

        :param config_file: path to configuration file
        :returns: ``None``
        """
        # Read options from one section of config file to dictionary.
        if os.path.isfile(config_file) and os.access(config_file, os.R_OK):
            self._parser.read(config_file)
        elif not os.path.exists(config_file):
            raise OSError("Configuration file: %s not found." %
                          config_file)
        elif not os.path.isfile(config_file):
            raise OSError("Configuration file: %s is not file." %
                          config_file)
        else:
            raise OSError("Configuration file: %s is not readable." %
                          config_file)

        # Get list of options for validation
        # pylint: disable=protected-access
        options = self._parser._sections[self.config_section]

        # Check that each option in config file exists in default options
        # dictionary
        for option in options:
            assert option in DEFAULTS

        # Check which options are not found from configuration file. Add
        # default value for those options.
        for option in DEFAULTS:
            if option not in options:
                logging.warning('Using default value  %s = %s',
                                option, DEFAULTS[option])
                self._parser.set(self.config_section, option, DEFAULTS[option])

        # Set config_file parameter
        self.config_file = config_file

    def get(self, parameter):
        """Get value for configuration parameter.

        :param parameter: parameter
        :returns: value for parameter
        """
        return self._parser.get(self.config_section, parameter)

    def getboolean(self, parameter):
        """Get boolean value for configuration parameter.

        :param parameter: parameter
        :returns: value for parameter
        """
        return self._parser.getboolean(self.config_section, parameter)
