"""Global configuration defaults and config file reader."""
import os
import logging
import ConfigParser


DEFAULTS = {
    'workspace_root': '/var/spool/siptools-research',
    'mongodb_host': 'localhost',
    'mongodb_database': 'siptools-research',
    'mongodb_collection': 'workflow',
    'metax_url': 'https://metax-test.csc.fi',
    'metax_user': 'tpas',
    'metax_password': '',
    'ida_url': 'https://86.50.169.61:4433',
    'ida_user': 'testuser_1',
    'ida_password': '',
    'dp_host': '86.50.168.218',
    'dp_user': 'tpas',
    'dp_ssh_key': '~/.ssh/id_rsa_tpas_pouta',
    'sip_sign_key': '~/sip_sign_pas.pem',
    'tpas_admin_email': 'esa.bister@csc.fi',
    'tpas_mail_sender': 'test.sender@tpas.fi',
    'sip_rejected_mail_subject': 'SIP hylätty',
    'sip_rejected_mail_msg': 'Aineistoa ei hyväksytty PASiin virheiden vuoksi. \
    Liitteenä virheraportti. \n\nOta tarvittaessa yhteyttä PAS-tukeen: {0}'
}

class Configuration(object):
    """Reads and stores configuration from configuration file."""

    config_section = 'siptools_research'
    config_file = None
    __shared_state = {}

    def __init__(self, config_file):

        # Share class state between instances (Borg design pattern)
        self.__dict__ = self.__shared_state

        # Read config file if it has not been read yet
        if self.config_file != config_file:
            self._parser = ConfigParser.RawConfigParser()
            self.read_config_file(config_file)


    def read_config_file(self, config_file):
        """Reads config file and checks all options. If an option is missing, a
        default option value is added to options. The configuration file must
        be format fotmat that can be read by ConfigParser. Options under
        section "[siptools_research]" are read.

        https://docs.python.org/2/library/configparser.html

        :config_file: Path to configuration file.
        :returns: None
        """
        # Read options from one section of config file to dictionary.
        if os.path.isfile(config_file) and os.access(config_file, os.R_OK):
            self._parser.read(config_file)
        elif not os.path.exists(config_file):
            raise IOError("Configuration file: %s not found." % \
                          config_file)
        elif not os.path.isfile(config_file):
            raise IOError("Configuration file: %s is not file." % \
                          config_file)
        else:
            raise IOError("Configuration file: %s is not readable." % \
                          config_file)

        # Get list of options for validation
        # pylint: disable=protected-access
        options = self._parser._sections[self.config_section]
        # Remove extra items from option list
        del options['__name__']

        # Check that each option in config file exists in default options
        # dictionary
        for option in options:
            assert option in DEFAULTS

        # Check which options are not found from configuration file. Add
        # default value for those options.
        for option in DEFAULTS:
            if not option in options:
                logging.warning('Using default value  %s = %s',
                                option, DEFAULTS[option])
                self._parser.set(self.config_section, option, DEFAULTS[option])

        # Set config_file parameter
        self.config_file = config_file


    def get(self, option):
        """Get value for option.

        :option: Option
        :returns: Value for Option"""
        return self._parser.get(self.config_section, option)
