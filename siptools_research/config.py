"""Global configuration defaults and config file reader."""
import ConfigParser

OPTIONS = {
    'MONGODB_HOST': 'localhost',
    'MONGODB_DATABASE': 'siptools-research',
    'MONGODB_COLLECTION': 'workflow',
}

def read_config_file(config_file):
    """Stores ConfigParser so that same instance can be used across all modules
    """
    # Read options from one section of config file to dictionary
    config_section = 'siptools_research'
    parser = ConfigParser.RawConfigParser()
    parser.read(config_file)
    # pylint: disable=protected-access
    options = parser._sections(config_section)

    # Remove extra items from option dictionary
    del options['__name__']

    # Check that each option in config file exists global options dictionary,
    # and replace the value
    for option in options:
        assert option in OPTIONS
        OPTIONS[option] = options[option]
