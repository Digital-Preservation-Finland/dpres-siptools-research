"""Digital preservation packaging service for research datasets."""
from siptools_research.dataset import Dataset


def generate_metadata(dataset_id, config='/etc/siptools_research.conf'):
    """Generate dataset metadata.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).generate_metadata()


def validate_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Validate metadata and files of dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).validate()


def preserve_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Preserve dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).preserve()
