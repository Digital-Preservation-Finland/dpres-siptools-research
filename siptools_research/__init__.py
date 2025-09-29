"""Digital preservation packaging service for research datasets."""
from siptools_research.dataset import Dataset

DEFAULT_CONFIG = '/etc/siptools_research.conf'


def generate_metadata(dataset_id, config=DEFAULT_CONFIG):
    """Generate dataset metadata.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).generate_metadata()


def validate_dataset(dataset_id, config=DEFAULT_CONFIG):
    """Validate metadata and files of dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).validate()


def preserve_dataset(dataset_id, config=DEFAULT_CONFIG):
    """Preserve dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    Dataset(identifier=dataset_id, config=config).preserve()


def reset_dataset(
        dataset_id: str,
        description: str,
        reason_description: str,
        config: str = DEFAULT_CONFIG) -> None:
    """Reset dataset.

    :param dataset_id: Dataset identifier
    :param description: Preservation description to set on Metax
    :param reason_description: Preservation reason description to set on Metax
    :param config: Path to configuration file
    """
    Dataset(identifier=dataset_id, config=config).reset(
        description=description,
        reason_description=reason_description
    )
