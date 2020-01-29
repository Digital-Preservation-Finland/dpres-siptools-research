"""Digital preservation packaging service for research datasets"""
# Allow importing preserve_dataset and validate_dataset functions straight from
# siptools_research module, for example:
#
#    from siptools_research import preserve_dataset
#    from siptools_research import validate_metadata
#    from siptools_research import validate_files
#    from siptools_research import generate_metadata

# Silence irrelevant import warnings
from siptools_research.utils.common import silence_stdout
with silence_stdout():

    from siptools_research.workflow_init import preserve_dataset
    from siptools_research.metadata_validator import validate_metadata
    from siptools_research.file_validator import validate_files
    from siptools_research.metadata_generator import generate_metadata
