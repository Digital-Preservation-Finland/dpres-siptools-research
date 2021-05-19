"""Install siptools-research package."""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install siptools-research."""
    setup(
        name='siptools-research',
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={'siptools_research': ['schemas/*.json']},
        python_requires='>=3.6',
        version=get_version(),
        install_requires=[
            "lxml",
            "luigi",
            "pymongo",
            "requests",
            "paramiko",
            "jsonschema",
            "iso-639",
            "python-dateutil",
            "file-scraper@git+https://gitlab.ci.csc.fi/dpres/file-scraper.git",
            "siptools@git+https://gitlab.ci.csc.fi/dpres/dpres-siptools.git"
            "@develop",
            "metax_access@git+https://gitlab.ci.csc.fi/dpres/metax-access.git"
            "@develop",
            "upload_rest_api@git+https://gitlab.ci.csc.fi/dpres/"
            "upload-rest-api.git@develop"
        ],
        entry_points={
            'console_scripts': [
                'siptools-research = siptools_research.__main__:main'
            ]
        }
    )


if __name__ == '__main__':
    main()
