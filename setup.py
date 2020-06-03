"""Install siptools-research package."""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install siptools-research."""
    setup(
        name='siptools-research',
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={'siptools_research': ['schemas/*.json']},
        version=get_version(),
        install_requires=[
            "lxml",
            # TODO: Luigi v3 does not support Python2. Version specifier should
            # be removed when Python2 compatibility is not required anymore.
            "luigi<3",
            "pymongo",
            "requests",
            "paramiko",
            "jsonschema",
            "iso-639",
            "python-dateutil",
            "configparser",  # required for python2
            "file-scraper@git+https://gitlab.csc.fi/dpres/file-scraper.git",
            "siptools@git+https://gitlab.csc.fi/dpres/dpres-siptools.git"
            "@develop",
            "metax_access@git+https://gitlab.csc.fi/dpres/metax-access.git"
            "@develop",
            "upload_rest_api@git+https://gitlab.csc.fi/dpres/"
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
