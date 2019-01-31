"""Install siptools-research package"""
from setuptools import setup, find_packages

from version import get_version


def main():
    """Install siptools-research"""
    setup(
        name='siptools-research',
        packages=find_packages(exclude=['tests', 'tests.*']),
        version=get_version(),
        install_requires=[
            "lxml",
            "luigi",
            "pymongo",
            "requests",
            "paramiko",
            "jsonschema",
            "iso-639",
            "siptools@git+https://gitlab.csc.fi/dpres/dpres-siptools.git'
            '@develop"
        ],
        entry_points={
            'console_scripts': [
                'siptools-research = siptools_research.__main__:main'
            ]
        }
    )

if __name__ == '__main__':
    main()
