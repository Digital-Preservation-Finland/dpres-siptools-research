"""Install siptools-research package"""
from setuptools import setup, find_packages

def main():
    """Install siptools-research"""
    setup(
        name='siptools_research',
        packages=find_packages(exclude=['tests', 'tests.*']),
        version='dev',
        entry_points={
            'console_scripts': [
                'siptools_research = '\
                'siptools_research.__main__:main'
            ]
        }
    )

if __name__ == '__main__':
    main()
