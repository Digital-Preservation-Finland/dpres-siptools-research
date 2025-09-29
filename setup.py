"""Install siptools-research package."""

from setuptools import setup, find_packages


def main():
    """Install siptools-research."""
    setup(
        name='siptools-research',
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={'siptools_research': ['schemas/*.json']},
        python_requires='>=3.6',
        include_package_data=True,
        setup_requires=['setuptools_scm'],
        use_scm_version=True,
        install_requires=[
            "lxml",
            "luigi",
            "pymongo",
            "requests",
            "paramiko",
            "jsonschema",
            "iso-639",
            "python-dateutil",
            "click",
            "flask",
            "mongoengine"
        ],
        entry_points={
            'console_scripts': [
                'siptools-research = siptools_research.__main__:main'
            ]
        },
        tests_require=['pytest']
    )


if __name__ == '__main__':
    main()
