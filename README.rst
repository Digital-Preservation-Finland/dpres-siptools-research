Digital Preservation Packaging Service for Research
===================================================
Service for creating SIP from research datasets.

Installation
------------
On Centos 7 siptools_research can be installed from `DPres RPM repository <https://dpres-rpms.csc.fi/>`_::

   yum install dpres-siptools-research

Usage
-----
Commandline interface
^^^^^^^^^^^^^^^^^^^^^
To package and preserve for example dataset 1234, run::

   siptools_research --config ~/siptools_config_file.conf 1234

where ``~/siptools_config_file.conf`` is  configuration file. If no config is provided, default config file: ``/etc/siptools_research.conf`` is used.

The dataset metadata can be validated without starting the packaging workflow::

   siptools_research --validate 1234

Python inteface
^^^^^^^^^^^^^^^
The workflow can be started from python code::

   from siptools_research import preserve_dataset
   preserve_dataset('1234', config='~/siptools_config_file.conf')

Also dataset metadata validation can be used from python::

   from siptools_research import validate_dataset
   validate_dataset('1234', config='~/siptools_config_file.conf')

The ``validate_dataset`` function returns ``True`` if dataset metadata is valid.

Testing
-------
Install required RPM packages::

   yum install libxslt-devel libxml2-devel openssl-devel mongodb-server gcc ImageMagic

Luigi will not install with old versions of pip, so upgrade pip::

   pip install --upgrade pip

Install required python packages for testing::

   pip install -r requirements_dev.txt

Run tests that do not require running luigi/mongo::

   make test

Run one unit test::

   py.test -v tests/utils/metax_test.py

Testing workflow
^^^^^^^^^^^^^^^^
Start luigid::

   luigid

Start mongodb::

   mkdir -p ~/.mongodata
   mongod --dbpath ~/.mongodata

Start workflow using luigi::

   luigi --module siptools_research.workflow.init_workflow InitWorkflow --scheduler-host=localhost  --workspace /var/spool/siptools-research/testworkspace_abdc1234 --dataset-id 1234 --config tests/data/siptools_research.conf

Testing in virtualenv
^^^^^^^^^^^^^^^^^^^^^
Create and activate virtualenv::

   virtualenv venv
   source venv/bin/activate

Install required python packages to virtual environment::

   pip install -r requirements_dev.txt

Run pytest::

   python -m pytest -v tests


Building
--------
Build RPM::

   make rpm

Generating documentation
------------------------
Documentation for modules is automatically generated from docstrings using Sphinx (`https://wiki.csc.fi/KDK/PythonKoodinDokumentointi <https://wiki.csc.fi/KDK/PythonKoodinDokumentointi>`_)::

   cd doc/
   make html
