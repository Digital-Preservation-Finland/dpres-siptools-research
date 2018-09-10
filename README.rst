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

   siptools-research --config ~/siptools_config_file.conf 1234

where ``~/siptools_config_file.conf`` is  configuration file. If no config is provided, default config file: ``/etc/siptools_research.conf`` is used.

The dataset metadata can be validated without starting the packaging workflow::

   siptools_research --validate 1234

The technical metadata can generated and posted to Metax::

   siptools_research --generate 1234

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

   yum install libxslt-devel libxml2-devel openssl-devel mongodb-server gcc ImageMagick dpres-xml-schemas

Create and activate virtualenv::

   virtualenv venv
   source venv/bin/activate

Luigi will not install with old versions of pip, so upgrade pip::

   pip install --upgrade pip

Install required python packages for testing::

   pip install -r requirements_dev.txt --process-dependency-links

Run tests that do not require running luigi/mongo::

   make test

or run one of the integration tests::

   py.test -v tests/integration_tests/workflow_test.py


Testing workflow
^^^^^^^^^^^^^^^^
Start luigid::

   luigid

Start mongodb::

   mkdir -p ~/.mongodata
   mongod --dbpath ~/.mongodata

Start workflow using luigi::

   luigi --module siptools_research.workflow.init_workflow InitWorkflow --scheduler-host=localhost  --workspace /var/spool/siptools-research/testworkspace_abdc1234 --dataset-id 1234 --config tests/data/configuration_files/siptools_research.conf



Building
--------
Build RPM::

   make rpm

Generating documentation
------------------------
Documentation for modules is automatically generated from docstrings using Sphinx (`https://wiki.csc.fi/KDK/PythonKoodinDokumentointi <https://wiki.csc.fi/KDK/PythonKoodinDokumentointi>`_)::

   cd doc/
   make html
