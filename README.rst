Digital Preservation Packaging Service for Research
===================================================
Service for creating SIP from research datasets.

Installation
------------
On Centos 7 siptools_research can be installed from `DPres RPM repository <https://dpres-rpms.csc.fi/>`_::

   yum install dpres-siptools-research

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

or start workflow from init script::

   python siptools_research/workflow/init_workflow.py 1234 --config tests/data/siptools_research.conf

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
