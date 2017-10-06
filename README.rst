Digital Preservation Packaging Service for Research
===================================================
Service for creating SIP from research datasets.

Installation
------------
On Centos 7 siptools_research can be installed from `DPres RPM repository <https://dpres-rpms.csc.fi/>`_::

   yum install dpres-siptools-research

Testing
-------
Install required packages for testing::

   pip install -r requirements_dev.txt

Start mongodb::

   mongod --dbpath /path/to/data

Run tests::

   make test

Run one test::

   py.test -v tests/utils/metax_test.py

Run some workflow::

   luigi --module siptools_research.workflow_b.init_workflow ProcessMetadata --scheduler-host=localhost  --workspace-root /var/spool/siptools-research --home-path /home

Testing in virtualenv
^^^^^^^^^^^^^^^^^^^^^
Install some packages dependecies (Centos 7)::

   yum install libxslt-devel libxml2-devel openssl-devel

Create and activate virtualenv::

   virtualenv venv
   source venv/bin/activate

Luigi will not install with old versions of pip, so upgrade pip::

   pip install --upgrade pip

Run pytest::

   python -m pytest -v tests


Building
--------
Build RPM::

   make rpm
