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

Run tests::

   make test

Run one test::

   py.test -v tests/utils/metax_test.py

Run some workflow::

   luigi --module siptools_research.workflow_b.init_workflow ProcessMetadata --scheduler-host=localhost  --workspace-root /var/spool/siptools-research --home-path /home

Building
--------
Build RPM::

   make rpm
