Digital Preservation Packaging Service
======================================
Service for creating submission information packages (SIP) from research datasets.
The service reads dataset metadata from Metax metadata database and collects files from file sources, such as Ida.
SIP creation workflow is implemented using `Luigi <https://luigi.readthedocs.io>`_ and workflow status is logged in `MongoDB <https://www.mongodb.com/>`_ database.

Installation
------------
Clone this repository and install with pip::

   pip install --use-pep517 ./dpres-siptools-research/

Packaging service requires MongoDB daemon running on default port (27017) and Luigi scheduler runnig on default port (8082).
Enable systemd timer that starts/restarts all incomplete and enabled workflows found in database::

   systemctl enable siptools_research.timer
   systemctl start siptools_research.timer


Usage
-----
Commandline interface
^^^^^^^^^^^^^^^^^^^^^
To package and preserve for example dataset 1234, run::

   siptools-research preserve 1234

The dataset metadata can be validated without starting the packaging workflow::

   siptools-research validate 1234

The technical metadata can generated and posted to Metax::

   siptools-research generate 1234

Python inteface
^^^^^^^^^^^^^^^
The workflow can be started from python code::

   from siptools_research import preserve_dataset
   preserve_dataset('1234', config='~/siptools_config_file.conf')

Also dataset metadata validation can be used from python::

   from siptools_research import validate_dataset
   validate_dataset('1234', config='~/siptools_config_file.conf')

The ``validate_dataset`` function returns ``True`` if dataset metadata is valid.

Starting workflows with luigi
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A single workflow can be started/restarted using luigi command::

   luigi --module siptools_research.workflow.init_workflow InitWorkflow --workspace /var/spool/siptools-research/testworkspace_1234 --dataset-id 1234 --config /etc/siptools_research.conf

Incomplete workflows can be restarted with::

   luigi --module siptools_research.workflow_init InitWorkflows --config /etc/siptools_research.conf


Testing
-------
Install required RPM packages. Gcc and openssl-devel are required to install M2Crypto, ImageMagick, file-5.30, and ffmpeg are required by dpres-ipt, and dpres-xml-schemas is required for testing XML validation::

   rpm -Uvh http://li.nux.ro/download/nux/dextop/el7/x86_64/nux-dextop-release-0-5.el7.nux.noarch.rpm
   yum install gcc openssl-devel swig ImageMagick file-5.30 ffmpeg dpres-xml-schemas libmediainfo jhove

Create and activate virtualenv::

   virtualenv venv
   source venv/bin/activate

Luigi will not install with old versions of pip, so upgrade pip::

   pip install --upgrade pip

Install required python packages for testing::

   pip install -r requirements_dev.txt

Run unit tests::

   make test

or run one of the integration tests::

   py.test -v tests/integration_tests/workflow_test.py

.. Note ::
    validate_sip_test.py uses cloud-user account to log into preservation
    server. Thus the SSH private key of the cloud-user should be found in
    current user's `~/.ssh/pouta-key.pem` file. The test sets the dataset state
    as rejected/accepted to simulate the behavior of the preservation server.


Building
--------
Building RPM package requires rpmtools from `DPres RPM repository <https://dpres-rpms.csc.fi/>`_ and packages listed as build requirements in SPEC file (include/rhel7/SPECS/dpres-siptools-research.spec.m4). dpres-ipt requires ffmpeg from li.nux.ro repositories.::

   yum install rpmtools
   rpm -Uvh http://li.nux.ro/download/nux/dextop/el7/x86_64/nux-dextop-release-0-5.el7.nux.noarch.rpm
   grep BuildRequires include/rhel7/SPECS/dpres-siptools-research.spec.m4 | cut -d ':' -f 2 | xargs yum install -y


Build RPM::

   make rpm

Documentation for modules is automatically generated from docstrings using `Sphinx <https://www.sphinx-doc.org/en/master/>`_. Generate documentation::

   make doc


Copyright
---------
Copyright (C) 2019 CSC - IT Center for Science Ltd.

This program is free software: you can redistribute it and/or modify it under the terms
of the GNU Lesser General Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.
