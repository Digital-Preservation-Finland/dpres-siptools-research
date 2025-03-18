Digital Preservation Packaging Service
======================================
Service for creating submission information packages (SIP) from research datasets.
The service reads dataset metadata from Metax metadata database and collects files from file sources, such as Ida.
SIP creation workflow is implemented using `Luigi <https://luigi.readthedocs.io>`_ and workflow status is logged in `MongoDB <https://www.mongodb.com/>`_ database.

Requirements
------------

Installation and usage requires Python 3.9 or newer.
The software is tested with Python 3.9 on AlmaLinux 9 release.

Installation using RPM packages (preferred)
-------------------------------------------

Installation on Linux distributions is done by using the RPM Package Manager.
See how to `configure the PAS-jakelu RPM repositories`_ to setup necessary software sources.

.. _configure the PAS-jakelu RPM repositories: https://www.digitalpreservation.fi/user_guide/installation_of_tools 

After the repository has been added, the package can be installed by running the following command::

    sudo dnf install python3-dpres-siptools-research


Usage
-----

To package and preserve dataset 1234, for example, run::

   siptools-research preserve 1234

The dataset metadata can be validated without starting the packaging workflow by running the following command::

   siptools-research validate 1234

To generate and post the technical metadata to Metax, run::

   siptools-research generate 1234

Installation using Python Virtualenv for development purposes
-------------------------------------------------------------

Install required RPM packages

* gcc, swig, python3-devel are required to install M2Crypto
* CRB repository is required to install swig
* dpres-xml-schemas is required for testing XML validation
* python3-file-scraper-full is required for testing file type detection
* rpmfusion repository is required for ffmpeg, which is required by file-scraper::

   dnf config-manager --set-enabled crb
   dnf install https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-9.noarch.rpm
   dnf install gcc python3-devel swig python3-file-scraper-full dpres-xml-schemas libmediainfo jhove

Create a virtual environment::

   python3 -m venv venv

Run the following to activate the virtual environment::

   source venv/bin/activate

Install the required software with commands::

   pip install --upgrade pip setuptools
   pip install -r requirements_dev.txt
   pip install --use-pep517 .

To deactivate the virtual environment, run ``deactivate``. To reactivate it, run the ``source`` command above.

Packaging service requires MongoDB daemon running on default port (27017) and Luigi scheduler runnig on default port (8082).
Enable systemd timer that starts/restarts all incomplete and enabled workflows found in database::

   systemctl enable siptools_research.timer
   systemctl start siptools_research.timer


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
