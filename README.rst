Digital Preservation Packaging Service
======================================

`siptools_research` is a service for creating submission information packages (SIP) from research datasets.
The service reads dataset metadata from Metax metadata database and collects files from file sources, such as Ida.

Requirements
------------

Installation and usage requires Python 3.9 or newer.
The software is tested with Python 3.9 on AlmaLinux 9 release.

Packaging service requires MongoDB daemon running on default port (27017) and Luigi scheduler runnig on default port (8082).

Installation using RPM packages (preferred)
-------------------------------------------

Installation on Linux distributions is done by using the RPM Package Manager.
See how to `configure the PAS-jakelu RPM repositories`_ to setup necessary software sources.

.. _configure the PAS-jakelu RPM repositories: https://www.digitalpreservation.fi/user_guide/installation_of_tools

After the repository has been added, the package can be installed by running the following command::

    sudo dnf install python3-dpres-siptools-research

Installation using Python Virtualenv for development purposes
-------------------------------------------------------------

Install required RPM packages

* gcc, swig, python3-devel and openssl-devel are required to install M2Crypto
* CRB repository is required to install swig
* dpres-xml-schemas is required for testing XML validation
* python3-file-scraper-full is required for testing file type detection
* rpmfusion repository is required for ffmpeg, which is required by file-scraper::

   dnf config-manager --set-enabled crb
   dnf install https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-9.noarch.rpm
   dnf install gcc python3-devel openssl-devel swig python3-file-scraper-full dpres-xml-schemas libmediainfo jhove mongodb-org-server

Create a virtual environment::

   python3 -m venv venv

Run the following to activate the virtual environment::

   source venv/bin/activate

Install the required software with commands::

   pip install --upgrade pip setuptools
   pip install -r requirements_dev.txt

To deactivate the virtual environment, run ``deactivate``. To reactivate it, run the ``source`` command above.


Usage
-----

Ensure that MongoDB is running::

    systemctl start mongod

Ensure that packaging root directory exists, and you have read/write permissions::

    mkdir /var/spool/siptools_research
    chown $USER /var/spool/siptools_research

Flask API
^^^^^^^^^
Define the location of app module and configuration file::

    export FLASK_APP=siptools_research/app.py
    export SIPTOOLS_RESEARCH_CONF=include/etc/siptools_research.conf

Run the development server::

    flask run

For example, metadata generation workflow for dataset `123` can be activated by sending a POST request::

    curl http://127.0.0.1:5000/dataset/123/generate-metadata

For full list of endpoints, run::

    flask routes

Luigi workflows
^^^^^^^^^^^^^^^
Once some workflows have been activated, all enabled workflows can be manually started with luigi::

    luigi --local-scheduler --module siptools_research.workflow_init InitWorkflows --config include/etc/siptools_research.conf

To start workflows automatically, run the command for example using crontab or systemd timer. For production, use the `systemd timer <include/rhel9/SOURCES/siptools-research.timer>`_ shipped with the RPM.

CLI
^^^
For example, to check the status of dataset `123`, run command::

    siptools-research dataset status 123

For more information see::

    siptools-research --help

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
