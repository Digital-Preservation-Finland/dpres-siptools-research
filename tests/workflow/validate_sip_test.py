"""Test task for testing ``siptools_workflow.validate_sip``.

Automatic testing is complicated because RemoteTarget is using ssh command to
connect remote server (see link below).

http://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ssh.html#RemoteFileSystem

To manually test the ``validate_sip`` task, add file "testworkspace" to
directory /home/tpas/rejected/2017-10-26/ or /home/tpas/accepted/2017-10-26/ on
pouta-ingest-tpas server. Then manually run the ``TestTask`` using luigi::

   luigi --module siptools_research.workflow.validate_sip ValidateSIP
