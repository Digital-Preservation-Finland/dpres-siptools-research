"""Luigi workflow that creates SIP, sends it to digital preservation service
and cleans up the workspace.

:Task 1: Creates file-elements (METS fileSec) and structure map (METS structMap)
:Task 2: Creates METS document
:Task 3: Signs METS-XML file digitally
:Task 4: Creates Tar archive from SIP
:Task 5: Sends SIP to to digital preservation
:Task 6: Polls for ingest report from digital preservation
:Task 7: Reads ingest report and
:Task 8: Cleans the trash
"""
