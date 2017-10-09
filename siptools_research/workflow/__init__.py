"""Luigi workflow that creates some METS data, creates SIP, sends it to digital
preservation service, and cleans up the workspace.

:Task 1: Gets metadata from Metax
:Task 2: Creates descriptive metadata to datacite.xml file in DataCite format
:Task 3: Gets the dataset from IDA

:Task 4: Creates METS descriptive metadata in DataCite-from datacite.xml file
:Task 5: Creates METS digital provenance metadata
:Task 6: Creates METS technical metadata based on metadata from Metax

:Task 7: Creates file-elements (METS fileSec) and structure map (METS structMap)
:Task 8: Creates METS document
:Task 9: Signs METS-XML file digitally
:Task 10: Creates Tar archive from SIP
:Task 11: Sends SIP to to digital preservation
:Task 12: Polls for ingest report from digital preservation
:Task 13: Reads ingest report and
:Task 14: Cleans the trash
"""
