"""Test the `siptools_research.workflow.read_ingest_report` module"""

from siptools_research.workflow import read_ingest_report

def test_readingestreport():
    """Tests that that task is complete after it has been run."""

    testpath = 'workspace'
    task = read_ingest_report.ReadIngestReport(workspace=testpath,
                                               dataset_id="1")
    assert not task.complete()
    task.run()
    assert task.complete()
