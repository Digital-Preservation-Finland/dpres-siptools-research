"""Test the `siptools_research.workflow.report_preservation_status` module"""

from siptools_research.workflow import report_preservation_status

def test_reportpreservationstatus():
    """Tests that that task is complete after it has been run."""

    testpath = 'workspace'
    task = report_preservation_status.ReportPreservationStatus(
        workspace=testpath,
        dataset_id="1"
    )
    assert not task.complete()
    task.run()
    assert task.complete()
