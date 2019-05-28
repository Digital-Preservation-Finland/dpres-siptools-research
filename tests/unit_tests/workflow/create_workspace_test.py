"""Test the :mod:`siptools_research.workflow.create_workspace` module"""

import os
import json
import httpretty
import pytest
import tests.conftest
from siptools_research.workflow import create_workspace


@pytest.mark.usefixtures("testmongoclient", "testmetax")
def test_createworkspace(testpath):
    """Tests for `CreateWorkspace` task.

    - `Task.complete()` is true after `Task.run()`
    - Directory structure is created in workspace
    - Log entry is created to mongodb

    :param testpath: Testpath fixture
    :returns: ``None``
    """

    workspace = os.path.join(testpath, 'test_workspace')
    assert not os.path.isdir(workspace)

    # Init task
    task = create_workspace.CreateWorkspace(
        workspace=workspace,
        dataset_id="dataset_1",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that directories were created
    assert os.path.isdir(workspace)
    assert os.path.isdir(os.path.join(workspace, 'sip-in-progress'))
    json_message = json.loads(httpretty.last_request().body)
    assert json_message['preservation_state'] == 90
    assert json_message['preservation_description'] == 'In packaging service'
    httpretty.reset()


@pytest.mark.usefixtures("testmongoclient", "testmetax")
def test_createworkspace_already_in_packaging_service(testpath):
    """Tests for `CreateWorkspace` task when dataset's preservation_state
    is already In Packaging Service(90).

    - `Task.complete()` is true after `Task.run()`
    - Directory structure is created in workspace
    - Log entry is created to mongodb
    - metax is not called to set preservation state

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    workspace = os.path.join(testpath, 'test_workspace')
    assert not os.path.isdir(workspace)

    # Init task
    task = create_workspace.CreateWorkspace(
        workspace=workspace,
        dataset_id="dataset_1_in_packaging_service",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that directories were created
    assert os.path.isdir(workspace)
    assert os.path.isdir(os.path.join(workspace, 'sip-in-progress'))
    # Check that metax set_preservation_state is not called
    assert isinstance(httpretty.last_request(),
                      httpretty.core.HTTPrettyRequestEmpty)
