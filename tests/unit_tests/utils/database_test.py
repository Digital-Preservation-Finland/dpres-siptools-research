"""Tests for :mod:`siptools_research.utils.database` module."""
import pytest
import siptools_research.utils.database
import tests.conftest


@pytest.mark.usefixtures('testmongoclient')
def test_add_task():
    """Test add_task method.

    Adds sample task to empty database and checks that new document is
    created.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Add task for a workflow
    database.add_task('foo', 'TestTask', 'success',
                      'Everything went better than expected')

    # Check that task was added to workflow
    workflow = database.get_one_workflow('foo')
    assert workflow['workflow_tasks']['TestTask']['messages'] == \
        'Everything went better than expected'
    assert workflow['workflow_tasks']['TestTask']['result'] == 'success'

    # Check that there is no extra workflows in database
    assert len(database.get_workflows(None)) == 1

@pytest.mark.usefixtures('testmongoclient')
def test_set_complete():
    """Test setting dataset completed/incomplete."""
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Create new dataset. New dataset should be incomplete
    database.add_workflow('test', "FooTask", "dataset")
    assert database._collection.find_one({'_id': 'test'})['completed'] is False

    # Set the dataset completed
    database.set_completed('test')
    assert database._collection.find_one({'_id': 'test'})['completed'] is True

    # Set the dataset incomplete
    database.set_incomplete('test')
    assert database._collection.find_one({'_id': 'test'})['completed'] is False


@pytest.mark.usefixtures('testmongoclient')
def test_set_target_task():
    """Test setting target task of dataset."""
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    database.add_workflow('test', "FooTask", "dataset")
    database.set_target_task('test', 'NewTargetTask')

    # Check that status has changed
    assert database._collection.find_one({'_id': 'test'})['target_task']\
        == 'NewTargetTask'


@pytest.mark.usefixtures('testmongoclient')
def test_add_workflow():
    """Test adding new workflow.

    Add new workflow to database using ``add_workflow`` function and
    check that new document contains correct information.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # add new workflow
    database.add_workflow('test_workflow', "FooTask", 'test_dataset')

    # Check result
    # pylint: disable=protected-access
    workflow = database._collection.find_one({'_id': 'test_workflow'})
    workflow['status'] = 'Request received'
    workflow['dataset'] = 'test_dataset'
    workflow['incomplete'] = True


@pytest.mark.usefixtures('testmongoclient')
def test_get_current_workflow():
    """Test getting current workflow.

    Populates database with few workflows of the same dataset and checks
    that ``get_current_workflow`` function returns finds the correct
    workflow.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    database.add_workflow('test1', "FooTask", 'dataset1')
    database.set_disabled('test1')
    database.add_workflow('test2', "FooTask", 'dataset1')
    database.set_completed('test2')

    current_workflow = database.get_current_workflow('dataset1')
    assert current_workflow['_id'] == 'test2'


@pytest.mark.usefixtures('testmongoclient')
def test_get_all_active_workflows():
    """Test listing incomplete datasets.

    Populates database with few completed, incomplete and disabled
    datasets and checks that ``get_all_active_workflows`` function
    returns list of active workflows.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Populate database
    database.add_workflow('test1', "FooTask", 'dataset1')
    database.add_workflow('test2', "FooTask", 'dataset1')
    database.set_completed('test2')
    database.add_workflow('test3', "FooTask", 'dataset1')
    database.set_disabled('test3')
    database.add_workflow('test4', "FooTask", 'dataset1')

    # The workflows found by ``get_all_active_workflows`` function
    # should be "test1" and "test4"
    workflows = database.get_all_active_workflows()
    assert {workflow['_id'] for workflow in workflows} == {'test1', 'test4'}
