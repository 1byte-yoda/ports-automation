import json
from collections import Counter
import pytest


@pytest.fixture(scope="class")
def dag(dagbag):
    return dagbag.get_dag('unece_dag')


unece_dag_structure = json.load(open('tests/dags/dag_tasks.json', 'r'))
unece_dag_structure = unece_dag_structure.get('tasks')


class TestUneceDagTasksDefinition:
    """Test for tasks' definition of unece_dag."""

    tasks = unece_dag_structure
    EXPECTED_TASKS = [t.get('task') for t in tasks]
    EXPECTED_TASKS_COUNT = 12

    def test_tasks(self, dag):
        """Test the number of tasks in the DAG."""
        tasks_count = len(dag.tasks)
        msg = (
            f'Wrong number of tasks, {self.EXPECTED_TASKS_COUNT} '
            f'expected, got {tasks_count}'
        )
        assert tasks_count == self.EXPECTED_TASKS_COUNT, msg

    def test_contain_tasks(self, dag):
        """Test if the DAG is composed of the expected tasks."""
        task_ids = list(map(lambda task: task.task_id, dag.tasks))
        assert self._compare(task_ids, self.EXPECTED_TASKS)

    @pytest.mark.parametrize("task", unece_dag_structure)
    def test_dependencies_of_tasks(self, dag, task):
        """Test if a given task has the expected upstream and downstream dep.

        Parametrized test function so that each task/item given in the
        unece_dag_structure is tested with the associated parameters.
        """
        expected_upstream = task.get('expected_upstream')
        expected_downstream = task.get('expected_downstream')
        task_name = task.get('task')

        task = dag.get_task(task_name)
        upstream_msg = (
            f'The task {task} doesn\'t have the expected '
            'upstream dependencies.'
        )
        downstream_msg = (
            f'The task {task} doesn\'t have the expected '
            'downstream dependencies.'
        )
        assert self._compare(task.upstream_task_ids,
                             expected_upstream), upstream_msg
        assert self._compare(
            task.downstream_task_ids, expected_downstream
        ), downstream_msg

    def test_not_catchup(self, dag):
        """Test if DAG's catchup argument is equal to False."""
        catchup = dag.catchup
        assert not catchup

    def test_same_start_date_all_tasks(self, dag):
        """Test if all tasks have the same start_date."""
        tasks = dag.tasks
        start_dates = list(map(lambda task: task.start_date, tasks))
        assert len(set(start_dates)) == 1

    def _compare(self, x, y):
        return Counter(x) == Counter(y)
