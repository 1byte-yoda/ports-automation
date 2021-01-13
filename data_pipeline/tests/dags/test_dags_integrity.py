# data_pipeline/tests/dags/test_dags_integrity.py


import os
import glob
import datetime
import importlib.util
import pytest
from airflow import DAG
from airflow.utils.dag_cycle_tester import test_cycle


DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


class TestDagIntegrity:
    """Check the integrity of dags in the 'dags' folder."""

    LOAD_SECOND_THRESHOLD = datetime.timedelta(seconds=3)
    REQUIRED_EMAIL = 'unece_ports@yopmail.com'
    EXPECTED_NUMBER_OF_DAGS = 1

    def test_import_dags(self, dagbag):
        """Test for import errors in our dags."""
        import_errors = dagbag.import_errors
        msg = f'DAG failures detected! Got: {import_errors}'
        assert len(import_errors) == 0, msg

    def test_default_args_email(self, dagbag):
        """Test if email argument is provided."""
        for dag_id, dag in dagbag.dags.items():
            emails = dag.default_args.get('email', [])
            error_msg = (
                f'The mail {self.REQUIRED_EMAIL} for sending alerts '
                f'is missing from the DAG {dag_id}'
            )
            assert self.REQUIRED_EMAIL in emails, error_msg

    def test_time_import_dags(self, dagbag):
        """Test for loading time. Verify that DAGs load fast enough"""
        stats = dagbag.dagbag_stats
        slow_dags = list(
            filter(lambda f: f.duration > self.LOAD_SECOND_THRESHOLD, stats)
        )
        slow_dag_files = ', '.join(map(lambda f: f.file[1:], slow_dags))
        msg = (
            'The following DAGs take more than '
            f'{self.LOAD_SECOND_THRESHOLD}s to load: {slow_dag_files}'
        )
        assert len(slow_dags) == 0, msg

    def test_default_args_retries(self, dagbag):
        """Test if DAGs have the required number of retries."""
        for dag_id, dag in dagbag.dags.items():
            retries = dag.default_args.get('retries', None)
            msg = f'You must specify a number of retries in the DAG: {dag_id}'
            assert retries is not None, msg

    def test_default_args_retry_delay(self, dagbag):
        """Test if DAGs have the required retry_delay expressed in seconds."""
        for dag_id, dag in dagbag.dags.items():
            retry_delay = dag.default_args.get('retry_delay', None)
            msg = (
                'You must specify a retry delay '
                f'(seconds) in the DAG: {dag_id}'
            )
            assert retry_delay is not None, msg

    def test_number_of_dags(self, dagbag):
        """Test if there is the right number of DAGs in the dags folder."""
        stats = dagbag.dagbag_stats
        dag_num = sum([o.dag_num for o in stats])
        msg = (
            f'Wrong number of dags, {self.EXPECTED_NUMBER_OF_DAGS} '
            f'expected got {dag_num} (Can be due to cycles!)'
        )
        assert dag_num == self.EXPECTED_NUMBER_OF_DAGS, msg

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_dag_cycles(self, dag_file):
        """Test for dag cycles on each of dag in dags directory."""
        module_name, _ = os.path.splitext(dag_file)
        module_path = os.path.join(DAG_PATH, dag_file)
        mod_spec = importlib.util.spec_from_file_location(
            module_name, module_path)
        module = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(module)

        dag_objects = [var for var in vars(
            module).values() if isinstance(var, DAG)]
        assert dag_objects

        for dag in dag_objects:
            # Test cycles
            test_cycle(dag)
