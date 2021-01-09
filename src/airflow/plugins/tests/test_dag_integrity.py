import os
import glob
import importlib.util
from unittest import TestCase
from airflow import DAG
from airflow.models import DagBag


DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


class TestDagIntegrity(TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_objects = self._get_dag_objects()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_alert_email_present(self):
        for dag in self.dag_objects:
            emails = dag.default_args.get('email', [])
            msg = f'Alert email not set for DAG {dag.dag_id}'
            self.assertIn('unece_ports@yopmail.com', emails, msg)

    def test_dag_integrity(self):
        assert self.dag_objects
        for dag in self.dag_objects:
            dag.test_cycle()

    def _get_dag_objects(self):
        dag_objects = []
        for dag_file in DAG_FILES:
            module_name, _ = os.path.splitext(dag_file)
            module_path = os.path.join(DAG_PATH, dag_file)
            mod_spec = importlib.util.spec_from_file_location(
                module_name, module_path
            )
            module = importlib.util.module_from_spec(mod_spec)
            mod_spec.loader.exec_module(module)
            dag_objects = [var for var in vars(
                module).values() if isinstance(var, DAG)
            ]
        return dag_objects
