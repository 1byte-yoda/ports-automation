import unittest
from airflow.models import DagBag


class TestUneceDagDependencies(unittest.TestCase):
    """Check UneceDAG expectation"""

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'unece_data_pipeline'
        self.task_ids = [
            'Begin_execution', 'Scrape_unece_ports_data',
            'Stage_to_mongodb',
            'Transform_and_load_to_postgres',
            'Run_data_quality_checks',
            'Load_to_analytics_dw',
            'Export_to_json',
            'Send_notification_email',
            'Send_notification_slack',
            'End_execution'
        ]

    def test_task_count(self):
        """Check task count of the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 10)

    def test_contain_tasks(self):
        """Check task contains in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(
            task_ids, self.task_ids
        )

    def test_dependencies_of_begin_execution_task(self):
        """Check the task dependencies of Begin_execution in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        begin_execution = dag.get_task('Begin_execution')
        upstream_task_ids = list(
            map(lambda task: task.task_id, begin_execution.upstream_list)
        )
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(
            map(lambda task: task.task_id, begin_execution.downstream_list)
        )
        self.assertListEqual(downstream_task_ids, ['Scrape_unece_ports_data'])

    def test_dependencies_of_scrape_unece_ports_data_task(self):
        """Check the task dependencies of Scrape_unece_ports_data in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        scrape_unece_ports_data = dag.get_task('Scrape_unece_ports_data')
        upstream_task_ids = list(
            map(
                lambda task: task.task_id,
                scrape_unece_ports_data.upstream_list
            )
        )
        self.assertListEqual(upstream_task_ids, ['Begin_execution'])
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                scrape_unece_ports_data.downstream_list
            )
        )
        self.assertListEqual(downstream_task_ids, ['Stage_to_mongodb'])

    def test_dependencies_of_Stage_to_mongodb_task(self):
        """Check the task dependencies of Stage_to_mongodb in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        stage_to_mongodb = dag.get_task('Stage_to_mongodb')
        upstream_task_ids = list(
            map(
                lambda task: task.task_id,
                stage_to_mongodb.upstream_list
            )
        )
        self.assertListEqual(upstream_task_ids, ['Scrape_unece_ports_data'])
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                stage_to_mongodb.downstream_list
            )
        )
        self.assertListEqual(
            downstream_task_ids,
            ['Transform_and_load_to_postgres']
        )

    def test_dependencies_of_Run_data_quality_checks_task(self):
        """Check the task dependencies of Run_data_quality_checks in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        run_data_quality_checks = dag.get_task('Run_data_quality_checks')
        upstream_task_ids = list(
            map(
                lambda task: task.task_id,
                run_data_quality_checks.upstream_list
            )
        )
        self.assertListEqual(upstream_task_ids, [
                             'Transform_and_load_to_postgres'])
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                run_data_quality_checks.downstream_list
            )
        )
        self.assertListEqual(
            sorted(downstream_task_ids),
            sorted(['Load_to_analytics_dw', 'Export_to_json'])
        )

    def test_dependencies_of_Load_to_analytics_dw_task(self):
        """Check the task dependencies of Load_to_analytics_dw in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        load_to_analytics_dw = dag.get_task('Load_to_analytics_dw')
        upstream_task_ids = list(
            map(lambda task: task.task_id, load_to_analytics_dw.upstream_list)
        )
        self.assertListEqual(upstream_task_ids, ['Run_data_quality_checks'])
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                load_to_analytics_dw.downstream_list
            )
        )
        self.assertListEqual(
            sorted(downstream_task_ids),
            sorted(['Send_notification_email', 'Send_notification_slack'])
        )

    def test_dependencies_of_Export_to_json_task(self):
        """Check the task dependencies of Export_to_json in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        export_to_json = dag.get_task('Export_to_json')
        upstream_task_ids = list(
            map(lambda task: task.task_id, export_to_json.upstream_list)
        )
        self.assertListEqual(upstream_task_ids, ['Run_data_quality_checks'])
        downstream_task_ids = list(
            map(lambda task: task.task_id, export_to_json.downstream_list)
        )
        self.assertListEqual(
            sorted(downstream_task_ids),
            sorted(['Send_notification_slack', 'Send_notification_email'])
        )

    def test_dependencies_of_Send_notification_slack_task(self):
        """Check the task dependencies of Send_notification_slack in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        send_notification_slack = dag.get_task('Send_notification_slack')
        upstream_task_ids = list(
            map(
                lambda task: task.task_id,
                send_notification_slack.upstream_list
            )
        )
        self.assertListEqual(
            sorted(upstream_task_ids),
            sorted(['Load_to_analytics_dw', 'Export_to_json'])
        )
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                send_notification_slack.downstream_list
            )
        )
        self.assertListEqual(
            sorted(downstream_task_ids),
            sorted(['End_execution'])
        )

    def test_dependencies_of_Send_notification_email_task(self):
        """Check the task dependencies of Send_notification_email in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        send_notification_email = dag.get_task('Send_notification_email')
        upstream_task_ids = list(
            map(
                lambda task: task.task_id,
                send_notification_email.upstream_list
            )
        )
        self.assertListEqual(
            sorted(upstream_task_ids),
            sorted(['Load_to_analytics_dw', 'Export_to_json'])
        )
        downstream_task_ids = list(
            map(
                lambda task: task.task_id,
                send_notification_email.downstream_list
            )
        )
        self.assertListEqual(
            sorted(downstream_task_ids),
            sorted(['End_execution'])
        )

    def test_dependencies_of_End_execution_task(self):
        """Check the task dependencies of End_execution in the dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        end_execution = dag.get_task('End_execution')
        upstream_task_ids = list(
            map(lambda task: task.task_id, end_execution.upstream_list)
        )
        self.assertListEqual(
            sorted(upstream_task_ids),
            sorted(['Send_notification_email', 'Send_notification_slack'])
        )
        downstream_task_ids = list(
            map(lambda task: task.task_id, end_execution.downstream_list)
        )
        self.assertListEqual(downstream_task_ids, [])
