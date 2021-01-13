# data_pipeline/tests/custom_operators/test_stage_operator.py


from airflow.models import BaseOperator
from plugins.custom_operators.stage_operator import (
    StageDatatoMongodbOperator
)


class TestStageOperator:
    def test_stage_operator(self):
        task = StageDatatoMongodbOperator(task_id='foo')
        task.execute({})
        assert isinstance(task, BaseOperator)
