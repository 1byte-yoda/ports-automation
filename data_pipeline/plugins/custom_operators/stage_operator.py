# data_pipeline/plugins/custom_operators/stage_operator.py


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageDatatoMongodbOperator(BaseOperator):
    """
    Airflow operator that acts as a placeholder for staging phase.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, settings="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = settings

    def execute(self, context):
        self.log.info("Executing Staging Operator.")
