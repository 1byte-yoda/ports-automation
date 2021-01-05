from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageDatatoMongodb(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, settings="", *args, **kwargs):
        super(StageDatatoMongodb, self).__init__(*args, **kwargs)
        self.settings = settings

    def execute(self, context):
        pass
