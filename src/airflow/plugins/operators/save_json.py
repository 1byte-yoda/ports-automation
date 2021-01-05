from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import get_json_from_db


class LoadToJsonOperator(BaseOperator):
    ui_color = '#20D9DD'

    @apply_defaults
    def __init__(self, settings="", *args, **kwargs):
        super(LoadToJsonOperator, self).__init__(*args, **kwargs)
        self.settings = settings

    def execute(self, context):
        get_json_from_db()
