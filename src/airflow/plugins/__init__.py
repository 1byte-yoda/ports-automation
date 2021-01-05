from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators


class UnecePlugin(AirflowPlugin):
    name = "unece_plugin"
    operators = [
        operators.WebScraperOperator,
        operators.StageDatatoMongodb,
        operators.DataQualityCheckOperator,
        operators.LoadToMasterdbOperator,
        operators.LoadToJsonOperator
    ]
