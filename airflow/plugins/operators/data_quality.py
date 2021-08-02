from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tests: list,
                 *args, **kwargs):
        """Initialize custom operator

        Args:
            tests (list): SQL test statements and expected results
                [
                    {'table':xxx, check_sql':xxx, 'expected_result': xxx},
                    {'table':xxx, check_sql':xxx, 'expected_result': xxx},
                    ...
                ]
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests

    def execute(self, context):
        # Connect to redshift
        redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
        for test in self.tests:
            # Retrieve information of the test
            table = test['table']
            sql_statement = test['check_sql']
            expected_result = test['expected_result']
            fixed_qry = f"{sql_statement} FROM {table}"
            logging.info(f"Running data quality check: {fixed_qry}")
            logging.info(f"Expected result: {expected_result}")

            # Check the if the query return any result
            res = redshift_hook.get_records(fixed_qry)
            logging.info(res)
            if len(res) < 1 or len(res[0]) < 1:
                raise ValueError(f"Data check failed. {table} returned no results")
            result = res[0][0]
            if result != int(expected_result):
                raise ValueError(f"Data check failed. Returned result: {result}")
