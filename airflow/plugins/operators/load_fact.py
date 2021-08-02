from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 target_table: str,
                 fact_columns: str,
                 sql: str,
                 truncate: bool,
                 *args, **kwargs):
        """Initialize custom operator

        Args:
            target_table (str): Fact table name
            fact_columns (str): Fact column names
            sql (str): Second part of SQL INSERT statement
            truncate (bool):
                True: Delete-load functionality
                False: Append-only functionality
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.fact_columns = fact_columns
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        # Connect to Redshift
        logging.debug("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id="redshift_connection")
        # Truncate table (if applicable)
        if self.truncate:
            redshift.run(f"TRUNCATE {self.target_table}")
        # Run query
        self.log.info('Populating fact table: {target_table}')
        redshift.run(
            f"INSERT INTO {self.target_table} ({self.fact_columns}) {self.sql}"
        )
