from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
import logging


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_statement = """
    COPY {target_table} from '{s3_bucket}/{s3_key}'
    ACCESS_KEY_ID '{access_key_id}'
    SECRET_ACCESS_KEY '{secret_access_key}'
    REGION '{s3_region}'
    COMPUPDATE OFF
    JSON '{json_path}'
    """

    @apply_defaults
    def __init__(self,
                 target_table: str,
                 s3_bucket: str,
                 s3_key: str,
                 s3_region: str,
                 s3_json_path: str,
                 *args, **kwargs):
        """[summary]

        Args:
            target_table (str): [description]
            s3_bucket (str): [description]
            s3_key (str): [description]
            s3_region (str): [description]
            s3_json_path (str): [description]
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.s3_json_path = s3_json_path
        self.postgres_conn_id = kwargs['postgres_conn_id']
        self.aws_conn_id = kwargs['aws_conn_id']

    def execute(self, context):
        # Create S3Hook to get credentials
        connection = BaseHook.get_connection(self.aws_conn_id)
        logging.info(connection)
        # Format SQL statement
        query = StageToRedshiftOperator.copy_statement.format(
            target_table=self.target_table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            s3_region=self.s3_region,
            s3_json_path=self.s3_json_path,
            access_key_id=connection.login,
            secret_access_key=connection.password
        )
        # Connect to Redshift
        # logging.debug("Connecting to Redshift")
        # redshift = PostgresHook(postgres_conn_id='redshift_connection')
        # Run query
        # logging.info(f"Staging data to {self.target_table}")
        # redshift.run(query)
