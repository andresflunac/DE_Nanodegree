from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


def create_spark_session(config):
    """Initialize a Spark session

    Returns:
        [SparkSession]: Spark session
    """
    spark = SparkSession \
        .builder \
        .appName('US_Inmigration_Analysis_2016') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key",
        config.get('AWS', 'AWS_ACCESS_KEY_ID'))
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key",
        config.get('AWS', 'AWS_SECRET_ACCESS_KEY'))
    return spark


class Processor(ABC):
    def __init__(self, path, config):
        self.path = path
        self.output = config.get('PATHS', 'OUTPUT_PATH')
        self.access = config.get('AWS', 'AWS_ACCESS_KEY_ID')
        self.secret = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
        self.redshift_url = config.get('AWS', 'REDSHIFT_URL')
        self.redshift_port = config.get('AWS', 'REDSHIFT_PORT')
        self.redshift_dev = config.get('AWS', 'REDSHIFT_DB')
        self.redshift_user = config.get('AWS', 'DBUSER')
        self.redshift_pass = config.get('AWS', 'DBPASS')

    @abstractmethod
    def load_data(self):
        """Steps to load source data
        """
        pass

    @abstractmethod
    def clean_data(self):
        """Steps to clean source data
        """
        pass

    @abstractmethod
    def transform_data(self):
        """Steps to transform source data
        """
        pass

    def write_data(self, df, table):
        """Steps to write final table
        """
        conn = f"{self.redshift_url}:{self.redshift_port}/{self.redshift_dev}"
        df.write.format('jdbc').options(
            url=conn,
            driver='com.amazon.redshift.jdbc42.Driver',
            dbtable=table,
            user=self.redshift_user,
            password=self.redshift_pass).mode('append').save()

    def run_pipeline(self, spark, table):
        """Execute steps tu run the pipeline
            load_data
            clean_data
            transform_data
            write_data

        Args:
            spark ([SparkSession]): Spark session
        """
        df = self.load_data()
        df = self.clean_data(df)
        df = self.transform_data(df, spark)
        self.write_data(df, table)
