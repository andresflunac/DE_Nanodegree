from processors.processor import Processor


class InmigrationProcessor(Processor):
    def load_data(self, spark):
        return spark.read.parquet(self.path)

    def clean_data(self, df):
        df = df.selectExpr(
            "cicid as cicid",
            "i94yr as year",
            "i94mon as month",
            "i94res as country",
            "i94port as port",
            "visatype as visatype",
            "gender as gender",
            "arrdate as arradate",
            "depdate as depdate",
            "i94mode as mode",
            "i94addr as state_id"
        )
        return df

    def transform_data(self, df, spark):
        return df

    def run_pipeline(self, spark, table):
        """Execute steps tu run the pipeline
            load_data
            clean_data
            transform_data
            write_data

        Args:
            spark ([SparkSession]): Spark session
        """
        df = self.load_data(spark)
        df = self.clean_data(df)
        df = self.transform_data(df, spark)
        self.write_data(df, table)
