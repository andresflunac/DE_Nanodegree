from processors.processor import Processor
import pandas as pd


class AirportProcessor(Processor):
    def load_data(self):
        return pd.read_csv(self.path)

    def clean_data(self, df):
        df = df.filter(["ident", 'type', 'iso_region', 'iso_country'])
        df = df.drop_duplicates()
        return df

    def transform_data(self, df, spark):
        df = df[df["iso_country"] == "US"]
        df['State'] = df['iso_region'].str.replace("US-", "")
        df = df.drop(columns=["iso_region", "ident", "iso_country"])
        df = self.pivot_table_type(df)
        df = df.rename(columns={'State': 'state_id'})
        return spark.createDataFrame(df)

    def pivot_table_type(self, df):
        df['Count'] = 1
        df = df.groupby(['State', 'type']).count().reset_index()
        df = df.pivot_table('Count', "State", 'type').reset_index()
        df = df.fillna(0)
        df.columns.name = ''
        return df
