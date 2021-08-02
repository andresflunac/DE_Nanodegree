from processors.processor import Processor
import pandas as pd


class DemogProcessor(Processor):
    def load_data(self):
        return pd.read_csv(self.path, delimiter=";")

    def clean_data(self, df):
        df = self.remove_cities_with_nulls(df)
        df = df.drop_duplicates()
        return df

    def transform_data(self, df, spark):
        df = self.pivot_table_race(df)
        df = df.fillna(0)
        df = df.groupby(['State', 'State Code']).sum().reset_index()
        df = df.drop(columns=['Average Household Size', 'Median Age'])
        df = df.rename(columns={
            'State Code': 'state_id',
            'State': 'state',
            'Male Population': 'male',
            'Female Population': 'female',
            'Total Population': 'total',
            'Number of Veterans': 'veterans',
            'Foreign-born': 'foreign_born',
            'American Indian and Alaska Native': 'indian_native',
            'Asian': 'asian',
            'Black or African-American': 'african_american',
            'Hispanic or Latino': 'hispan',
            'White': 'white'
        })
        return spark.createDataFrame(df)

    def remove_cities_with_nulls(self, df):
        records_with_nulls = df[df.isnull().any(axis=1)].filter(['City', 'State']).drop_duplicates()
        keys = list(records_with_nulls.columns.values)
        i1 = df.set_index(keys).index
        i2 = records_with_nulls.set_index(keys).index
        return df[~i1.isin(i2)]

    def pivot_table_race(self, df):
        columns = [col_name for col_name in list(df.columns) if col_name not in ['Race', 'Count']]
        df = df.pivot_table('Count', columns, 'Race').reset_index()
        df.columns.name = ''
        return df
