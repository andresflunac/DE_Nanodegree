from processors.processor import create_spark_session
from processors.demog_processor import DemogProcessor
from processors.airport_processor import AirportProcessor
from processors.inmigration_processor import InmigrationProcessor
import configparser

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.cfg')

    spark = create_spark_session(config)

    # Create demographics dimension table
    demographics = DemogProcessor(
        path=config.get('PATHS', 'DEMOGRAPHICS_PATH'),
        config=config
    )
    demographics.run_pipeline(spark, "demographics")

    # Create airports dimension table
    airports = AirportProcessor(
        path=config.get('PATHS', 'AIRPORTS_PATH'),
        config=config
    )
    airports.run_pipeline(spark, "airports")

    # Create inmigrations fact table
    processor = InmigrationProcessor(
        path=config.get('PATHS', 'INMIGRATIONS_PATH'),
        config=config
    )
    processor.run_pipeline(spark, "inmigrations")
