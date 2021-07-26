import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType


@udf(TimestampType())
def get_timestamp(ts):
    """Convert StringType to TimeStampType

    Args:
        ts ([StringType]): Initial timestamp

    Returns:
        [TimestampType]: Final timestamp
    """
    return datetime.fromtimestamp(ts/1e3)


def create_spark_session():
    """Initialize a Spark session

    Returns:
        [SparkSession]: Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def select_data(df, columns, pkey):
    """Select columns of a Spark DataFrame

    Args:
        df (DataFrame): Initial DataFrame
        columns (list(str)): Column names
        pkey (list[str]): Primary key column names

    Returns:
        [DataFrame]: Selected DataFrame
    """
    return df.select(columns).dropDuplicates(pkey)


def write_data(df, output_folder, pt_cols=None):
    """Write parquet files parititioned by specific columns

    Args:
        df ([DataFrame]): DataFrame used for generating parquet files
        output_folder ([str]): Output path of parquet files
        part_cols ([list(str)], optional): Columns of the partition.
        Defaults to None.
    """
    if pt_cols:
        df.write.partitionBy(pt_cols).parquet(output_folder, mode='overwrite')
    else:
        df.write.parquet(output_folder, mode='overwrite')


def process_time_data(df, ts_col_name):
    """Extract month, year, day, hour, week and weekday of a timestamp

    Args:
        df (DataFrame): Base DataFrame
        ts_column_name ([str]): Name of the column with the timestamp

    Returns:
        [DataFrame]: Transformed DataFrame
    """
    df = df.withColumn('month', month(get_timestamp(ts_col_name)))
    df = df.withColumn('year', year(get_timestamp(ts_col_name)))
    df = df.withColumn('day', dayofmonth(get_timestamp(ts_col_name)))
    df = df.withColumn('hour', hour(get_timestamp(ts_col_name)))
    df = df.withColumn('week', weekofyear(get_timestamp(ts_col_name)))
    df = df.withColumn('weekday', date_format(get_timestamp(ts_col_name), "u"))
    return df


def create_songplays_table(spark, song_df, log_df):
    """Join song and log DataFrames to create Songplays DataFrame

    Args:
        spark ([SparkSession]): Spark session
        song_df ([DataFrame]): DataFrame with song data
        log_df ([DataFrame]): DataFrame with log data

    Returns:
        [DataFrame]: Songplays DataFrame
    """
    # Select columns of interest of both datasets
    songplays_columns_song = ['artist_id', 'artist_name', 'song_id', 'title']
    songplays_columns_log = [
        'ts', 'userId', 'level', 'song', 'artist',
        'sessionId', 'location', 'userAgent'
        ]
    filtered_song = song_df.select(songplays_columns_song)
    filtered_log = log_df.select(songplays_columns_log)

    # Add aditional columns to log dataset
    filtered_log = filtered_log.withColumn('month', month(get_timestamp('ts')))
    filtered_log = filtered_log.withColumn('year', year(get_timestamp('ts')))

    # Join tables
    filtered_song.createOrReplaceTempView("song_view")
    filtered_log.createOrReplaceTempView("log_view")
    songplays_table = spark.sql(
        '''
            SELECT
                l.ts as ts,
                l.userId as user_id,
                l.level as level,
                s.song_id as song_id,
                s.artist_id as artist_id,
                l.sessionId as session_id,
                l.location as location,
                l.userAgent as user_agent,
                l.month as month,
                l.year as year
            FROM log_view l
            JOIN song_view s
            ON l.song = s.title AND l.artist = s.artist_name
        '''
    )
    songplays_table = songplays_table.withColumn(
        "songplay_id",
        monotonically_increasing_id()
        )
    return songplays_table


def process_song_data(spark, input_data, output_data):
    """Process song data files to create Songs and Artist dimension tables

    Args:
        spark ([SparkSession]): Spark session
        input_data ([type]): [description]
        output_data ([type]): [description]
    """
    # Read song data files
    song_json_path = input_data + "/song_data/*/*/*"
    df = spark.read.json(song_json_path)

    # Create Songs table
    songs_table_folder = output_data + "/songs_table"
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_primary_key = ['song_id']
    partition_columns = ["year", "artist_id"]
    song_table = select_data(df, song_columns, song_primary_key)
    write_data(song_table, songs_table_folder, partition_columns)

    # Create Artists table
    artists_table_folder = output_data + "/artists_table"
    artist_columns = [
        'artist_id', 'artist_name', 'artist_location',
        'artist_latitude', 'artist_longitude']
    artist_primary_key = ['artist_id']
    artist_table = select_data(df, artist_columns, artist_primary_key)
    write_data(artist_table, artists_table_folder)


def process_log_data(spark, input_data, output_data):
    """Process log data files to create Users and Time dimension tables
    Process log data to create Songplays fact table

    Args:
        spark ([type]): [description]
        input_data ([type]): [description]
        output_data ([type]): [description]
    """
    # Read log data files
    log_json_path = input_data + "/log_data"
    df = spark.read.json(log_json_path).where("page == 'NextSong'")

    # Create Users table
    users_table_folder = output_data + "/users_table"
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_primary_key = ['userId']
    users_table = select_data(df, user_columns, user_primary_key)
    write_data(users_table, users_table_folder)

    # Create Time table
    time_table_folder = output_data + "/time_table"
    time_columns = ['ts']
    time_primary_key = ['ts']
    partition_columns = ["year", "month"]
    time_table = select_data(df, time_columns, time_primary_key)
    time_table = process_time_data(time_table, 'ts')
    write_data(time_table, time_table_folder, partition_columns)

    # Create Songplays table
    songplays_table_folder = output_data + "/songplays_table"
    song_json_path = input_data + "/song_data/*/*/*"
    partition_columns = ["year", "month"]
    song_df = spark.read.json(song_json_path)
    songplays_table = create_songplays_table(spark, song_df, df)
    write_data(songplays_table, songplays_table_folder, partition_columns)


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    #input_data = "./data"
    output_data = "./spark-warehouse"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
