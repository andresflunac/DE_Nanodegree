import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def insert_row(cur, query, df, columns):
    """Insert row into a DB table taking first row of a DataFrame

    Args:
        cur : Connection cursor
        query (str): Insert query statement
        df (pd.DataFrame): Dataframe with row information
        columns (list(str)): Names of the columns of the row
    """
    row_data = df[columns].values[0].tolist()
    try:
        cur.execute(query, row_data)
    except psycopg2.Error as e:
        print("Error: Inserting rows")
        print(e)


def insert_df(cur, query, df):
    """Insert DataFrame rows in a database table

    Args:
        cur : Connection cursor
        query (str): Insert query statement
        df (pd.DataFrame): Dataframe with the information
    """
    for i, row in df.iterrows():
        try:
            cur.execute(query, list(row))
        except psycopg2.Error as e:
            print("Error: Inserting rows")
            print(e)


def select_qry(cur, query, params):
    """Execute SELECT query and retrieve results

    Args:
        cur : Connection cursor
        query (str): Query select statement
        params (tuple): Query parameters

    Returns:
        [tuple]: Query results
    """
    try:
        cur.execute(query, params)
    except psycopg2.Error as e:
        print("Error: Retrieving data")
        print(e)

    results = cur.fetchone()
    return results


def get_time_df(df):
    """Process DataFrame to return time DataFrame

    Args:
        df (pd.DataFrame): DataFrame with information

    Returns:
        [pd.DataFrame]: Time DataFrame
    """
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (
        df['ts'], t.dt.week, t.dt.hour,
        t.dt.day, t.dt.year, t.dt.month,
        t.dt.weekday)

    column_labels = (
        "start_time", "week", "hour", "day",
        "year", "month", "weekday")

    time_df = pd.DataFrame({k: v for k, v in zip(column_labels, time_data)})

    return time_df


def process_song_file(cur, filepath):
    """Process song file to populate 'songs' and 'artists' dimension tables

    Args:
        cur : Connection cursor
        filepath (str): Filepath of the .json file
    """
    df = pd.read_json(filepath, lines=True)

    # Insert song record
    insert_row(
        cur=cur,
        query=song_table_insert,
        df=df,
        columns=[
            'song_id', 'title', 'artist_id', 'year', 'duration'
        ]
    )

    # Insert artist record
    insert_row(
        cur=cur,
        query=artist_table_insert,
        df=df,
        columns=[
            "artist_id", "artist_name", "artist_location",
            "artist_latitude", "artist_longitude"
        ]
    )


def process_log_file(cur, filepath):
    """Process log file to populate 'users' and 'time' dimension tables.
    Process log file to populate 'songplays' fact table.

    Args:
        cur : Connection cursor
        filepath (str): Filepath of the .json file
    """
    df = pd.read_json(filepath, lines=True)

    # Filter by NextSong action
    df = df.loc[df.page == "NextSong"]

    # Insert time records
    time_df = get_time_df(df=df)
    insert_df(cur=cur, query=time_table_insert, df=time_df)

    # Insert user records
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    insert_df(cur=cur, query=user_table_insert, df=user_df)

    # Insert songplay records
    for index, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        results = select_qry(
            cur, song_select, (row.song, row.artist, row.length))

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Build songplay records
        songplay_data = (
            row.ts, row.userId, row.level, songid,
            artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Inserting records")
            print(e)


def process_data(cur, conn, filepath, func):
    """Process all data files to populate database tables

    Args:
        cur : Connection cursor
        conn : Database connection
        filepath (str): Directory with .json files
        func : Processing function (process_log_file / process_song_file)
    """
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # Connect to the database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the postgres database")
        print(e)

    # Get connection cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the database")
        print(e)

    # Process song and log files to populate tables
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
