timport configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS  (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR(1),
        item_in_session INT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        session_id INT,
        song VARCHAR,
        status INT,
        ts VARCHAR,
        user_agent VARCHAR,
        user_id INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR,
        num_songs INT,
        title VARCHAR,
        artist_name VARCHAR,
        artist_latitude FLOAT,
        year INT,
        duration FLOAT,
        artist_id VARCHAR,
        artist_longitude FLOAT,
        artist_location VARCHAR
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        ts TIMESTAMP REFERENCES time,
        user_id INT REFERENCES users,
        level VARCHAR NOT NULL,
        song_id VARCHAR REFERENCES songs,
        artist_id VARCHAR REFERENCES artists,
        session_id INT NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        ts TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON {}
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs from {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto'
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES
user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        user_id AS user_id,
        first_name AS first_name,
        last_name AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration   
    FROM staging_songs
    WHERE song_id IS NOT NULL
    
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        ts,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        real_ts AS ts,
        EXTRACT(hour FROM real_ts) AS hour,
        EXTRACT(day FROM real_ts) AS day,
        EXTRACT(week FROM real_ts) AS week,
        EXTRACT(month FROM real_ts) AS month,
        EXTRACT(year FROM real_ts) AS year,
        EXTRACT(dow FROM real_ts) AS weekday
        FROM (
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS real_ts
            FROM staging_events
            WHERE ts IS NOT NULL
        )
""")

songplay_table_insert = ("""
    INSERT INTO songplays (
        ts,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS ts,
        e.user_id AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.session_id AS session_id,
        e.location AS location,
        e.user_agent AS user_agent  
    FROM staging_events e
    JOIN staging_songs s
    ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong'
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
