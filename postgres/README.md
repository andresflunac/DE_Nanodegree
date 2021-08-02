# Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description
The goal of this project is to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Data Model
Star schema is the simplest style of a data mart schema and is the approach most widely used to develop data warehouses and dimensional data marts. The star schema consists of one or more fact tables referencing any number of dimension tables. In this example, Sparkify required the following design:
### Dimension Tables: 
- users: users in the app (user_id, first_name, last_name, gender, level)
- songs :songs in music database (song_id, title, artist_id, year, duration)
- artists: artists in music database (artist_id, name, location, latitude, longitude)
- time: timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)
### Fact Table:
- songplays: records in log data associated with song plays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Given that star schemas are denormalized, Sparkify can take advantage of this desing to make simpler queries and retrieve relevant information according to their needs. The 'songplays' fact table allows the company to easily and rapidly retrieve how many users have listened specific songs, or which locations have listened specific songs of specific artists.

## ETL Pipeline:
The designed pipeline has three main functions:
1. process_data: Iterate over all the Sparkify files. There are two dataset types: Song dataset (Information about artists and songs), and Log dataset (Activiy logs from the music streaming app).
2. process_log_file: Process log file to populate 'users' and 'time' dimension tables, as well as 'songplays' fact table.
3. process_song_file: Process song file to populate 'songs' and 'artists' dimension tables.

## Intructions
For local execution you can use PostgreeSQL docker:

- Download docker image:
*docker pull postgres*

- Execute docker:
*docker run --name postgres --rm -p 5432:5432 -e -d postgres*

- Connect string:
*"host=127.0.0.1 dbname=postgres user=postgres password=postgres port=5432"*

Then, you can execute the scripts in the following order:
1. create_tables.py: Deletes current database and create empty tables
2. etl.py: Populates the tables
