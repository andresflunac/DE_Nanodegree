# Project: Data Lake

## Context
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Objective
The main goal of the project is to build an ETL pipeline that extracts Sparkigy data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow Sarkify's analytics team to continue finding insights in what songs their users are listening to.

## Data
The ETL creates the following dimensional tables:
- ```songplays``` (fact table: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
- ```users```  (dimension table: user_id, first_name, last_name, gender, level)
- ```songs``` (dimension tabl: song_id, title, artist_id, year, duration)
- ```artists``` (dimension table: artist_id, name, location, lattitude, longitude)
- ```time``` (dimension table: start_time, hour, day, week, month, year, weekday)

![](images/ER-Diagram.png)

## Prerequisites
1. AWS EMR cluster (5.20.0+) with the following specifications:
- Spark Spark 2.4.7
- Valid key pair to set up SSH connections with the cluster
- Valid security group that allows SSH access on port 22
2. AWS S3 bucket reachable from AWS EMR
3. Python 3.6+
4. Python package pyspark

## Usage
1. SSH into the AWS EMR cluster using a valid key pair
2. If not already available, ```install pyspark via sudo /usr/bin/pip-3.6 install pyspark```
3. Edit ```dl.cfg```file:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- OUTPUT_DATA= Path of the output tables for the star schema (e.g. s3a///sparkify-star-schema/)
4. Execute the pipeline via ```/usr/bin/python3 etl.py```

## Testing
```Test_DataPartition.ipynb``` runs the ETL pipeline with a small portion of the original data (```./data/log-data.zip``` and ```./data/song-data```)