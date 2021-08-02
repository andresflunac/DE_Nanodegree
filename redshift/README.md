# Project Data Warehouse
## Context
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in public S3 buckets, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app:
- ```LOG_DATA```: s3://udacity-dend/log_data
- ```LOG_JSONPATH```: s3://udacity-dend/log_json_path.json
- ```SONG_DATA```: s3://udacity-dend/song_data
## Project Objective
Tha main goal of the project is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. Since star schema is an effective option to store data when fast queries with few joins are required, the following project will implement that design.
## Data Architecture
The following ETL creates and populates 7 tables:
### Staging tables
Used to stage the information of the S3 buckets without any transformation.
- ```staging_events```: Information about the events of music listening.
- ```staging_songs```: Information about the songs
### Star Schema
Used to store the information following a star schema:
- ```songplays``` (fact table)
- ```users```  (dimension table)
- ```songs``` (dimension table)
- ```artists``` (dimension table)
- ```time``` (dimension table)
## Prerequisites
1. AWS Redshift running instance with the following features:
- Create/Drop/Insert/Update rights.
- Associated AWS IAM Role to allow S3 reading rigths (```AmazonS3ReadOnlyAccess```).
- Associated AWS Security group to connect to Redshift through internet.
2. Python 3.6+
3. Python package ```psycopg2```
4. Unix-like environment (Linux, macOS, WSL on Windows)
## Usage
1. Edit ```dwh.cfg``` file:
- ```HOST```: Redshift endpoint
- ```DB_NAME```: Database name
- ```DB_USER```: Database username
- ```DB_PASSWORD```: Database password
- ```DB_PORT```: Database port (5439)
- ```ARN```: ARN of the IAM role

2. Run ```create_tables.py``` script to create the 7 tables used to store data. The script will drop the tables in case they already exist.
3. Run ```etl.py``` script to populate the tables. This file will execute two functions:
- ```load_staging_tables```: Copy information from S3 buckets to staging tables
- ```insert_tables```: Populate star schema tables from staging tables executing the respective transformations.

## Query Examples
### Locations where more songs were played 
```SELECT location, COUNT(*) AS songs_played FROM songplays GROUP BY location ORDER BY songs_played DESC LIMIT 5```  

San Francisco-Oakland-Hayward, CA (41)  
Portland-South Portland, ME	(31)  
Lansing-East Lansing, MI	(28)  
Waterloo-Cedar Falls, IA	(20)  
Tampa-St. Petersburg-Clearwater, FL	(18)

### Users who listened more songs  
```SELECT u.user_id as ID, u.first_name AS NAME, u.last_name AS LASTNAME, COUNT(*) as TOTAL FROM songplays s JOIN users u ON s.user_id = u.user_id GROUP BY ID,NAME,LASTNAME ORDER BY TOTAL DESC LIMIT 5```

Chloe Cuevas (82)  
Tegan Levine (62)  
Mohammad Rodriguez (34)  
Lily Koch (30)  
Kate Harrell (28)