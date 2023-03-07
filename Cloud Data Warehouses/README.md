# Udacity Project2: Create a Data Warehouse with AWS Redshift


## Introduction

---

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 
As their data engineer, the mission is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 



## ETL Pipeline

---

1. Load song and log data from S3 buckets.
2. Stage the loaded data.
3. Create Data tables according to each Database schema
4. Execute SQL statements that create the analytics tables from these staging tables.




## Project Datasets

---

There are 3 datasets that reside in S3:
1. Song data : s3://udacity-dend/song_data
2. Log data : s3://udacity-dend/log_data
3. meta info : s3://udacity-dend/log_json_path.json (to correctly load Log data)


## Database Schema

---

- staging_events : staging table for event data
- staging_songs : staging table for song data
- songplays : all the events about songplays (e.g. who listened which songs, when, and which artist plays that song...)
- users : user information (e.g. name, gender..)
- songs : song information (e.g. artist, year, duration..)
- artists : artist name and location
- times : detailed time info regarding timestamps
