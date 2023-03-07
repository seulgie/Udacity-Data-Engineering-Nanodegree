import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts NUMERIC,
        userAgent VARCHAR,
        userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR NOT NULL,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
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
    CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP PRIMARY KEY,
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
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    FORMAT as JSON {}
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH')) 

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    FORMAT as JSON 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))



# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplay (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        SELECT DISTINCT se.ts AS start_time,
                        se.userId AS user_id,
                        se.level AS level,
                        ss.song_id AS song_id,
                        ss.artist_id AS artist_id,
                        se.sessionId AS session_id,
                        se.location AS location,
                        se.userAgent AS user_agent
        FROM staging_events AS se
        JOIN staging_songs ss 
        ON se.song = ss.title AND se.artist = ss.artist_name 
        WHERE se.page = 'NextSong';                        
""")



user_table_insert = ("""
        INSERT INTO users (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )
        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender AS gender,
                        level AS level 
        FROM staging_events 
        WHERE userId IS NOT NULL AND page = 'NextSong';
""")



song_table_insert = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration 
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")



artist_table_insert = ("""
        INSERT INTO artists (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
        SELECT DISTINCT artist_id,
                        artist_name AS name,
                        artist_location AS location,
                        artist_latitude AS latitude,
                        artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")



time_table_insert = ("""
        INSERT INTO times (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT ts AS start_time,
                        EXTRACT(hour from ts) AS hour,
                        EXTRACT(day from ts) AS day,
                        EXTRACT(week from ts) AS week,
                        EXTRACT(month from ts) AS month,
                        EXTRACT(year from ts) AS year,
                        EXTRACT(weekday from ts) AS weekday
        FROM staging_events
        WHERE page = 'NextSong';                        
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
