import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


#Read Values From Config
S3_LOG_DATA = config.get('S3','LOG_DATA')
DWH_IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
S3_SONG_DATA = config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS \"user\""
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
                    artist VARCHAR, 
                    auth VARCHAR, 
                    firstName VARCHAR, 
                    gender CHAR(1),
                    itemInSession  INTEGER,
                    lastName VARCHAR,
                    length DECIMAL,
                    level VARCHAR,
                    location VARCHAR,
                    method VARCHAR,
                    page VARCHAR,
                    registration FLOAT,
                    sessionId INTEGER,
                    song VARCHAR,
                    status INTEGER,
                    ts BIGINT,
                    userAgent VARCHAR,
                    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
                    artist_id VARCHAR,
                    artist_latitude DECIMAL,
                    artist_location VARCHAR,
                    artist_longitude DECIMAL,
                    artist_name VARCHAR,
                    duration DECIMAL,
                    num_songs INTEGER,
                    song_id VARCHAR,
                    title VARCHAR,
                    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INTEGER NOT NULL REFERENCES "user"(user_id),
        "level" VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "user"(
         user_id INTEGER SORTKEY PRIMARY KEY,
         first_name VARCHAR NOT NULL,
         gender CHAR(1),
         "level" VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
          song_id VARCHAR SORTKEY PRIMARY KEY,
          title VARCHAR NOT NULL,
          artist_id VARCHAR NOT NULL DISTKEY REFERENCES artist(artist_id),
          year INTEGER NOT NULL,
          duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
          artist_id VARCHAR SORTKEY PRIMARY KEY,
          name VARCHAR NOT NULL,
          location VARCHAR,
          latitude DECIMAL,
          longitude DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
         start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
         hour NUMERIC NOT NULL,
         day NUMERIC NOT NULL,
         week NUMERIC NOT NULL,
         month NUMERIC NOT NULL,
         year NUMERIC NOT NULL,
         weekday NUMERIC NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
         COPY staging_events
         FROM {}
         iam_role '{}'
         FORMAT AS json {}
""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
         COPY staging_songs
         FROM {}
         iam_role '{}'
         FORMAT AS json 'auto'
""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
        e.userId AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
     FROM staging_events e
     LEFT JOIN staging_songs s ON TRIM(LOWER(e.song)) = TRIM(LOWER(s.title))
     WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO "user"(user_id, first_name, gender, level)
    SELECT DISTINCT (userId) AS user_id,
               firstName AS first_name,
               gender,
               level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId NOT IN (SELECT DISTINCT user_id FROM "user")
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
     FROM staging_songs
     WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song)
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist)
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
           EXTRACT(hour FROM start_time) AS hour,
           EXTRACT(day FROM start_time) AS day,
           EXTRACT(week FROM start_time) AS week,
           EXTRACT(month FROM start_time) AS month,
           EXTRACT(year FROM start_time) AS year, 
           EXTRACT(dayofweek FROM start_time) AS weekday
     FROM staging_events e;
""")

analyze_queries = [
    'SELECT COUNT(*) AS total FROM song',
    'SELECT COUNT(*) AS total FROM songplay',
    'SELECT COUNT(*) AS total FROM artist',
    'SELECT COUNT(*) AS total FROM "user"',
    'SELECT COUNT(*) AS total FROM time_table',
    'SELECT COUNT(*) AS total FROM staging_events',
    'SELECT COUNT(*) AS total FROM staging_songs'
]

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    user_table_create,
    songplay_table_create]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop]

copy_table_order = ['staging_events','staging_songs']
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_order = ['artist','song','time_table','user','songplay']
insert_table_queries = [artist_table_insert, song_table_insert,  time_table_insert, user_table_insert, songplay_table_insert]
