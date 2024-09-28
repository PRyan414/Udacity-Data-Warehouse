import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
REGION = config.get("S3", "REGION")
SONG_JSON = config.get("S3", "SONG_JSON")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES - ALL (staging and star schema)

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events(    
    artist TEXT,
    auth TEXT,
    first_name TEXT,  
    gender TEXT,
    item_in_session INTEGER, 
    last_name TEXT,  
    length FLOAT,  
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    session_id INTEGER, 
    song TEXT,
    status DECIMAL,
    ts BIGINT,
    user_agent TEXT,  
    user_id INTEGER,
    PRIMARY KEY (session_id, ts)
    );""")

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR(255),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        duration FLOAT,
        year INT
        );
        """)

songplay_table_create = (
    """CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(10),
        song_id VARCHAR(256),
        artist_id VARCHAR(256),
        session_id INTEGER,
        location TEXT,
        user_agent TEXT
        );""")

user_table_create = (
    """CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR(256),
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level VARCHAR
        );""")

song_table_create = (
    """CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(256),
        title VARCHAR(256),
        artist_id VARCHAR(256),
        year INTEGER,
        duration DECIMAL
        );""")

artist_table_create = (
    """CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(256),
        artist_name VARCHAR(256),
        artist_location TEXT,
        artist_latitude DECIMAL(9,6),
        artist_longitude DECIMAL(9,6)
        );""")

time_table_create = (
    """CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
        );""")

# STAGING TABLES - COPY QUERIES (COPY DATA FROM BUCKETS INTO TABLES)

staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON '{}'
    REGION '{}'
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON '{}'
    REGION '{}'
""").format(SONG_DATA, ARN, SONG_JSON, REGION)


# FINAL TABLES - INSERT DATA INTO STAR SCHEMA FACT AND DIM TABLES FROM STAGING TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
           SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
           e.user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.session_id,
           e.location,
           e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
    """)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL;
    """)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    """)

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    
        SELECT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(WEEKDAY FROM start_time) AS weekday
    FROM staging_events e;
    """)

# QUERY LISTS - ACCUMULATE QUERIES IN LISTS; LISTS ARE IMPORTED INTO OTHER SCRIPTS AND EXECUTED SEQUENTIALLY.

# CREATE TABLES LIST
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# DROP TABLES LIST
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# STAGING TABLES DATA COPY FROM BUCKETS; used in etl.py
copy_table_queries = [staging_events_copy, staging_songs_copy]

# STAR SCHEMA TABLES; DATA INSERTED FROM STAGING TABLES
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# LIST OF TABLES
# all_tables = [staging_events_copy, staging_songs_copy, songplay_table_insert]

# DATA DISCOVERY QUERIES
count_query = (
    """
    SELECT s.title, count(sp.song_id) as Song_Count
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    GROUP BY s.title, sp.song_id
    ORDER BY COUNT(sp.song_id) DESC
    LIMIT 10;
    """)

table_count = (
    """
    SELECT 
      (SELECT COUNT(*) FROM staging_events),
      (SELECT COUNT(*) FROM staging_songs),
      (SELECT COUNT(*) FROM songplays),
      (SELECT COUNT(*) FROM songs),
      (SELECT COUNT(*) FROM users),
      (SELECT COUNT(*) FROM artists),
      (SELECT COUNT(*) FROM time);
    """)

# RE-FIGURE THIS LOGIC; SEE RUBRIC
# user_time_count = (
#     """
#     SELECT start_time, COUNT(start_time) AS Popular_Time
#     FROM time
#     GROUP BY start_time
#     ORDER BY Popular_Time DESC
#     LIMIT 10;
#     """
# )

# DATA QUERIES list
data_queries_list = [count_query, table_count]
