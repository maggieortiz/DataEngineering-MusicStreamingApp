import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                    artist VARCHAR,
                                    auth VARCHAR(255),
                                    first_name VARCHAR(255),
                                    gender VARCHAR(1),
                                    item_in_session INTEGER,
                                    last_name VARCHAR(255),
                                    length FLOAT,
                                    level VARCHAR(255),
                                    location VARCHAR(255),
                                    method VARCHAR(255),
                                    page VARCHAR(255),
                                    registration FLOAT,
                                    session_id BIGINT,
                                    song VARCHAR(255),
                                    status INTEGER,
                                    ts BIGINT,
                                    user_agent VARCHAR(255),
                                    user_id INTEGER);                                 
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                            
                            num_songs INTEGER,
                            artist_id VARCHAR(255),
                            artist_latitude FLOAT,
                            artist_longitude FLOAT,
                            artist_location VARCHAR(255),
                            artist_name VARCHAR(255),
                            song_id VARCHAR,
                            title VARCHAR(255),
                            duration FLOAT,
                            year FLOAT);                     
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id INTEGER IDENTITY(1,1) NOT NULL PRIMARY KEY, 
                                start_time TIMESTAMP NOT NULL, 
                                user_id INTEGER, 
                                level VARCHAR, 
                                song_id VARCHAR, 
                                artist_id VARCHAR, 
                                session_id INTEGER, 
                                location VARCHAR, 
                                user_agent VARCHAR
                                )
                                DISTSTYLE KEY
                                DISTKEY (start_time)
                                SORTKEY(start_time);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id INTEGER PRIMARY KEY NOT NULL, 
                            first_name VARCHAR(225), 
                            last_name VARCHAR(225), 
                            gender VARCHAR(1) ENCODE BYTEDICT,
                            level VARCHAR ENCODE BYTEDICT
                            )
                            SORTKEY(user_id);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY NOT NULL, 
                        title VARCHAR(255),
                        artist_id VARCHAR(255),
                        year INTEGER, 
                        duration FLOAT
                        )
                        SORTKEY(song_id);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id VARCHAR PRIMARY KEY NOT NULL,
                            name VARCHAR(255) ,
                            location VARCHAR(255),
                            latitude FLOAT,
                            longitude FLOAT
                            )
                            SORTKEY(artist_id);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time TIMESTAMP PRIMARY KEY, 
                            hour INTEGER,
                            day INTEGER,
                            week INTEGER,
                            month INTEGER,
                            year INTEGER,
                            weekday VARCHAR(20)
                            )
                            DISTSTYLE KEY
                            DISTKEY(start_time)
                            SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
            COPY staging_events
            FROM {}
            iam_role {}
            FORMAT AS json {};
            """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

#staging_songs_copy = (""" copy staging_songs from 's3://udacity-dend/song_data/A/A/A'
#                            credentials 'aws_iam_role={}'
 #                           format as json 'auto' 
  #                          compupdate off region 'us-west-2';
#""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
        COPY staging_songs
        FROM {}
        iam_role {}
        FORMAT AS json 'auto';
        """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
#staging_events_copy = (""" copy staging_events from 's3://udacity-dend/log_data'
#                            credentials 'aws_iam_role={}'
#                             compupdate off region 'us-west-2'
#                             timeformat as 'epochmillisecs'
#                             truncatecolumns blanksasnull emptyasnull
#                             json 's3://udacity-dend/log_json_path.json';
#""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, 
                        level, song_id, artist_id, session_id, location, user_agent)
                        
                        SELECT DISTINCT 
                            TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                            se.user_id,
                            se.level,
                            ss.song_id,
                            ss.artist_id,
                            se.session_id,
                            se.location,
                            se.user_agent
                        FROM staging_events se
                        JOIN staging_songs ss ON ss.title = se.song 
                        AND se.artist = ss.artist_name
                        WHERE se.page = 'NextSong';
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) 
                         SELECT DISTINCT
                             se.user_id,
                             se.first_name,
                             se.last_name,
                             se.gender,
                             se.level 
                        FROM staging_events se 
                        WHERE se.page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                            SELECT DISTINCT
                                ss.song_id,
                                ss.title,
                                ss.artist_id,
                                ss.year,
                                ss.duration
                            FROM staging_songs ss
                            WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                            SELECT DISTINCT
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                            FROM staging_songs
                            WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                            SELECT DISTINCT 
                                TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                                EXTRACT(hour from start_time) as hour,
                                EXTRACT(day from start_time) as day,
                                EXTRACT(week from start_time) as week,
                                EXTRACT(month from start_time) as month,
                                EXTRACT(year from start_time) as year,
                                to_char(start_time, 'Day') AS weekday
                            FROM staging_events se
                            WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
