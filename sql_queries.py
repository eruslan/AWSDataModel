"""
sql_queries.py module is used to determine SQL statements performed against database specified at 
DB_NAME=dwhs during etl.py import process
there are DROP TABLES, CREATE TABLES, STAGING COPY, ISERT TABLES statements
The statements can be used as strings in query lists provided:
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create,
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]
drop_table_queries = [staging_events_table_drop, 
                        staging_songs_table_drop,
                        songplay_table_drop, 
                        user_table_drop, 
                        song_table_drop, 
                        artist_table_drop, 
                        time_table_drop]
copy_table_queries = [staging_events_copy, 
                        staging_songs_copy]
insert_table_queries = [songplay_table_insert, 
                        user_table_insert,
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]

"""

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

staging_events_table_create = " CREATE TABLE staging_events ( \
                                                           artist varchar,   \
                                                           auth varchar,    \
                                                           firstName varchar, \
                                                           gender varchar,  \
                                                           itemInSession varchar, \
                                                           lastName varchar,  \
                                                           length varchar,  \
                                                           level varchar,  \
                                                           location varchar,  \
                                                           method varchar,   \
                                                           page  varchar,  \
                                                           registration varchar, \
                                                           sessionId  varchar, \
                                                           song varchar,   \
                                                           status varchar,   \
                                                           ts varchar,  \
                                                           userAgent varchar, \
                                                           userId int);"

staging_songs_table_create = "CREATE TABLE staging_songs (song_id varchar, \
                                                           num_songs varchar, \
                                                           title varchar, \
                                                           artist_name varchar, \
                                                           artist_latitude varchar, \
                                                           year varchar, \
                                                           duration varchar, \
                                                           artist_id varchar, \
                                                           artist_longitude varchar, \
                                                           artist_location varchar);"

songplay_table_create = "CREATE  TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY, \
                                                               user_id  int NOT NULL, \
                                                               song_id varchar, \
                                                               artist_id varchar, \
                                                               session_id int, \
                                                               start_time timestamp NOT NULL, \
                                                               level varchar, \
                                                               location varchar, \
                                                               user_agent varchar);"


user_table_create = "CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, \
                                                       first_name varchar, \
                                                       last_name varchar, \
                                                       gender varchar, \
                                                       level varchar);"


song_table_create = "CREATE TABLE IF NOT EXISTS songs (song_id varchar  PRIMARY KEY, \
                                                        title varchar sortkey distkey, \
                                                        artist_id varchar, \
                                                        year int, \
                                                        duration float);"


artist_table_create = "CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                            name varchar sortkey distkey, \
                                                            location varchar, \
                                                            latitude float, \
                                                            longitude float);"


time_table_create = "CREATE  TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, \
                                                       hour int, \
                                                       day int, \
                                                       week int, \
                                                       month int , \
                                                       year int , \
                                                       weekday int);"

# STAGING TABLES

staging_events_copy = ("""copy staging_events
                            from '{}'
                            credentials 'aws_iam_role={}'
                            format json as '{}' 
                            dateformat 'auto';
                       """).format(config.get("S3", "LOG_DATA"),config.get("IAM_ROLE", "ARN") , config.get("S3", "LOG_JSONPATH") )

staging_songs_copy = ("""copy staging_songs
                            from '{}'
                            credentials 'aws_iam_role={}'
                            json 'auto'
                            region 'us-west-2'
                            BLANKSASNULL 
                            TRIMBLANKS
                            TRUNCATECOLUMNS;
                      """).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays (user_id, \
                                                song_id, \
                                                artist_id, \
                                                session_id, \
                                                start_time, \
                                                level, \
                                                location, \
                                                user_agent) \
                          SELECT distinct \
                                                 se.userId as user_id, \
                                                 s.song_id as song_id, \
                                                 a.artist_id as artist_id, \
                                                 sessionId::int as session_id, \
                                                 timestamp 'epoch' + \
                                                   se.ts/1000 * interval '1 second' as start_time, \
                                                 se.level as level,  \
                                                 se.location as location, \
                                                 se.userAgent as user_agent \
                          FROM  \
                                                 staging_events se \
                                                 LEFT JOIN songs s on se.song = s.title \
                                                 LEFT JOIN artists a on s.artist_id = a.artist_id and se.artist=a.name \
                          WHERE se.Page='NextSong';"

user_table_insert = "INSERT INTO users (user_id, \
                                        first_name, \
                                        last_name, \
                                        gender, \
                                        level) \
                    SELECT distinct      userId::int as user_id, \
                                        firstName as first_name, \
                                        lastName as last_name, \
                                        gender as gender, \
                                        level as level \
                    FROM staging_events \
                    WHERE userId IS NOT NULL;"

song_table_insert = "INSERT INTO songs (song_id, \
                                            title, \
                                             artist_id, \
                                             year, \
                                             duration) \
                          SELECT  distinct                       \
                                            song_id as song_id, \
                                            title as title, \
                                            artist_id as artist_id, \
                                            year::int as year, \
                                            duration::float as duration \
                          FROM staging_songs;"

artist_table_insert = "INSERT INTO artists (artist_id, \
                                            name, \
                                            location, \
                                            latitude, \
                                            longitude) \
                       SELECT distinct \
                                             artist_id               as artist_id, \
                                             artist_name             as name, \
                                             artist_location         as location, \
                                             artist_latitude::float  as latitude, \
                                             artist_longitude::float as longitude \
                       FROM staging_songs;"

time_table_insert = "INSERT INTO time (start_time, \
                                             hour, \
                                             day, \
                                             week, \
                                             month, \
                                             year, \
                                             weekday) \
                      SELECT  distinct\
                                             timestamp 'epoch' + \
                                                 se.ts/1000 * interval '1 second' as start_time, \
                                             DATE_PART(hrs, start_time) as hour, \
                                             DATE_PART(dayofyear, start_time) as day, \
                                             DATE_PART(w, start_time) as week, \
                                             DATE_PART(mons,start_time) as month, \
                                             DATE_PART(yrs, start_time) as year, \
                                             DATE_PART(dow, start_time) as weekday \
                     FROM staging_events se;"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
