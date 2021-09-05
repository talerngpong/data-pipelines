# DROP-related queries
staging_events_table_drop_query: str = '''
    DROP TABLE IF EXISTS staging_events
'''
staging_songs_table_drop_query: str = '''
    DROP TABLE IF EXISTS staging_songs
'''
songplay_table_drop_query: str = '''
    DROP TABLE IF EXISTS songplays
'''
user_table_drop_query: str = '''
    DROP TABLE IF EXISTS users
'''
song_table_drop_query: str = '''
    DROP TABLE IF EXISTS songs
'''
artist_table_drop_query: str = '''
    DROP TABLE IF EXISTS artists
'''
time_table_drop_query: str = '''
    DROP TABLE IF EXISTS times
'''

# CREATE-TABLE-related queries
staging_events_table_create_query: str = '''
    CREATE TABLE staging_events (
        artist        TEXT      NULL,
        auth          TEXT      NULL,
        firstName     TEXT      NULL,
        gender        TEXT      NULL,
        itemInSession INT       NULL,
        lastName      TEXT      NULL,
        length        NUMERIC   NULL,
        level         TEXT      NULL,
        location      TEXT      NULL,
        method        TEXT      NULL,
        page          TEXT      NULL,
        registration  NUMERIC   NULL,
        sessionId     TEXT      NULL DISTKEY,
        song          TEXT      NULL,
        status        INT       NULL,
        ts            TIMESTAMP NULL, -- from epochmillisecs
        userAgent     TEXT      NULL,
        userId        TEXT      NULL
    )
'''
staging_songs_table_create_query: str = '''
    CREATE TABLE staging_songs (
        num_songs        INT     NULL,
        artist_id        TEXT    NULL DISTKEY,
        artist_latitude  TEXT    NULL,
        artist_longitude TEXT    NULL,
        artist_location  TEXT    NULL,
        artist_name      TEXT    NULL,
        song_id          TEXT    NULL,
        title            TEXT    NULL,
        duration         NUMERIC NULL,
        year             INT     NULL
    )
'''
songplay_table_create_query: str = '''
    CREATE TABLE songplays (
        songplay_id TEXT      NOT NULL,
        start_time  TIMESTAMP NOT NULL,
        user_id     TEXT      NULL,
        level       TEXT      NULL,
        song_id     TEXT      NULL,
        artist_id   TEXT      NULL,
        session_id  TEXT      NULL,
        location    TEXT      NULL,
        user_agent  TEXT      NULL,
        PRIMARY KEY (songplay_id)
    )
'''
user_table_create_query: str = ('''
    CREATE TABLE IF NOT EXISTS users (
        user_id    TEXT NOT NULL,
        first_name TEXT NULL,
        last_name  TEXT NULL,
        gender     TEXT NULL,
        level      TEXT NULL,
        PRIMARY KEY (user_id)
    )
''')
song_table_create_query: str = '''
    CREATE TABLE IF NOT EXISTS songs (
        song_id   TEXT    NOT NULL,
        title     TEXT    NULL,
        artist_id TEXT    NOT NULL,
        year      INT     NULL,
        duration  NUMERIC NULL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
'''
artist_table_create_query: str = '''
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL,
        name      TEXT,
        location  TEXT,
        latitude  TEXT,
        longitude TEXT,
        PRIMARY KEY (artist_id)
    )
'''
time_table_create_query: str = '''
    CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP NOT NULL,
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    INT,
        PRIMARY KEY (start_time)
    )
'''

table_drop_queries = [
    songplay_table_drop_query,
    song_table_drop_query,
    time_table_drop_query,
    artist_table_drop_query,
    user_table_drop_query,
    staging_songs_table_drop_query,
    staging_events_table_drop_query
]

table_create_queries = [
    staging_events_table_create_query,
    staging_songs_table_create_query,
    user_table_create_query,
    artist_table_create_query,
    time_table_create_query,
    song_table_create_query,
    songplay_table_create_query
]
