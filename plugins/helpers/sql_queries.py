from typing import Callable, List

from helpers.aws_credentials import AwsCredentials
from helpers.rich_query import RichQuery


class SqlQueries:
    # INSERT-related queries
    songplay_table_insert_query: RichQuery = RichQuery(
        value='''
            INSERT INTO songplays (
                songplay_id,
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent
            )
            SELECT
                MD5(se.sessionId || EXTRACT(EPOCH FROM se.ts)) AS songplay_id,
                se.ts AS start_time,
                se.userId AS user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId AS session_id,
                se.location,
                se.userAgent as user_agent
            FROM staging_events se
            JOIN staging_songs ss
            ON (
                ss.title = se.song
                AND
                ss.duration = se.length
                AND
                ss.artist_name = se.artist
            )
        ''',
        table_name='songplays',
    )
    user_table_insert_query: RichQuery = RichQuery(
        value='''
            INSERT INTO users (
                user_id,
                first_name,
                last_name,
                gender,
                level
            )
            SELECT
                DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
            FROM staging_events
            WHERE COALESCE(userId, '') <> ''
        ''',
        table_name='users'
    )
    song_table_insert_query: RichQuery = RichQuery(
        value='''
            INSERT INTO songs (
                song_id,
                title,
                artist_id,
                year,
                duration
            )
            SELECT
                DISTINCT song_id AS song_id,
                title,
                artist_id,
                year,
                duration
            FROM staging_songs
            WHERE COALESCE(song_id, '') <> ''
        ''',
        table_name='songs'
    )
    artist_table_insert_query: RichQuery = RichQuery(
        value='''
            INSERT INTO artists (
                artist_id,
                name,
                location,
                latitude,
                longitude
            )
            SELECT
                DISTINCT artist_id AS artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
            FROM staging_songs
            WHERE COALESCE(artist_id, '') <> ''
        ''',
        table_name='artists'
    )
    time_table_insert_query: RichQuery = RichQuery(
        value='''
            INSERT INTO times (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday
            )
            SELECT
                DISTINCT ts AS start_time,
                extract(hour from ts) AS hour,
                extract(day from ts) AS day,
                extract(week from ts) AS week,
                extract(month from ts) AS month,
                extract(year from ts) AS year,
                extract(weekday from ts) AS weekday
            FROM staging_events
            WHERE ts IS NOT NULL
        ''',
        table_name='times'
    )

    # COPY-related queries
    @staticmethod
    def build_staging_events_copy_query_builder(
            s3_data_set_source_path: str,
            s3_json_path_source_path: str
    ) -> Callable[[AwsCredentials], RichQuery]:
        table_name = 'staging_events'

        def builder(aws_credentials: AwsCredentials) -> RichQuery:
            return RichQuery(
                value='''
                    COPY {table_name}
                    FROM '{s3_data_set_source_path}'
                    ACCESS_KEY_ID '{aws_access_key_id}'
                    SECRET_ACCESS_KEY '{aws_secret_access_key}'
                    FORMAT AS JSON '{s3_json_path_source_path}'
                    timeformat as 'epochmillisecs'
                '''.format(
                    table_name=table_name,
                    s3_data_set_source_path=s3_data_set_source_path,
                    aws_access_key_id=aws_credentials.access_key_id,
                    aws_secret_access_key=aws_credentials.secret_access_key,
                    s3_json_path_source_path=s3_json_path_source_path
                ),
                table_name=table_name
            )

        return builder

    @staticmethod
    def build_staging_songs_copy_query_builder(s3_data_set_source_path: str) -> Callable[[AwsCredentials], RichQuery]:
        table_name = 'staging_songs'

        def builder(aws_credentials: AwsCredentials) -> RichQuery:
            return RichQuery(
                value='''
                    COPY {table_name}
                    FROM '{s3_data_set_source_path}'
                    ACCESS_KEY_ID '{aws_access_key_id}'
                    SECRET_ACCESS_KEY '{aws_secret_access_key}'
                    FORMAT AS JSON 'auto'
                '''.format(
                    table_name=table_name,
                    s3_data_set_source_path=s3_data_set_source_path,
                    aws_access_key_id=aws_credentials.access_key_id,
                    aws_secret_access_key=aws_credentials.secret_access_key
                ),
                table_name=table_name
            )

        return builder

    data_quality_check_queries: List[RichQuery] = [
        RichQuery(
            value=f'SELECT COUNT(*) AS number_of_rows FROM {table_name}',
            table_name=table_name
        )
        for table_name
        in ['songplays', 'users', 'songs', 'artists', 'times']
    ]
