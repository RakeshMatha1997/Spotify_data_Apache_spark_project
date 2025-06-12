CREATE DATABASE Spotify_db;

create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::717279706991:role/Spotify_spark_snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ('S3://projectglue1997')
    COMMENT = 'Creating connection to s3';

desc integration s3_init

create or replace file format csv_fileformat
    type = csv
    field_delimiter =','
    skip_header = 1
    null_if= ('NULL','null')
    empty_field_as_null = TRUE;

create or replace stage  spotify_stage
    URL = 's3://projectglue1997/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

List @spotify_stage/song_data;

create or replace TABLE tbl_album(
    album_id STRING,
    album_name STRING,
    album_release_date DATE,
    album_total_tracks INTEGER,
    URL STRING
);

create or replace TABLE tbl_artist(
    artist_id STRING,
    artist_name STRING,
    artist_type STRING,
    URL STRING
);

create or replace TABLE tbl_song(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    explicit STRING,
    url STRING,
    popularity INTEGER,
    track_number INTEGER,
    album_id STRING,
    album_name STRING,
    album_release_date DATE,
    album_total_tracks INTEGER
);

select * from tbl_song;

copy into tbl_song
FROM @spotify_stage/song_data/album_data_tranformed_2025-06-06/run-1749221831802-part-r-00004;

create or replace schema pipe;

create or replace pipe spotify_db.pipe.tbl_song_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_song
from  @spotify_db.public.spotify_stage/song_data/;

create or replace pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
from  @spotify_db.public.spotify_stage/album_data/;

create or replace pipe spotify_db.pipe.tbl_artist_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artist
from  @spotify_db.public.spotify_stage/artist_data/;

desc pipe pipe.tbl_album_pipe;

desc pipe pipe.tbl_artist_pipe;

desc pipe pipe.tbl_song_pipe;

select count(*) from tbl_album;

select count(*) from tbl_artist;

select count(*) from tbl_song;