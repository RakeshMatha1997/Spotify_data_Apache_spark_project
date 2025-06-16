# Spotify_data_Apache_spark_project
This is the ETL Project which Extracts data from spotify API using Aws services and loads the data into snowflake database.

Project flow:

1)Extract the data from spotify dev app. here i have took the URL of my songs playlist and using lambda and loaded the raw data to AWS S3.

2)Once the data in S3 by using the glue job by doing some pyspark transformations i have cleaned the raw data and transformed to well designed formatt and loaded the transformed data to s3.

3)Created Snowpipe and snowflake database, schemas, tables needed as per the tables that needs to load and loaded the data into it.

Source:
My Playlist
![image](https://github.com/user-attachments/assets/b90d7c02-f98d-411d-828b-f101365338be)

Target 
