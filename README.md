# Spotify_data_Apache_spark_project
This is the ETL Project which Extracts data from spotify API using Aws services and loads the data into snowflake database.

# Architecture:
![image](https://github.com/user-attachments/assets/fe36079a-18e3-40a7-b390-6609ab1deed6)

# Project flow:

1)Extract the data from spotify dev app. here i have took the URL of my songs playlist and using lambda and loaded the raw data to AWS S3.

2)Once the data in S3 by using the glue job by doing some pyspark transformations i have cleaned the raw data and transformed to well designed formatt and loaded the transformed data to s3.

3)Created Snowpipe and snowflake database, schemas, tables needed as per the tables that needs to load and loaded the data into it.

Source:
My Playlist
![image](https://github.com/user-attachments/assets/b90d7c02-f98d-411d-828b-f101365338be)

Target sample data:
Album_data:
![image](https://github.com/user-attachments/assets/1dfb7a1a-a05b-486f-9c2a-85ba2615fdf0)
Artist_data:
![image](https://github.com/user-attachments/assets/7e6288e2-d669-47d5-a640-0b3b0587dac7)
Song_data:
![image](https://github.com/user-attachments/assets/4c279b31-459f-4d14-a5e8-263aee4edaf2)

Target count details:
![image](https://github.com/user-attachments/assets/93bddba2-d429-4e10-a881-724e58d00f58)
