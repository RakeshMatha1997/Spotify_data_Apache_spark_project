
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import *
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path="s3://projectglue1997/raw_data/to_processed/"
source_dyf = glueContext.create_dynamic_frame_from_options(
  connection_type="s3",
  connection_options={"paths": [s3_path]},
  format="json"
)
spotify_df=source_dyf.toDF()
def process_album(df):
    df= df.withColumn("items",explode("items")).select(col("items.track.album.id").alias("album_id"),
                                               col("items.track.album.name").alias("album_name"),
                                               col("items.track.album.release_date").alias("album_release_date"),
                                               col("items.track.album.total_tracks").alias("album_total_tracks"),
                                               col("items.track.album.external_urls.spotify").alias("url")).drop_duplicates(['album_id'])
    return df

def process_artist(df):
    df_artist_exploded= df.select(explode(col("items")).alias("items")).select(explode(col("items.track.artists")).alias("artist"))
    df_artist= df_artist_exploded.select(col("artist.id").alias("artist_id"),
                         col("artist.name").alias("artist_name"),
                          col("artist.type").alias("artist_type"),
                          col("artist.external_urls.spotify").alias("url")
                         ).drop_duplicates(['artist_id'])
    return df_artist
    
def process_song(df):
    df_song_expoded= df.select(explode(col("items")).alias("items"))
    df_song= df_song_expoded.select(col("items.track.id").alias("song_id"),
                      col("items.track.name").alias("song_name"),
                      col("items.track.duration_ms").alias("duration_ms"),
                      col("items.track.explicit").alias("explicit"),
                      col("items.track.external_urls.spotify").alias("url"),
                      col("items.track.popularity").alias("popularity"),
                      col("items.track.track_number").alias("track_number"),
                      col("items.track.album.id").alias("album_id"),
                      col("items.track.album.name").alias("album_name"),
                      col("items.track.album.release_date").alias("album_release_date"),
                      col("items.track.album.total_tracks").alias("album_total_tracks"),
                      col("items.track.album.external_urls.spotify").alias("url")
                      ).drop_duplicates(['song_id'])
    return df_song
    
#To process the data
Album_df= process_album(spotify_df)
Artist_df= process_artist(spotify_df)
Song_df= process_song(spotify_df)
Song_df.show()
def write_to_s3(df,path, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(\
                                                 frame = dynamic_frame,\
                                                 connection_options = {'path':f"s3://projectglue1997/transformed_data/{path}/"},\
                                                 connection_type = 's3',\
                                                 format = format_type)
write_to_s3(Album_df, "album_data/album_data_tranformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(Artist_df, "artist_data/album_data_tranformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(Song_df, "song_data/album_data_tranformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
job.commit()
