import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id="23719c6422274100bf0bfc19ae3adccd", client_secret="9909667689884b11bb4160e6f7fc6320")
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    
    playlist_link = "https://open.spotify.com/playlist/3wdfyFfb9KtWCsi75Xgqtn"
    playlist_URI = playlist_link.split('/')[-1]
    
    spotify_data = sp.playlist_tracks(playlist_URI)   
    
    cilent = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    cilent.put_object(
        Bucket="projectglue1997",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
        )
    glue = boto3.client('glue')
    gluejobname = "Spotify transformation job"

    try:
        runID= glue.start_job_run(JobName=gluejobname)
        status= glue.get_job_run(JobName=gluejobname, RunId=runID['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)