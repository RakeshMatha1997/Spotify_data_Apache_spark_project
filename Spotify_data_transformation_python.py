import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

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
        Bucket="ajay-test05122024",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
        )
    df=pd.DataFrame.from_dict(spotify_data, orient='index')
    items_data= df[0]['items']
    album_list=[]
    for item in items_data:
        album_id= item['track']['album']['id']
        album_name= item['track']['album']['name']
        album_release_date= item['track']['album']['release_date']
        album_release_date_precision= item['track']['album']['release_date_precision']
        album_total_tracks= item['track']['album']['total_tracks']
        album_json={"album_id":album_id,"album_name":album_name,"album_release_date":album_release_date,"album_total_tracks":album_total_tracks}
        album_list.append(album_json)
    album_data= pd.DataFrame(album_list)
    csv_buffer = StringIO()
    album_data.to_csv(csv_buffer, index=False)

    filename_album = "album_transformed_" + str(datetime.now()) + ".csv"

    cilent.put_object(
        Bucket="ajay-test05122024",
        Key="raw_data/processed/album/" + filename_album,
        Body=csv_buffer.getvalue()
        )

 
